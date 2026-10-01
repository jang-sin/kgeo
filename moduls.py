import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os, csv
import requests, re
import types,sys,json
import threading, shutil, time

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# kgeo 서버는 IP 당 "신규 TCP 연결 수"를 제한한다.
# 요청 수가 아니라 연결 수가 기준이므로, 연결 하나를 계속 재사용(keep-alive)하면
# 제한에 걸리지 않는다. requests.get() 은 매 호출마다 Session 을 만들고 버리기
# 때문에 이 제한에 정면으로 부딪힌다. 아래 공용 Session 을 반드시 사용할 것.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# .env 로더
#
# main.py 가 GitHub raw 에서 코드를 받아 실행하므로 외부 라이브러리를 늘리기
# 어렵다. python-dotenv 대신 직접 파싱한다.
# 접속정보는 절대 저장소에 넣지 않는다 - 이 저장소는 공개다.
# ---------------------------------------------------------------------------

def load_env(path='.env'):
    env = {}
    if not os.path.isfile(path):
        return env
    for line in open(path, encoding='utf-8'):
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    for k, v in env.items():
        if v:
            os.environ.setdefault(k, v)
    return env


ENV = load_env()


MAX_WORKERS = 5
TIMEOUT = (5, 30)  # (연결, 응답 대기) 초 - 무한 대기 방지


def _build_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'),
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://kgeop.go.kr/',
        'Connection': 'keep-alive',
    })
    # 재시도는 일시적 흔들림만 흡수하도록 짧게 잡는다.
    # 차단은 urllib3 재시도로 뚫는 게 아니라 throttle 로 물러서서 푼다.
    # (예전 connect=4 / backoff 1.5 는 차단된 서버를 요청당 47초씩 두드렸다.)
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET'],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
        max_retries=retry,
    )
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s


SESSION = _build_session()


def warm_up():
    """메인 페이지를 한 번 방문해 세션 쿠키(SCOUTER, XSRF-TOKEN)를 받아둔다."""
    try:
        SESSION.get('https://kgeop.go.kr/', timeout=TIMEOUT)
    except Exception as e:
        print(f'[warm_up] 세션 초기화 실패(무시하고 진행): {type(e).__name__}: {e}')


# ---------------------------------------------------------------------------
# 적응형 제동 (throttle)
#
# 실측: 연결을 재사용해도 누적 요청 약 11,000건(12 PNU/s 로 8분) 지점에서
# IP 단위 하드 차단이 걸렸다. 차단은 SYN 드롭이라 ConnectTimeout 으로 나타난다.
#
# 대응: 요청 간 최소 간격을 두고, 연결 실패가 연속되면
#   1) 모든 스레드를 멈춘 뒤 대기하고
#   2) 재개할 때 간격을 늘려(= 속도를 낮춰) 다시 걸리지 않게 한다.
# 성공이 쌓이면 간격을 조금씩 줄여 원래 속도로 되돌린다.
# ---------------------------------------------------------------------------

# 실측: 초당 약 29요청을 6분 유지한 지점(누적 약 11,000요청)에서 하드 차단.
# 그보다 낮춰서 출발한다. 0.05초 간격 = 전체 초당 약 20요청 = 약 7 PNU/s.
MIN_INTERVAL = float(os.environ.get('MIN_INTERVAL', '0.05'))
MAX_INTERVAL = 2.0
FAIL_THRESHOLD = 3        # 연속 연결 실패 몇 번이면 차단으로 볼지

# 차단이 한 시간 넘게 지속된 사례가 있어 정지 시간도 회차마다 늘린다.
# 5분 고정이면 풀리지도 않은 서버에 계속 재시도하며 시간을 버린다.
PAUSE_BASE = 300
PAUSE_MAX = 1800
_pause_seconds = PAUSE_BASE

_throttle_lock = threading.Lock()
_next_slot = 0.0
_interval = MIN_INTERVAL
_consecutive_fails = 0
_successes_since_backoff = 0
_pause_until = 0.0


def _acquire_slot():
    """요청 간 최소 간격을 지키고, 전체 정지 중이면 풀릴 때까지 기다린다."""
    global _next_slot
    while True:
        with _throttle_lock:
            now = time.time()
            if now >= _pause_until:
                slot = max(now, _next_slot)
                _next_slot = slot + _interval
                wait = slot - now
                break
            wait_pause = _pause_until - now
        time.sleep(min(wait_pause, 5))
    if wait > 0:
        time.sleep(wait)


def _on_success():
    global _consecutive_fails, _successes_since_backoff, _interval
    with _throttle_lock:
        _consecutive_fails = 0
        if _interval > MIN_INTERVAL:
            _successes_since_backoff += 1
            if _successes_since_backoff >= 200:
                _successes_since_backoff = 0
                _interval = max(MIN_INTERVAL, _interval / 2)
                print(f'[throttle] 안정적 - 간격을 {_interval:.2f}초로 완화', flush=True)


def _on_connect_error():
    """연결 실패 누적. 임계치를 넘으면 전체 정지 + 속도 하향."""
    global _consecutive_fails, _interval, _pause_until, _successes_since_backoff
    with _throttle_lock:
        _consecutive_fails += 1
        if _consecutive_fails < FAIL_THRESHOLD or time.time() < _pause_until:
            return
        global _pause_seconds
        _consecutive_fails = 0
        _successes_since_backoff = 0
        _interval = min(MAX_INTERVAL, max(0.25, _interval * 2))
        _pause_until = time.time() + _pause_seconds
        print(f'[throttle] 차단 감지 - {_pause_seconds}초 전체 정지 후 '
              f'간격 {_interval:.2f}초로 재개', flush=True)
        _pause_seconds = min(PAUSE_MAX, _pause_seconds * 2)
    _reset_session()


def get_json(url):
    """공용 Session 으로 GET 후 JSON 반환.

    연결 재사용 + 타임아웃 + 적응형 제동을 적용한다.
    """
    _acquire_slot()
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.ConnectionError:
        _on_connect_error()
        raise
    except requests.exceptions.Timeout:
        _on_connect_error()
        raise
    _on_success()
    return data


# ---------------------------------------------------------------------------
# 소프트 차단 감지
#
# kgeo 는 연결 제한에 걸린 클라이언트에게 HTTP 429 를 주지 않는다.
# 200 OK 에 모든 배열이 빈 278바이트 JSON(code 974, "조회 결과 없음")을 돌려준다.
# 즉 '데이터가 없는 PNU' 와 '차단당한 상태' 의 응답이 똑같이 생겼다.
#
# 구분 방법: 데이터가 확실히 있는 PNU(카나리)를 한 번 찔러본다.
#   - 카나리도 비어 있으면  -> 차단 상태. 대기 후 연결을 새로 맺고 재시도한다.
#   - 카나리에 데이터가 있으면 -> 그 PNU 는 진짜로 데이터가 없는 것.
# 이 판별이 없으면 차단 구간에 걸린 PNU 들이 '데이터 없음'으로 조용히 유실된다.
# ---------------------------------------------------------------------------

CANARY_PNU = '4128111500103900000'   # 경기도 고양시 덕양구 선유동 390 (응답 약 82KB)
CANARY_MIN_INTERVAL = 30             # 카나리 확인 최소 간격(초)
BLOCK_COOLDOWN = 90                  # 차단 확인 시 대기(초)

_guard_lock = threading.Lock()
_last_canary = 0.0
_canary_healthy = True
_recoveries = 0          # 차단을 감지하고 복구한 횟수(세대 번호)


def is_empty_response(jsondata):
    return not any(jsondata.get(k) for k in (
        'landOwnerShipHistList', 'jigaRst', 'landLedgRst',
        'bldgInfoRstList', 'moveHistList', 'shrYmbList'))


def recovery_count():
    """요청을 보내기 직전의 세대 번호. looks_blocked() 에 그대로 넘긴다."""
    return _recoveries


def looks_blocked(seen_at=None):
    """빈 응답을 받았을 때 호출. 재시도가 필요하면 True 를 반환한다.

    seen_at: 요청 직전에 recovery_count() 로 받아 둔 세대 번호.
             그 사이에 차단 복구가 있었다면 내 응답은 차단 중에 받은 쓰레기이므로
             무조건 재시도한다. 이 인자가 없으면 대기 중이던 다른 스레드들이
             자기 PNU 를 '데이터 없음'으로 잘못 기록한다.
    """
    global _last_canary, _canary_healthy, _recoveries
    with _guard_lock:
        if seen_at is not None and seen_at != _recoveries:
            return True

        now = time.time()
        if now - _last_canary < CANARY_MIN_INTERVAL:
            # 방금 확인했다. 중복 확인으로 부하를 더하지 않고 직전 결과를 쓴다.
            return not _canary_healthy
        _last_canary = now

        try:
            canary = get_json(
                f'https://kgeop.go.kr/geopass/api/selectOneParcelInfo.do?pnu={CANARY_PNU}')
        except Exception as e:
            _canary_healthy = False
            print(f'[guard] 카나리 요청 실패({type(e).__name__}) - 차단으로 간주하고 대기')
        else:
            if not is_empty_response(canary):
                _canary_healthy = True
                return False   # 서버 정상. 해당 PNU 는 진짜 데이터 없음.
            _canary_healthy = False
            print('[guard] 카나리도 빈 응답 - 소프트 차단 상태로 판단')

        print(f'[guard] {BLOCK_COOLDOWN}초 대기 후 연결을 새로 맺습니다.')
        time.sleep(BLOCK_COOLDOWN)
        _reset_session()
        _recoveries += 1
        # 복구 직후 상태는 아직 검증되지 않았다. 다음 빈 응답이 오면
        # 간격 제한 없이 곧바로 카나리를 다시 확인하도록 초기화한다.
        _last_canary = 0.0
        _canary_healthy = True
        return True


def _reset_session():
    """차단된 연결을 버리고 새 연결로 교체한다."""
    global SESSION
    try:
        SESSION.close()
    except Exception:
        pass
    SESSION = _build_session()
    warm_up()


# ---------------------------------------------------------------------------
# CSV 기록
#   - 행마다 DataFrame 생성 + to_csv(mode='a') 는 1.41ms/행.
#     파일 핸들을 한 번만 열고 csv.writer 로 쓰면 0.01ms/행 (약 270배).
#   - 스레드 5개가 같은 파일에 동시에 append 하면 행이 섞이므로 락으로 보호한다.
# ---------------------------------------------------------------------------

class CsvWriter:
    def __init__(self):
        self._handles = {}
        self._lock = threading.Lock()

    def _open(self, path, columns):
        """파일을 열고 헤더를 확정한다. 이어쓰기면 기존 헤더를 읽어 온다."""
        header = None
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, 'r', newline='', encoding='utf-8-sig') as rf:
                first = rf.readline()
            if first:
                header = next(csv.reader([first]), None)
        f = open(path, 'a', newline='', encoding='utf-8-sig')
        w = csv.writer(f)
        if header is None:
            w.writerow(columns)
            header = list(columns)
        return [f, w, header]

    def write(self, path, row):
        """row: dict(컬럼명 -> 값). 파일이 비어 있으면 헤더를 먼저 쓴다.

        컬럼 구성이 헤더와 다르면 예외를 던진다. 예전에는 검증이 없어서,
        같은 파일에 컬럼 수가 다른 행이 섞이면 헤더와 값이 어긋난 채로
        조용히 기록됐다(csv.writer 는 순서대로만 쓴다).
        """
        cols = list(row.keys())
        with self._lock:
            entry = self._handles.get(path)
            if entry is None:
                entry = self._open(path, cols)
                self._handles[path] = entry
            f, w, header = entry
            if cols != header:
                raise ValueError(
                    f'{path} 컬럼 불일치 - 파일={header} / 기록시도={cols}')
            w.writerow([row[c] for c in header])

        # DB 적재 대상이면 함께 버퍼에 넣는다. CSV 는 백업 겸 검증용으로 계속 남긴다.
        if DB is not None:
            DB.add(path, row)

    def write_safe(self, path, row):
        """실패 기록용. 여기서 예외가 나도 수집 자체를 죽이지 않는다."""
        try:
            self.write(path, row)
        except Exception as e:
            print(f'[CSV] {path} 기록 실패: {type(e).__name__}: {e}', flush=True)

    def flush(self):
        with self._lock:
            for f, _w, _h in self._handles.values():
                f.flush()

    def close(self):
        with self._lock:
            for f, _w, _h in self._handles.values():
                try:
                    f.close()
                except Exception:
                    pass
            self._handles.clear()


CSV = CsvWriter()


OUTPUT_FILES = [
    'kgeo_land_owner_hist.csv',
    'kgeo_shrymblist.csv',
    'kgeo_jigaRst.csv',
    'kgeo_landLedgRst.csv',
    'kgeo_bldgInfoRstList.csv',
    'kgeo_flrList.csv',
    'kgeo_moveHistList.csv',
    'kgeo_sub_addr.csv',
    'failed_pnu.csv',
    'failed_step.csv',
    'failed_bldg.csv',
    'empty_pnu.csv',
]


def backup_outputs():
    """기존 결과 파일을 백업 폴더로 옮긴다(삭제하지 않음).

    옮기지 않으면 재실행분이 계속 누적된다. 실제로 kgeo_jigaRst.csv 는
    고유 PNU 996건인데 67,736행 - 같은 데이터가 17회분 쌓여 있었다.
    """
    existing = [f for f in OUTPUT_FILES if os.path.isfile(f)]
    if not existing:
        return
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join('backup', stamp)
    os.makedirs(backup_dir, exist_ok=True)
    for f in existing:
        shutil.move(f, os.path.join(backup_dir, f))
    print(f'[backup] 기존 결과 {len(existing)}개 파일을 {backup_dir}/ 로 이동했습니다.')


# PNU 컬럼명이 파일마다 다르다. 이어받기 판정에 쓸 (파일, 컬럼) 목록.
# 완료 판정에 쓰는 CSV 는 empty_pnu.csv 하나뿐이다.
#
# 데이터 CSV(kgeo_*.csv)는 일부러 뺐다. 적재 대상이 DB 로 바뀐 뒤로 CSV 는
# 백업일 뿐이라 DB 와 어긋날 수 있다. 실제로 DB 만 비우고 CSV 를 남겨둔 채
# 실행하면, CSV 에 있다는 이유로 808건이 완료 처리되어 DB 에 영영 들어가지
# 않았다. 완료 판정의 기준은 적재처인 DB 여야 한다.
#
# empty_pnu.csv 는 남겨둔다. 서버에 데이터가 없는 PNU 는 어느 테이블에도
# 들어가지 않아 DB 로는 판정할 수 없다. 이 기록이 사라져도 해당 PNU 를 다시
# 조회해 또 비었음을 확인할 뿐이라 데이터가 틀어지지 않는다.
_DONE_SOURCES = [
    ('empty_pnu.csv', 'pnu'),
]


def load_done_pnus():
    """데이터가 없는 것으로 확인된 PNU 집합. DB 로는 판정할 수 없는 부분이다.

    데이터가 있는 PNU 의 완료 판정은 DbWriter.done_pnus() 가 담당한다.
    failed_pnu.csv 는 일부러 제외한다 - 다시 시도해야 하는 건이다.
    """
    done = set()
    for fname, col in _DONE_SOURCES:
        if not os.path.isfile(fname):
            continue
        try:
            s = pd.read_csv(fname, dtype=str, usecols=[col])[col]
            done |= set(s.dropna().str.strip())
        except Exception as e:
            print(f'[resume] {fname} 읽기 실패(건너뜀): {type(e).__name__}: {e}')
    done.discard('')
    return done


# ---------------------------------------------------------------------------
# Oracle 적재
#
# 테이블명 = 고정 접두사 + 실행 시 받는 접미사. 예) KGEO_OWNER_HIST_1
# 접미사는 테이블명의 일부라서 바인드 변수로 넘길 수 없다(SQL 문법상 불가).
# 문자열로 붙일 수밖에 없으므로 숫자만 허용해 인젝션 여지를 없앤다.
#
# 대상 컬럼은 전부 VARCHAR2 라 콤마가 박힌 숫자("109,900")도 변환 없이 들어간다.
# kgeo_sub_addr 과 실패 기록 4종은 테이블이 없거나 성격이 달라 CSV 로만 남긴다.
# ---------------------------------------------------------------------------

TABLE_MAP = {
    'kgeo_land_owner_hist.csv': 'KGEO_OWNER_HIST_{s}',
    'kgeo_shrymblist.csv':      'KGEO_SHRYMBLIST_{s}',
    'kgeo_jigaRst.csv':         'KGEO_JIGARST_{s}',
    'kgeo_landLedgRst.csv':     'KGEO_LANDLEDGRST_{s}',
    'kgeo_bldgInfoRstList.csv': 'KGEO_BLDGINFO_{s}',
    'kgeo_flrList.csv':         'KGEO_FIRLIST_{s}',
    'kgeo_moveHistList.csv':    'KGEO_MOVEHIST_{s}',
}

# PNU 하나당 DB 로 가는 행이 평균 9행 정도다. 2000 으로 잡았더니 약 220 PNU 를
# 처리해야 첫 커밋이 일어나, 중간에 멈추면 그때까지 작업이 통째로 날아갔다.
# 행수와 시간 중 먼저 걸리는 쪽에서 커밋한다.
DB_BATCH_ROWS = 300      # 약 30 PNU
DB_FLUSH_SECONDS = 20    # 수집이 느려도 20초마다는 커밋한다

# 중단 요청. ThreadPoolExecutor 에 이미 쌓인 작업을 즉시 비우기 위한 플래그.
STOP = threading.Event()


def _bind(v):
    """파이썬 값 -> 바인드 값. 빈 값은 NULL 로 넣는다."""
    if v is None:
        return None
    s = str(v)
    return s if s != '' else None


class DbWriter:
    """행을 모아 executemany 로 적재한다.

    - flush 는 PNU 처리가 끝난 시점에만 일어난다. 한 PNU 의 행들이 여러 테이블에
      걸쳐 있는데 중간에 커밋되면, 중단 시 '완료로 보이지만 일부만 있는 PNU' 가
      생긴다. 이어받기가 그 PNU 를 건너뛰므로 데이터가 조용히 비게 된다.
    - 배치가 실패하면 한 행씩 다시 넣어 원인 행만 골라낸다. 한 행 때문에
      2000행이 통째로 날아가지 않게 하려는 것.
    """

    def __init__(self, suffix):
        self.suffix = suffix
        self.conn = None
        self._buf = {}        # table -> [row dict, ...]
        self._pending = 0
        self._last_flush = time.time()
        self._lock = threading.Lock()
        self.inserted = 0
        self.rejected = 0

    # -- 연결 ------------------------------------------------------------
    def connect(self):
        import cx_Oracle
        user, pw, dsn = (os.environ.get('DB_USER'), os.environ.get('DB_PASSWORD'),
                         os.environ.get('DB_DSN'))
        missing = [k for k, v in (('DB_USER', user), ('DB_PASSWORD', pw), ('DB_DSN', dsn)) if not v]
        if missing:
            raise RuntimeError(f'.env 에 {", ".join(missing)} 가 없습니다')
        self.conn = cx_Oracle.connect(user, pw, dsn, encoding='UTF-8')
        return self.conn

    def table_of(self, csvname):
        pat = TABLE_MAP.get(csvname)
        return pat.format(s=self.suffix) if pat else None

    # -- 사전 검증 -------------------------------------------------------
    def preflight(self, sample_columns):
        """테이블 존재와 컬럼 일치를 시작 전에 확인한다.

        20 분 돌린 뒤 'ORA-00942 테이블이 없습니다' 로 끝나는 일을 막는다.
        sample_columns: {csv파일명: [CSV 헤더...]}
        """
        cur = self.conn.cursor()
        problems = []
        for csvname, cols in sample_columns.items():
            tbl = self.table_of(csvname)
            if not tbl:
                continue
            cur.execute("""SELECT column_name, data_length FROM user_tab_columns
                            WHERE table_name = :t""", t=tbl)
            found = {r[0]: r[1] for r in cur.fetchall()}
            if not found:
                problems.append(f'{tbl} : 테이블이 없습니다')
                continue
            miss = [c for c in cols if c.upper() not in found]
            if miss:
                problems.append(f'{tbl} : 컬럼 없음 {", ".join(miss)}')
        cur.close()
        return problems

    # -- 기록 ------------------------------------------------------------
    def add(self, csvname, row):
        tbl = self.table_of(csvname)
        if not tbl:
            return
        with self._lock:
            self._buf.setdefault(tbl, []).append(row)
            self._pending += 1

    def pnu_done(self):
        """PNU 하나가 끝난 시점. 여기서만 flush 한다."""
        with self._lock:
            over = (self._pending >= DB_BATCH_ROWS or
                    (self._pending and time.time() - self._last_flush >= DB_FLUSH_SECONDS))
        if over:
            self.flush()
            CSV.flush()     # CSV 도 같이 내려써서 DB 와 어긋나지 않게 한다

    def flush(self):
        with self._lock:
            buf, self._buf, self._pending = self._buf, {}, 0
            self._last_flush = time.time()
        if not buf:
            return
        cur = self.conn.cursor()
        try:
            for tbl, rows in buf.items():
                self._insert(cur, tbl, rows)
            self.conn.commit()
        finally:
            cur.close()

    def _insert(self, cur, tbl, rows):
        cols = list(rows[0].keys())
        names = ', '.join(c.upper() for c in cols)
        binds = ', '.join(f':{i + 1}' for i in range(len(cols)))
        sql = f'INSERT INTO {tbl} ({names}) VALUES ({binds})'
        data = [[_bind(r.get(c)) for c in cols] for r in rows]
        try:
            cur.executemany(sql, data)
            self.inserted += len(data)
        except Exception as e:
            # 배치 실패 -> 한 행씩 넣어 문제 행만 분리한다
            print(f'[db] {tbl} 배치 실패, 행 단위 재시도: {type(e).__name__}', flush=True)
            for r, d in zip(rows, data):
                try:
                    cur.execute(sql, d)
                    self.inserted += 1
                except Exception as e2:
                    self.rejected += 1
                    CSV.write_safe('failed_step.csv', {
                        'pnu': str(r.get('PNU') or r.get('pnu') or ''),
                        'step': f'db:{tbl}',
                        'error': f'{type(e2).__name__}: {str(e2).strip().splitlines()[0]}',
                        'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    })

    # -- 이어받기 --------------------------------------------------------
    def done_pnus(self):
        """테이블에 이미 들어 있는 PNU. CSV 가 없어도 이어받기가 동작하게 한다."""
        done = set()
        cur = self.conn.cursor()
        for csvname in TABLE_MAP:
            tbl = self.table_of(csvname)
            try:
                cur.execute(f'SELECT DISTINCT PNU FROM {tbl}')
                done |= {str(r[0]).strip() for r in cur if r[0] is not None}
            except Exception as e:
                print(f'[resume] {tbl} 조회 실패(건너뜀): {type(e).__name__}')
        cur.close()
        done.discard('')
        return done

    def close(self):
        try:
            self.flush()
        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None


DB = None          # apps.py 가 실행 시 생성한다


def resolve_suffix(value):
    """접미사 검증. 테이블명에 문자열로 붙으므로 숫자만 허용한다."""
    s = (value or '').strip().strip('"').strip("'")
    if not s:
        raise ValueError('접미사가 비어 있습니다')
    if not s.isdigit():
        raise ValueError(f'숫자만 입력하세요: {s!r}')
    return s


def install_shutdown_guard():
    """종료 신호를 받으면 버퍼를 내려쓴 뒤 끝낸다.

    예전에는 finally 블록에만 의존했는데, Ctrl+C 나 창 닫기로 죽으면 실행되지
    않아 CSV 와 DB 버퍼가 통째로 날아갔다. 실제로 131 PNU 작업분이 사라졌다.
    """
    import atexit, signal

    def _drain(reason=''):
        if reason:
            print(f'\n[중단] {reason} - 지금까지 수집분을 저장하는 중입니다...', flush=True)
        try:
            if DB is not None:
                DB.flush()
        except Exception as e:
            print(f'[중단] DB 저장 실패: {type(e).__name__}: {e}', flush=True)
        try:
            CSV.flush()
        except Exception:
            pass

    atexit.register(_drain)

    def _on_signal(signum, frame):
        STOP.set()                      # 큐에 쌓인 작업을 즉시 비운다
        _drain('종료 요청을 받았습니다')
        print('[중단] 저장 완료. 다시 실행하면 이어서 진행합니다.', flush=True)

    for sig in ('SIGINT', 'SIGTERM', 'SIGBREAK'):
        s = getattr(signal, sig, None)
        if s is not None:
            try:
                signal.signal(s, _on_signal)
            except (ValueError, OSError):
                pass


def ask_suffix():
    """대상 테이블 접미사를 실행 시 입력받는다.

    TABLE_SUFFIX 환경변수(.env 포함)가 지정돼 있으면 묻지 않는다.
    스케줄러나 백그라운드 실행처럼 입력을 받을 수 없는 경우를 위한 것이다.
    """
    env_val = os.environ.get('TABLE_SUFFIX', '')
    if env_val.strip().strip('"').strip("'"):
        s = resolve_suffix(env_val)
        print(f'[db] 접미사 {s} - TABLE_SUFFIX 지정값을 사용합니다', flush=True)
        return s

    while True:
        try:
            raw = input('대상 테이블 접미사를 입력하세요 (숫자, 예: 1) > ')
        except EOFError:
            raise RuntimeError(
                '접미사를 입력받을 수 없습니다. 백그라운드 실행이면 .env 의 '
                'TABLE_SUFFIX 를 지정하세요')
        try:
            return resolve_suffix(raw)
        except ValueError as e:
            print(f'  {e}')
