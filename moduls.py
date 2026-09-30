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
_DONE_SOURCES = [
    ('kgeo_land_owner_hist.csv', 'PNU'),
    ('kgeo_jigaRst.csv', 'PNU'),
    ('kgeo_landLedgRst.csv', 'PNU'),
    ('kgeo_moveHistList.csv', 'PNU'),
    ('kgeo_shrymblist.csv', 'PNU'),
    ('kgeo_bldgInfoRstList.csv', 'PNU'),
    ('kgeo_sub_addr.csv', 'PNU'),
    ('empty_pnu.csv', 'pnu'),
]


def load_done_pnus():
    """이미 처리된 PNU 집합을 기존 결과 파일에서 읽는다.

    차단으로 중단된 실행을 이어받기 위한 것. failed_pnu.csv 는 일부러 제외한다
    - 차단 때문에 실패한 건이라 다시 시도해야 한다.
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
