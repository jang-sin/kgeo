from moduls import *
import crwaler_method
import sub_addrs_util

import traceback
from concurrent.futures import as_completed


_counter_lock = threading.Lock()
_done = 0
_started_at = None


def _progress(total):
    global _done
    with _counter_lock:
        _done += 1
        n = _done
    if not (n == 1 or n % 100 == 0 or n == total):
        return
    now = datetime.now()
    line = f'[{n}/{total}] {now.strftime("%H:%M:%S")}'
    if _started_at is not None and n > 1:
        el = (now - _started_at).total_seconds()
        rate = n / el if el > 0 else 0
        if rate > 0:
            eta = (total - n) / rate
            line += f'  {rate:.1f} PNU/s  남은시간 약 {eta/60:.0f}분'
    print(line, flush=True)


def _parcel_xy(jsondata):
    """좌표 추출. 주소 조회가 실패(code 974)하면 jusoResult 가 dict 가 아니라
    문자열("조회 결과 없음")로 오므로 방어적으로 접근한다.

    이 경우에도 landOwnerShipHistList / jigaRst / moveHistList 는 정상적으로
    내려온다. 좌표 2개 때문에 나머지를 통째로 버리면 안 된다.
    """
    addr = jsondata.get('addrResultFromPnuMap') or {}
    juso = addr.get('jusoResult')
    if isinstance(juso, dict):
        juso_list = juso.get('jusoList') or []
        if juso_list:
            return juso_list[0].get('parcelX'), juso_list[0].get('parcelY')
    return None, None


def _fail_owner(pnu, e):
    """소유자 정보(kgeo_land_owner_hist) 를 못 받은 PNU 만 failed_pnu.csv 에 남긴다.

    이 파일은 '재수집해야 하는 PNU 목록' 으로 쓰인다. 핵심 데이터가
    소유자 정보이므로, 그것을 확보하지 못한 경우만 여기에 들어간다.
    """
    CSV.write_safe('failed_pnu.csv', {
        'pnu': pnu,
        'error': f'{type(e).__name__}: {e}',
        'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })


def _fail_step(pnu, step, e):
    """소유자 정보 외 단계의 실패. failed_pnu.csv 와 섞지 않고 따로 남긴다.

    기록하지 않으면 콘솔 로그가 밀려나는 순간 무엇이 빠졌는지 알 수 없다.
    """
    CSV.write_safe('failed_step.csv', {
        'pnu': pnu,
        'step': step,
        'error': f'{type(e).__name__}: {e}',
        'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })


def go_run(cnt, pnu, total):
    """단일 PNU 에 대해 kgeo API 호출 및 데이터 파싱을 수행합니다."""
    _progress(total)

    url = f'https://kgeop.go.kr/geopass/api/selectOneParcelInfo.do?pnu={pnu}'
    seen_at = recovery_count()

    # --- 토지 조회 : 실패하면 소유자 정보도 못 받으므로 failed_pnu.csv ---
    try:
        jsondata = get_json(url)

        # 빈 응답(278바이트)은 두 가지를 뜻한다: 진짜 데이터 없음, 또는 소프트 차단.
        # looks_blocked() 가 카나리로 구분해 준다. 차단이었다면 대기+재연결 후
        # True 를 돌려주므로 이 PNU 를 한 번 더 시도한다.
        if is_empty_response(jsondata):
            if looks_blocked(seen_at):
                jsondata = get_json(url)
            if is_empty_response(jsondata):
                addr = jsondata.get('addrResultFromPnuMap') or {}
                CSV.write('empty_pnu.csv', {
                    'pnu': pnu,
                    'code': addr.get('code'),
                    'message': addr.get('message'),
                })
                return
    except Exception as e:
        _fail_owner(pnu, e)
        if not isinstance(e, (requests.exceptions.RequestException, KeyError, IndexError, TypeError)):
            traceback.print_exc()
        return

    parcelX, parcelY = _parcel_xy(jsondata)

    ### 소유자 정보 - 이 단계의 실패만 failed_pnu.csv 에 들어간다
    try:
        crwaler_method.kgeo_landOwnerShipHistList(jsondata, cnt, pnu, parcelX, parcelY)
    except Exception as e:
        _fail_owner(pnu, e)

    ### 나머지 단계 - 실패해도 failed_pnu.csv 에 넣지 않는다.
    # 각 단계를 격리해, 하나가 실패해도 뒤 단계가 통째로 날아가지 않게 한다.
    steps = [
        ('shrYmbList', lambda: crwaler_method.kgeo_shrYmbList(jsondata, pnu, parcelX, parcelY)
            if len(jsondata.get('shrYmbList') or []) >= 2 else None),
        ('jigaRst', lambda: crwaler_method.kgeo_jigaRst(jsondata, pnu)),
        ('landLedgRst', lambda: crwaler_method.kgeo_landLedgRst(jsondata, pnu)),
        ('bldgInfoRstList', lambda: crwaler_method.kgeo_bldgInfoRstList(jsondata, pnu)),
        # 같은 URL 을 다시 호출하지 않는다. 위에서 받은 jsondata 를 그대로 쓴다.
        ('moveHistList', lambda: crwaler_method.kgeo_moveHistList(jsondata, pnu)),
        ('sub_addr', lambda: sub_addrs_util.get_sub_addr(jsondata, pnu)),
    ]
    for name, fn in steps:
        try:
            fn()
        except Exception as e:
            _fail_step(pnu, name, e)


if __name__ == "__main__":
    # RESUME=0 이면 기존 결과를 백업으로 밀어내고 처음부터 다시 받는다.
    # 기본은 이어받기 - 차단으로 중단된 실행을 그대로 이어서 진행한다.
    resume = os.environ.get('RESUME', '1') != '0'

    if not resume:
        backup_outputs()

    warm_up()

    # dtype=str 필수. 빈 행이 하나라도 있으면 PNU 컬럼이 float64 가 되어
    # 4.159033025200010e+18 처럼 망가진 채로 URL 에 들어간다.
    df = pd.read_csv('input.csv', dtype=str)
    pnus = [p.strip() for p in df['PNU'].dropna().tolist() if p.strip()]
    all_count = len(pnus)

    if resume:
        done = load_done_pnus()

        # 소유자 정보만 실패한 PNU 는 다른 파일에 행이 남아 있어 '완료' 로 잡힌다.
        # 그대로 두면 재시도되지 않으므로 완료 목록에서 빼준다.
        retry = set()
        if os.path.isfile('failed_pnu.csv'):
            try:
                retry = set(pd.read_csv('failed_pnu.csv', dtype=str)['pnu'].dropna().str.strip())
            except Exception as e:
                print(f'[resume] failed_pnu.csv 읽기 실패: {type(e).__name__}: {e}')
            os.remove('failed_pnu.csv')
        if retry:
            done -= retry
            print(f'[resume] 소유자 정보 실패 {len(retry):,}건 재시도 대상에 포함', flush=True)
            print('         (해당 PNU 는 다른 파일에 기존 행이 남아 중복될 수 있습니다)', flush=True)

        if done:
            pnus = [p for p in pnus if p not in done]
            print(f'[resume] 완료 {len(done):,}건 건너뜀', flush=True)

    total = len(pnus)
    if total == 0:
        print('처리할 PNU 가 없습니다. 이미 전부 수집되었습니다.', flush=True)
        sys.exit(0)
    print(f'{all_count:,}건 중 {total:,}건 수집 시작 '
          f'(워커 {MAX_WORKERS}개, 연결 재사용)', flush=True)

    started = datetime.now()
    _started_at = started
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(go_run, i + 1, pnu, total)
                       for i, pnu in enumerate(pnus)]
            for fut in as_completed(futures):
                fut.result()   # 삼켜지던 예외를 드러낸다
    finally:
        CSV.close()

    elapsed = (datetime.now() - started).total_seconds()
    print(f'{total}건 수집 완료 - {elapsed/60:.1f}분 ({total/elapsed:.1f} PNU/s)', flush=True)
    try:
        input('Press Enter to exit...')
    except EOFError:
        pass   # 백그라운드/파이프 실행 시 stdin 이 없다
