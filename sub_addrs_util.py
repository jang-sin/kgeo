from moduls import *


def extract_address(address):
    pattern = r'.+(리|동)'
    match = re.search(pattern, address)
    if match:
        return match.group(0)
    return None


def merge_jibun(rep_tokens, entry):
    """대표지번 주소에 관련지번 한 건을 합쳐 완전한 주소를 만든다.

    relJibun 의 각 항목은 행정구역을 몇 단계까지 적어 주는지가 제각각이다.
        '457'                      -> 지번만
        '천리 179-12'               -> 리부터
        '이동읍 천리 179-12'         -> 읍면부터
        '경기도 구리시 아천동 42-2'    -> 전체

    그래서 대표주소를 앞에 그냥 붙이면 관련지번이 다른 동/리일 때
    '경기도 용인시 처인구 이동읍 덕성리 이동읍 천리 179-12' 처럼
    서로 다른 두 주소가 이어 붙는다. 항목이 스스로 적어 준 단계만큼
    대표주소의 뒤쪽을 덮어써야 한다.
    """
    tokens = entry.split()
    if not tokens:
        return None
    jibun = tokens[-1]
    adm = tokens[:-1]                      # 항목이 직접 명시한 행정구역

    if len(adm) >= len(rep_tokens):
        merged = adm                       # 항목이 대표주소만큼 상세하다
    else:
        merged = rep_tokens[:len(rep_tokens) - len(adm)] + adm

    # 원본이 '고양시 일산동구-식사동' 처럼 적어 주는 경우가 있어
    # 겹치는 행정구역명이 연달아 남는 것을 정리한다.
    cleaned = []
    for t in merged:
        if not cleaned or cleaned[-1] != t:
            cleaned.append(t)

    return ' '.join(cleaned + [jibun])


def db_insert(pnu, total_juso, oracle_cursor, oracle_connection):
    insert_query = """INSERT INTO kgeo_sub_addr(PNU,ADDR) VALUES (:1, :2)"""
    data_list = [pnu, total_juso]
    oracle_cursor.execute(insert_query, data_list)
    oracle_connection.commit()


def addr_split(total_juso, oracle_cursor, oracle_connection):
    addr_splitting = f"""
                           UPDATE kgeo_sub_addr a
                           SET (ADDR_1, ADDR_2, ADDR_3, ADDR_4, ADDR_5) =
                               (SELECT b.ADDR_1, b.ADDR_2, b.ADDR_3, b.ADDR_4, b.ADDR_5
                                FROM TABLE(sf_tbl_post('{total_juso}')) b)
                           WHERE addr_5 IS NULL and ADDR = '{total_juso}'
                       """
    oracle_cursor.execute(addr_splitting)  # 분할주소 입력
    oracle_connection.commit()


def get_sub_pnu(total_juso, oracle_cursor, oracle_connection):
    create_pnu_query = f"""
                   UPDATE kgeo_sub_addr
                   SET SUB_PNU = sf_conv_addr_to_pnu(ADDR_1, ADDR_2, ADDR_3, ADDR_4, ADDR_5)
                   WHERE SUB_PNU IS NULL and ADDR = '{total_juso}'
                """

    oracle_cursor.execute(create_pnu_query)
    oracle_connection.commit()


def get_sub_addr(jsondata, pnu):
    """부속지번(관련지번)을 kgeo_sub_addr.csv 에 기록한다.

    예외를 자체적으로 삼키지 않는다. 호출자(apps.go_run)가 단계별로 잡아
    failed_step.csv 에 남기므로, 여기서 print 로 끝내면 실패가 어느 파일에도
    기록되지 않는다.
    """
    addr = jsondata.get('addrResultFromPnuMap') or {}
    juso = addr.get('jusoResult')
    if not isinstance(juso, dict):
        return            # code 974 등: jusoResult 가 문자열이면 부속지번이 없다
    juso_list = juso.get('jusoList') or []
    if not juso_list:
        return

    relJibun = juso_list[0].get('relJibun')
    if not relJibun:
        return

    relJibun_list = relJibun.split(",")
    if relJibun_list[0].strip() == "":
        return

    juso_address = extract_address(relJibun_list[0])
    if not juso_address:
        return

    rep_tokens = juso_address.split()
    seen = set()
    for sample_bunji in relJibun_list:
        total_juso = merge_jibun(rep_tokens, sample_bunji.strip())
        if not total_juso or total_juso in seen:
            continue      # 원본에 같은 지번이 중복 기재된 경우
        seen.add(total_juso)
        # pnu, 주소
        CSV.write('kgeo_sub_addr.csv', {
            'PNU': pnu,
            'ADDR': total_juso,
        })
