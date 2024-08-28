from moduls import *


def extract_address(address):
    pattern = r'.+(리|동)'
    match = re.search(pattern, address)
    if match:
        return match.group(0)
    return None


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
    try:
        if ('addrResultFromPnuMap' in jsondata and
                'jusoResult' in jsondata['addrResultFromPnuMap'] and
                'jusoList' in jsondata['addrResultFromPnuMap']['jusoResult'] and
                len(jsondata['addrResultFromPnuMap']['jusoResult']['jusoList']) > 0):

            relJibun = jsondata['addrResultFromPnuMap']['jusoResult']['jusoList'][0].get('relJibun')

            if relJibun:
                relJibun_list = relJibun.split(",")
                if relJibun_list[0] != "":
                    juso_address = extract_address(relJibun_list[0])
                    for sample_bunji in relJibun_list:
                        total_juso = f"{juso_address} {sample_bunji.replace(juso_address, '').strip()}"
                        # pnu, 주소
                        result_sub = pd.DataFrame({
                            'PNU': [pnu],
                            'ADDR': [total_juso],

                        })

                        # 파일이 존재하는지 확인
                        file_exists = os.path.isfile('kgeo_sub_addr.csv')
                        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
                        result_sub.to_csv('kgeo_sub_addr.csv', mode='a', header=not file_exists, index=False)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except KeyError as e:
        print(f"KeyError: {e}")
    except cx_Oracle.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
