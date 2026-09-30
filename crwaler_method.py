from moduls import *


### 소유자 정보
def kgeo_landOwnerShipHistList(jsondata, cnt, pnu, parcelX, parcelY):
    """
    소유자 정보 데이터를 파싱하고, CSV로 저장합니다.
    - jsondata: API 응답 JSON 데이터.
    - cnt: 현재 작업 순번.
    - pnu, parcelX, parcelY: 추가 정보.
    """
    landOwnerShipHistList = jsondata.get('landOwnerShipHistList') or []
    for i, landOwnerShipHist in enumerate(landOwnerShipHistList):
        seq = len(landOwnerShipHistList) - i
        CSV.write('kgeo_land_owner_hist.csv', {
            'cnt': cnt,
            'PNU': pnu,
            'SEQ': seq,
            'OWNSHIPCHANGEHISTSN': landOwnerShipHist.get('ownshipChangeHistSn'),
            'OWNSHIPCHGCSNM': landOwnerShipHist.get('ownshipChgcsNm'),     # 변동사유
            'OWNSHIPCHANGEDE': landOwnerShipHist.get('ownshipChangeDe'),   # 변동일자
            'POSESNTYNM': landOwnerShipHist.get('posesnTyNm'),             # 소유구분
            'OWNERREGNOENCPT': landOwnerShipHist.get('ownerRegnoEncpt'),   # 소유자 주민/법인번호
            'OWNERNMENCPT': landOwnerShipHist.get('ownerNmEncpt'),         # 소유자 이름
            'OWNERADRES': landOwnerShipHist.get('ownerAdres'),             # 소유자 주소
            'PARCELX': parcelX,
            'PARCELY': parcelY,
        })


### 공유지 연명부
def kgeo_shrYmbList(jsondata, pnu, parcelX, parcelY):
    shrYmbList = jsondata.get('shrYmbList') or []
    for z, shrYmb in enumerate(shrYmbList):
        seq = len(shrYmbList) - z
        CSV.write('kgeo_shrymblist.csv', {
            'PNU': pnu,
            'SEQ': seq,
            'OWNSHIPCHGCSNM': shrYmb.get('ownshipChgcsNm'),     # 변동사유
            'OWNSHIPCHANGEDE': shrYmb.get('ownshipChangeDe'),   # 변동일자
            'POSESNTYNM': shrYmb.get('posesnTyNm'),             # 소유구분
            'OWNERREGNOENCPT': shrYmb.get('ownerRegnoEncpt'),   # 소유자 주민번호
            'OWNERNMENCPT': shrYmb.get('ownerNmEncpt'),         # 소유자 이름
            'COCNRSN': shrYmb.get('cocnrSn'),
            'OWNERADRES': shrYmb.get('ownerAdres'),             # 소유자주소
            'OWNSHIPQOTACN': shrYmb.get('ownshipQotaCn'),       # 소유지분
            'PARCELX': parcelX,
            'PARCELY': parcelY,
        })


### 공시지가
def kgeo_jigaRst(jsondata, pnu):
    for j in (jsondata.get('jigaRst') or [])[:4]:
        CSV.write('kgeo_jigaRst.csv', {
            'PNU': pnu,
            'stdrDe': j.get('stdrDe'),                 # 기준년월
            'pblntfDe': j.get('pblntfDe'),             # 공시일자
            'jiga': j.get('indvdlzPblntfPclnd'),       # 공시지가(원)
        })


### landLedgRst (뭔지 모르겠음, kgeo 화면에 없는 데이터)
def kgeo_landLedgRst(jsondata, pnu):
    for land_rst in (jsondata.get('landLedgRst') or []):
        CSV.write('kgeo_landLedgRst.csv', {
            'PNU': pnu,
            'admSectNm': land_rst.get('admSectNm'),
            'lndcgrNm': land_rst.get('lndcgrNm'),
            'lndcgrCode': land_rst.get('lndcgrCode'),
            'ladMvmnDe': land_rst.get('ladMvmnDe'),
            'ladMvmnResnNm': land_rst.get('ladMvmnResnNm'),
            'ownshipChgcsNm': land_rst.get('ownshipChgcsNm'),
            'ownshipChangeDe': land_rst.get('ownshipChangeDe'),
            'posesnTyNm': land_rst.get('posesnTyNm'),
            'posesnTyCode': land_rst.get('posesnTyCode'),
            'ownerRegno': land_rst.get('ownerRegno'),
            'ownerNmEncpt': land_rst.get('ownerNmEncpt'),
            'lndpclAr': land_rst.get('lndpclAr'),
            'pblonsipNmprCo': land_rst.get('pblonsipNmprCo'),
        })


### 건축물 정보(기본현황, 층별현황)
def kgeo_bldgInfoRstList(jsondata, pnu):
    for build_info in (jsondata.get('bldgInfoRstList') or []):
        # 건축물 정보를 얻기위한 건물 목록 데이터 받아오기
        buldKndCode = build_info.get('buldKndCode')
        buldIdno = build_info.get('buldIdno')

        # 기본현황 - 공용 Session 사용(연결 재사용 + 타임아웃 + 재시도)
        building_url = (
            f'https://kgeop.go.kr/geopass/api/estateOne-bldg-info.do'
            f'?buldKndCode={buldKndCode}&pnu={pnu}&buldIdno={buldIdno}'
        )
        try:
            building_json = get_json(building_url)
        except Exception as e:
            # 동 하나를 못 받아도 그 PNU 의 다른 데이터는 살린다.
            # 다만 buldKndCode/buldIdno 를 남겨 두면 나중에 그 동만 콕 집어
            # estateOne-bldg-info.do 를 다시 호출해 복구할 수 있다.
            print(f'[bldg] {pnu}/{buldIdno} 실패: {type(e).__name__}: {e}')
            CSV.write_safe('failed_bldg.csv', {
                'pnu': pnu,
                'buldKndCode': buldKndCode,
                'buldIdno': buldIdno,
                'error': f'{type(e).__name__}: {e}',
                'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
            continue

        building_data = building_json.get('resultVo') or {}

        CSV.write('kgeo_bldgInfoRstList.csv', {
            'PNU': pnu,
            'buldKndNm': build_info.get('buldKndNm'),
            'buldNm': build_info.get('buldNm'),
            'bulddongNm': build_info.get('bulddongNm'),

            'larea': building_data.get('larea'),
            'barea': building_data.get('barea'),
            'garea': building_data.get('garea'),
            'fsiCalcGarea': building_data.get('fsiCalcGarea'),
            'blr': building_data.get('blr'),

            'fsi': building_data.get('fsi'),
            'hehdCnt': building_data.get('hehdCnt'),
            'hoCnt': building_data.get('hoCnt'),
            'fmlyCnt': building_data.get('fmlyCnt'),
            'parkCnt': building_data.get('parkCnt'),

            'mainUseNm': building_data.get('mainUseNm'),
            'etcUse': building_data.get('etcUse'),
            'struNm': building_data.get('struNm'),
            'etcStru': building_data.get('etcStru'),
            'roofNm': building_data.get('roofNm'),

            'etcRoof': building_data.get('etcRoof'),
            'mainBldgCnt': building_data.get('mainBldgCnt'),
            'subBldgCnt': building_data.get('subBldgCnt'),
            'subBldgArea': building_data.get('subBldgArea'),
            'permYmd': building_data.get('permYmd'),

            'bgconsYmd': building_data.get('bgconsYmd'),
            'useAprvYmd': building_data.get('useAprvYmd'),
            'repJibun': building_data.get('repJibun'),
            'relJibun': building_data.get('relJibun'),
            'buldKndCode': buldKndCode,

            'buldIdno': buldIdno,
        })

        # 층별현황
        for flr in (building_json.get('flrList') or []):
            CSV.write('kgeo_flrList.csv', {
                'PNU': pnu,
                'flrGbnNm': flr.get('flrGbnNm'),
                'flr': flr.get('flr'),
                'etcStru': flr.get('etcStru'),
                'etcUse': flr.get('etcUse'),
                'btmArea': flr.get('btmArea'),
                'buldKndCode': buldKndCode,
                'buldIdno': buldIdno,
            })


### 토지이동 연혁
def kgeo_moveHistList(jsondata, pnu):
    """selectOneParcelInfo.do 응답에 moveHistList 가 이미 들어 있다.
    예전엔 같은 URL 을 한 번 더 호출해서 요청 수가 2배였다."""
    for hist in (jsondata.get('moveHistList') or []):
        CSV.write('kgeo_moveHistList.csv', {
            'PNU': pnu,
            'lndcgrNm': hist.get('lndcgrNm'),
            'lndpclAr': hist.get('lndpclAr'),
            'ladMvmnDe': hist.get('ladMvmnDe'),
            'ladMvmnResnNm': hist.get('ladMvmnResnNm'),
        })
