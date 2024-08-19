from moduls import *


### 소유자 정보
def kgeo_landOwnerShipHistList(jsondata, cnt, pnu, parcelX, parcelY):
    landOwnerShipHistList = jsondata['landOwnerShipHistList']
    for i, landOwnerShipHist in enumerate(landOwnerShipHistList):
        seq = len(landOwnerShipHistList) - i
        ownshipChangeHistSn = landOwnerShipHist['ownshipChangeHistSn']  # ??
        ownshipChgcsNm = landOwnerShipHist['ownshipChgcsNm']  # 변동사유
        ownshipChangeDe = landOwnerShipHist['ownshipChangeDe']  # 변동일자
        posesnTyNm = landOwnerShipHist['posesnTyNm']  # 소유구분
        ownerRegnoEncpt = landOwnerShipHist['ownerRegnoEncpt']  # 소유자 주민/법인번호
        ownerNmEncpt = landOwnerShipHist['ownerNmEncpt']  # 소유자 이름
        ownerAdres = landOwnerShipHist['ownerAdres']  # 소유자 주소

        result_hist = pd.DataFrame({
            'cnt': [cnt],
            'PNU': [pnu],
            'SEQ': [seq],
            'OWNSHIPCHANGEHISTSN': [ownshipChangeHistSn],
            'OWNSHIPCHGCSNM': [ownshipChgcsNm],
            'OWNSHIPCHANGEDE': [ownshipChangeDe],
            'POSESNTYNM': [posesnTyNm],
            'OWNERREGNOENCPT': [ownerRegnoEncpt],
            'OWNERNMENCPT': [ownerNmEncpt],
            'OWNERADRES': [ownerAdres],
            'PARCELX': [parcelX],
            'PARCELY': [parcelY],

        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_land_owner_hist.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_hist.to_csv('kgeo_land_owner_hist.csv', mode='a', header=not file_exists, index=False)


### 공유지 연명부
def kgeo_shrYmbList(jsondata, pnu, parcelX, parcelY):
    shrYmbList = jsondata['shrYmbList']
    for z, shrYmb in enumerate(shrYmbList):
        seq = len(shrYmbList) - z
        ownshipChgcsNm = shrYmb['ownshipChgcsNm']  # 변동사유
        ownshipChangeDe = shrYmb['ownshipChangeDe']  # 변동일자
        posesnTyNm = shrYmb['posesnTyNm']  # 소유구분
        ownerRegnoEncpt = shrYmb['ownerRegnoEncpt']  # 소유자 주민번호
        ownerNmEncpt = shrYmb['ownerNmEncpt']  # 소유자 이름
        cocnrSn = shrYmb['cocnrSn']  # ???
        ownerAdres = shrYmb['ownerAdres']  # 소유자주소
        ownshipQotaCn = shrYmb['ownshipQotaCn']  # 소유지분

        result_shrYmb = pd.DataFrame({
            'PNU': [pnu],
            'SEQ': [seq],
            'OWNSHIPCHGCSNM': [ownshipChgcsNm],
            'OWNSHIPCHANGEDE': [ownshipChangeDe],
            'POSESNTYNM': [posesnTyNm],
            'OWNERREGNOENCPT': [ownerRegnoEncpt],
            'OWNERNMENCPT': [ownerNmEncpt],
            "COCNRSN": [cocnrSn],
            'OWNERADRES': [ownerAdres],
            'OWNSHIPQOTACN': [ownshipQotaCn],
            'PARCELX': [parcelX],
            'PARCELY': [parcelY],
        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_shrymblist.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_shrYmb.to_csv('kgeo_shrymblist.csv', mode='a', header=not file_exists, index=False)


### 공시지가
def kgeo_jigaRst(jsondata, pnu):
    for j in jsondata['jigaRst']:
        stdrDe = j['stdrDe']  # 기준년월
        pblntfDe = j['pblntfDe']  # 공시일자
        jiga = j['indvdlzPblntfPclnd']  # 공시자가(원)

        result_shrYmb = pd.DataFrame({
            'PNU': [pnu],
            'stdrDe': [stdrDe],
            'pblntfDe': [pblntfDe],
            'jiga': [jiga],
        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_jigaRst.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_shrYmb.to_csv('kgeo_jigaRst.csv', mode='a', header=not file_exists, index=False)


### landLedgRst (뭔지 모르겠음, kgeo 화면에 없는 데이터)
def kgeo_landLedgRst(jsondata, pnu):
    for land_rst in jsondata['landLedgRst']:
        admSectNm = land_rst['admSectNm']
        lndcgrNm = land_rst['lndcgrNm']
        lndcgrCode = land_rst['lndcgrCode']
        ladMvmnDe = land_rst['ladMvmnDe']
        ladMvmnResnNm = land_rst['ladMvmnResnNm']
        ownshipChgcsNm = land_rst['ownshipChgcsNm']
        ownshipChangeDe = land_rst['ownshipChangeDe']
        posesnTyNm = land_rst['posesnTyNm']
        posesnTyCode = land_rst['posesnTyCode']
        ownerRegno = land_rst['ownerRegno']
        ownerNmEncpt = land_rst['ownerNmEncpt']
        lndpclAr = land_rst['lndpclAr']
        pblonsipNmprCo = land_rst['pblonsipNmprCo']

        result_landLedgRst = pd.DataFrame({
            'PNU': [pnu],
            'admSectNm': [admSectNm],
            'lndcgrNm': [lndcgrNm],
            'lndcgrCode': [lndcgrCode],
            'ladMvmnDe': [ladMvmnDe],
            'ladMvmnResnNm': [ladMvmnResnNm],
            'ownshipChgcsNm': [ownshipChgcsNm],
            "ownshipChangeDe": [ownshipChangeDe],
            'posesnTyNm': [posesnTyNm],
            'posesnTyCode': [posesnTyCode],
            'ownerRegno': [ownerRegno],
            'ownerNmEncpt': [ownerNmEncpt],
            'lndpclAr': [lndpclAr],
            'pblonsipNmprCo': [pblonsipNmprCo],

        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_landLedgRst.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_landLedgRst.to_csv('kgeo_landLedgRst.csv', mode='a', header=not file_exists, index=False)


### 건축물 정보(기본현황, 층별현황)
def kgeo_bldgInfoRstList(jsondata, pnu):
    for build_info in jsondata['bldgInfoRstList']:
        # 건축물 정보를 얻기위한 건물 목록 데이터 받아오기
        buldKndNm = build_info['buldKndNm']
        buldKndCode = build_info['buldKndCode']
        buldNm = build_info['buldNm']
        bulddongNm = build_info['bulddongNm']
        buldIdno = build_info['buldIdno']

        # 기본현황
        building_url = f'https://kgeop.go.kr/geopass/api/estateOne-bldg-info.do?buldKndCode={buldKndCode}&pnu={pnu}&buldIdno={buldIdno}'
        building_result = requests.get(building_url)
        building_data = building_result.json()["resultVo"]

        larea = building_data['larea']
        barea = building_data['barea']
        garea = building_data['garea']
        fsiCalcGarea = building_data['fsiCalcGarea']
        blr = building_data['blr']
        fsi = building_data['fsi']
        hehdCnt = building_data['hehdCnt']
        hoCnt = building_data['hoCnt']
        fmlyCnt = building_data['fmlyCnt']
        parkCnt = building_data['parkCnt']

        mainUseNm = building_data['mainUseNm']
        etcUse = building_data['etcUse']
        struNm = building_data['struNm']
        etcStru = building_data['etcStru']
        roofNm = building_data['roofNm']
        etcRoof = building_data['etcRoof']
        mainBldgCnt = building_data['mainBldgCnt']
        subBldgCnt = building_data['subBldgCnt']
        subBldgArea = building_data['subBldgArea']
        permYmd = building_data['permYmd']

        bgconsYmd = building_data['bgconsYmd']
        useAprvYmd = building_data['useAprvYmd']
        repJibun = building_data['repJibun']
        relJibun = building_data['relJibun']

        result_bldgInfoRstList = pd.DataFrame({
            'PNU': [pnu],
            'buldKndNm': [buldKndNm],
            'buldNm': [buldNm],
            'bulddongNm': [bulddongNm],

            'larea': [larea],
            'barea': [barea],
            'garea': [garea],
            'fsiCalcGarea': [fsiCalcGarea],
            'blr': [blr],

            'fsi': [fsi],
            "hehdCnt": [hehdCnt],
            'hoCnt': [hoCnt],
            'fmlyCnt': [fmlyCnt],
            'parkCnt': [parkCnt],

            'mainUseNm': [mainUseNm],
            'etcUse': [etcUse],
            'struNm': [struNm],
            'etcStru': [etcStru],
            'roofNm': [roofNm],

            'etcRoof': [etcRoof],
            'mainBldgCnt': [mainBldgCnt],
            'subBldgCnt': [subBldgCnt],
            'subBldgArea': [subBldgArea],
            'permYmd': [permYmd],

            'bgconsYmd': [bgconsYmd],
            'useAprvYmd': [useAprvYmd],
            'repJibun': [repJibun],
            'relJibun': [relJibun],
            'buldKndCode': [buldKndCode],

            'buldIdno': [buldIdno],

        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_bldgInfoRstList.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_bldgInfoRstList.to_csv('kgeo_bldgInfoRstList.csv', mode='a', header=not file_exists, index=False)

        # 층별현황
        try:
            floor_list = building_result.json()["flrList"]
        except:
            continue
        for flr in floor_list:
            flrGbnNm = flr['flrGbnNm']
            flr_ = flr['flr']
            etcStru = flr['etcStru']
            etcUse = flr['etcUse']
            btmArea = flr['btmArea']

            result_flrList = pd.DataFrame({
                'PNU': [pnu],

                'flrGbnNm': [flrGbnNm],
                'flr': [flr_],
                'etcStru': [etcStru],
                'etcUse': [etcUse],
                'btmArea': [btmArea],

                'buldKndCode': [buldKndCode],
                'buldIdno': [buldIdno],
            })

            # 파일이 존재하는지 확인
            file_exists = os.path.isfile('kgeo_flrList.csv')
            # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
            result_flrList.to_csv('kgeo_flrList.csv', mode='a', header=not file_exists, index=False)


### 토지이동 연혁
def kgeo_moveHistList(pnu):
    moveHist_url = f'https://kgeop.go.kr/geopass/api/selectOneParcelInfo.do?pnu={pnu}'
    moveHist_result = requests.get(moveHist_url)
    moveHist_jsondata = moveHist_result.json()

    for hist in moveHist_jsondata['moveHistList']:
        lndcgrNm = hist['lndcgrNm']
        lndpclAr = hist['lndpclAr']
        ladMvmnDe = hist['ladMvmnDe']
        ladMvmnResnNm = hist['ladMvmnResnNm']

        result_moveHistList = pd.DataFrame({
            'PNU': [pnu],
            'lndcgrNm': [lndcgrNm],
            'lndpclAr': [lndpclAr],
            'ladMvmnDe': [ladMvmnDe],
            'ladMvmnResnNm': [ladMvmnResnNm],

        })

        # 파일이 존재하는지 확인
        file_exists = os.path.isfile('kgeo_moveHistList.csv')
        # 파일이 존재하지 않으면 헤더 포함하여 저장, 존재하면 헤더 없이 추가
        result_moveHistList.to_csv('kgeo_moveHistList.csv', mode='a', header=not file_exists, index=False)
