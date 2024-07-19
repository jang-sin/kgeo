from moduls import *

def go_run(cnt, pnu):
    print(pnu)
    if cnt == 1:
        print(f'[{cnt}] {datetime.now().strftime("%H:%M:%S")}')
    elif cnt % 100 == 0:
        print(f'[{cnt}] {datetime.now().strftime("%H:%M:%S")}')

    url = f'https://kgeop.go.kr/geopass/api/selectOneParcelInfo.do?pnu={pnu}'
    result = requests.get(url)
    jsondata = result.json()
    landOwnerShipHistList = jsondata['landOwnerShipHistList']
    parcelX = jsondata['addrResultFromPnuMap']['jusoResult']['jusoList'][0]['parcelX']
    parcelY = jsondata['addrResultFromPnuMap']['jusoResult']['jusoList'][0]['parcelY']
    lndpclAr = jsondata['landLedgRst'][0]['lndpclAr']
    lndcgrNm = jsondata['landLedgRst'][0]['lndcgrNm']
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

    # kgeo_shrymblist (공유지 연명부)
    if len(jsondata['shrYmbList']) >= 2:
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

    # 공시지가
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


if __name__ == "__main__":
    df = pd.read_csv('input.csv')

    with ThreadPoolExecutor(max_workers=5) as executor:  # 변경 가능한 스레드 수
        for cnt, pnu in enumerate(df['PNU']):
            executor.submit(go_run, cnt+1, pnu)

    input(f"{cnt+1}건 수집 완료")