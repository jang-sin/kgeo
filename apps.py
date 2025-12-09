from moduls import *
import crwaler_method
import sub_addrs_util


def go_run(cnt, pnu):
    """
    단일 PNU에 대해 kgeo API 호출 및 데이터 파싱을 수행합니다.
    - cnt: 현재 작업의 순번.
    - pnu: 토지 고유번호.
    """
    try:
        print("dddd")
        print(pnu)

        if cnt == 1:
            print(f'[{cnt}] {datetime.now().strftime("%H:%M:%S")}')
        elif cnt % 100 == 0:
            print(f'[{cnt}] {datetime.now().strftime("%H:%M:%S")}')

        url = f'https://kgeop.go.kr/geopass/api/selectOneParcelInfo.do?pnu={pnu}'
        result = requests.get(url)
        jsondata = result.json()

        parcelX = jsondata['addrResultFromPnuMap']['jusoResult']['jusoList'][0]['parcelX']
        parcelY = jsondata['addrResultFromPnuMap']['jusoResult']['jusoList'][0]['parcelY']
        lndpclAr = jsondata['landLedgRst'][0]['lndpclAr']
        lndcgrNm = jsondata['landLedgRst'][0]['lndcgrNm']

        ### 소유자 정보
        crwaler_method.kgeo_landOwnerShipHistList(jsondata,cnt,pnu,parcelX, parcelY)

        ### 공유지 연명부
        if len(jsondata['shrYmbList']) >= 2:
            crwaler_method.kgeo_shrYmbList(jsondata, pnu, parcelX, parcelY)

        ### 공시지가
        crwaler_method.kgeo_jigaRst(jsondata, pnu)

        ### landLedgRst (뭔지 모르겠음, kgeo 화면에 없는 데이터)
        crwaler_method.kgeo_landLedgRst(jsondata, pnu)

        ### 건축물 정보(기본현황, 층별현황)
        crwaler_method.kgeo_bldgInfoRstList(jsondata, pnu)

        ### 토지이동 연혁
        crwaler_method.kgeo_moveHistList(pnu)

        ### 부속지번
        sub_addrs_util.get_sub_addr(jsondata, pnu)
    except:
        # ❌ 실패 처리 (바로 CSV에 저장)
        file_path = "failed_pnu.csv"
        file_exists = os.path.exists(file_path)

        df = pd.DataFrame(
            [[pnu, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]],
            columns=["pnu", "timestamp"]
        )

        df.to_csv(
            file_path,
            mode="a",
            index=False,
            header=not file_exists,  # 파일 없으면 헤더 추가
            encoding="utf-8-sig"
        )




if __name__ == "__main__":
    df = pd.read_csv('input.csv')
    pnus = [
        '111111','222222','343333'
    ]
    with ThreadPoolExecutor(max_workers=5) as executor:  # 변경 가능한 스레드 수
        # for cnt, pnu in enumerate(df['PNU']):
        for cnt, pnu in enumerate(pnus):
            executor.submit(go_run, cnt+1, pnu)

    input(f"{cnt+1}건 수집 완료")