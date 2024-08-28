from moduls import *
import crwaler_method
import sub_addrs_util


def go_run(cnt, pnu):
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




if __name__ == "__main__":
    df = pd.read_csv('input.csv')

    with ThreadPoolExecutor(max_workers=5) as executor:  # 변경 가능한 스레드 수
        for cnt, pnu in enumerate(df['PNU']):
            executor.submit(go_run, cnt+1, pnu)

    input(f"{cnt+1}건 수집 완료")