import os
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup as bs
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import time
import csv
from dotenv import load_dotenv

load_dotenv()
MAX_WORKERS = int(os.getenv("STOCK_ANALYSIS_WORKERS", "2"))

# 네이버 주식
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
url = 'https://finance.naver.com/item/sise_day.nhn?'

# KOSPI 데이터 로드
def get_ticker_list():
    ticker_list = []
    with open('kospi_data_20241231.csv', mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            ticker_list.append({'ticker': row['ticker'], 'name': row['name']})
    return ticker_list

# 예측 데이터 처리 함수
def process_stock_data(ticker_info):
    ticker = ticker_info['ticker']
    name = ticker_info['name']

    dfs = []  # DataFrame을 담을 리스트
    code_url = f'{url}code={ticker}'

    for page in range(1, 8):
        page_url = f'{code_url}&page={page}'

        response = requests.get(page_url, headers=headers)
        html = bs(response.text, 'html.parser')
        html_table = html.select_one("table")  # 첫 번째 테이블만 가져옵니다.

        if html_table:  # 테이블이 존재할 경우에만 처리
            table_html = str(html_table)
            table = pd.read_html(StringIO(table_html))  # StringIO로 HTML 문자열 감싸기

            # 현재 데이터를 리스트에 추가
            dfs.append(table[0].dropna())

    # 모든 데이터를 하나의 DataFrame으로 병합
    df = pd.concat(dfs, ignore_index=True)

    # 날짜를 datetime 형식으로 변환
    df["날짜"] = pd.to_datetime(df["날짜"], format="%Y.%m.%d")

    # 정렬 (최근 날짜부터 오름차순으로)
    df = df.sort_values("날짜")

    # 5일, 20일, 60일 이동평균 계산
    df["MA5"] = df["종가"].rolling(window=5).mean()
    df["MA20"] = df["종가"].rolling(window=20).mean()
    df["MA60"] = df["종가"].rolling(window=60).mean()

    # 기준선 (26일 기준 고가, 저가의 평균) 계산
    df["Baseline"] = ((df["고가"].rolling(window=26).max() + df["저가"].rolling(window=26).min()) / 2).fillna(0).round().astype(int)
    # 전환선 (9일 기준 고가, 저가의 평균) 계산
    df["ConversionLine"] = ((df["고가"].rolling(window=9).max() + df["저가"].rolling(window=9).min()) / 2).fillna(0).round().astype(int)

    # 이격도 계산 및 예측
    df["Disparity"] = (df["MA5"] / df["MA20"]) * 100

    # 예측값 설정
    df["초단기예측"] = None  # 초기값 설정
    df["단기예측"] = None  # 초기값 설정
    df["전환예측"] = None  # 초기값 설정
    df["이격도예측"] = None  # 초기값 설정

    # 상승/하락 예측 조건 설정 (위의 예측 조건들 포함)

    # 초단기 예측값 설정
    condition_down = (df["MA5"] < df["MA20"]) & (df["MA5"].shift(1) >= df["MA20"].shift(1)) & (df["MA5"].shift(2) >= df["MA20"].shift(2))
    condition_up = (df["MA5"] > df["MA20"]) & (df["MA5"].shift(1) <= df["MA20"].shift(1)) & (df["MA5"].shift(2) <= df["MA20"].shift(2))

    # 단기 예측값 설정
    condition2_down = (df["MA20"] < df["MA60"]) & (df["MA20"].shift(1) >= df["MA60"].shift(1)) & (df["MA20"].shift(2) >= df["MA60"].shift(2))
    condition2_up = (df["MA20"] > df["MA60"]) & (df["MA20"].shift(1) <= df["MA60"].shift(1)) & (df["MA20"].shift(2) <= df["MA60"].shift(2))

    # 전환 예측값 설정
    condition3_down = (df["Baseline"] > df["ConversionLine"]) & (df["Baseline"].shift(1) <= df["ConversionLine"].shift(1)) & (df["Baseline"].shift(2) <= df["ConversionLine"].shift(2))
    condition3_up = (df["Baseline"] < df["ConversionLine"]) & (df["Baseline"].shift(1) >= df["ConversionLine"].shift(1)) & (df["Baseline"].shift(2) >= df["ConversionLine"].shift(2))

    # 이격도 예측값 설정
    df["Disparity5"]  = (df["종가"] / df["MA5"])  * 100   # 단기 이격도
    df["Disparity20"] = (df["종가"] / df["MA20"]) * 100   # 중기 이격도

    DISPARITY_OVERBOUGHT  = 106   # 과열 기준
    DISPARITY_OVERSOLD    = 94    # 침체 기준
    DISPARITY_CENTER      = 100   # 중립 기준
    DISPARITY_LOWER_LIMIT = 75
    DISPARITY_UPPER_LIMIT = 130

    # 침체 구간에서 100선 회복 → 매수 신호
    disparity_up = (
        (df["Disparity20"] > DISPARITY_CENTER) &          # 오늘 100 돌파
        (df["Disparity20"].shift(1) <= DISPARITY_CENTER) & # 어제 100 이하
        (df["Disparity20"].shift(2) < DISPARITY_OVERSOLD)  # 이틀 전 침체 구간 확인
    )

    # 과열 구간에서 100선 하락 → 매도 신호
    disparity_down = (
        (df["Disparity20"] < DISPARITY_CENTER) &           # 오늘 100 하락
        (df["Disparity20"].shift(1) >= DISPARITY_CENTER) & # 어제 100 이상
        (df["Disparity20"].shift(2) > DISPARITY_OVERBOUGHT) # 이틀 전 과열 구간 확인
    )

    # 침체 구간
    oversold_zone = (
        (df["Disparity20"] >= DISPARITY_LOWER_LIMIT) &
        (df["Disparity20"] <= DISPARITY_OVERSOLD)
    )

    # 이격도 회복 확인
    disparity_recovering = (
        (df["Disparity20"] > df["Disparity20"].shift(1)) &
        (df["Disparity5"] > df["Disparity5"].shift(1))
    )

    # 가격 반등 확인
    price_recovering = (
        df["종가"] > df["종가"].shift(1)
    )

    # 다른 예측과 방향 일치
    other_up_signal = (
        condition_up  | condition_up.shift(1)  | condition_up.shift(2)  |
        condition2_up | condition2_up.shift(1) | condition2_up.shift(2) |
        condition3_up | condition3_up.shift(1) | condition3_up.shift(2)
    )

    # 강한상승
    disparity_extreme_up = (
        oversold_zone &
        disparity_recovering &
        price_recovering &
        other_up_signal
    )

    # 과열 구간
    overbought_zone = (
        (df["Disparity20"] >= DISPARITY_OVERBOUGHT) &
        (df["Disparity20"] < DISPARITY_UPPER_LIMIT)
    )

    # 이격도 하락 확인
    disparity_falling = (
        (df["Disparity20"] < df["Disparity20"].shift(1)) &
        (df["Disparity5"] < df["Disparity5"].shift(1))
    )

    # 가격 하락 확인
    price_falling = (
        df["종가"] < df["종가"].shift(1)
    )

    # 다른 예측과 방향 일치
    other_down_signal = (
        condition_down  | condition_down.shift(1)  | condition_down.shift(2)  |
        condition2_down | condition2_down.shift(1) | condition2_down.shift(2) |
        condition3_down | condition3_down.shift(1) | condition3_down.shift(2)
    )

    # 강한하락
    disparity_extreme_down = (
        overbought_zone &
        disparity_falling &
        price_falling &
        other_down_signal
    )

    # 단기/중기 이격도 동시 확인으로 신뢰도 향상
    disparity_confirm_up = (
        (df["Disparity5"]  < DISPARITY_CENTER) &  # 단기도 침체
        (df["Disparity20"] < DISPARITY_CENTER)    # 중기도 침체 → 강한 매수 신호
    )

    df.loc[condition_down, "초단기예측"] = "하락"
    df.loc[condition_up, "초단기예측"] = "상승"
    df.loc[condition2_down, "단기예측"] = "하락"
    df.loc[condition2_up, "단기예측"] = "상승"
    df.loc[condition3_down, "전환예측"] = "하락"
    df.loc[condition3_up, "전환예측"] = "상승"
    df.loc[disparity_up, "이격도예측"] = "상승"
    df.loc[disparity_down, "이격도예측"] = "하락"
    df.loc[disparity_extreme_up, "이격도예측"] = "강한상승"
    df.loc[disparity_extreme_down, "이격도예측"] = "강한하락"
    

    # 마지막 3개의 행에서 예측 데이터 필터링
    filtered_df = df.tail(3)
    filtered_df = filtered_df.dropna(subset=["초단기예측", "단기예측", "전환예측", "이격도예측"], how="all")

    output = []
    if not filtered_df.empty:
        for index, row in filtered_df.iterrows():
            output.append({
                "날짜": row["날짜"],
                "종목명": name,
                "종목코드": ticker,
                "초단기예측": row["초단기예측"],
                "단기예측": row["단기예측"],
                "전환예측": row["전환예측"],
                "이격도예측": row["이격도예측"],
                "이격도수치": round(row["Disparity20"], 2)
            })
    
    return output

# 병렬 처리
def fetch_and_process_data(start_time, formatted_time):
    ticker_list = get_ticker_list()
    all_output = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:  # 최대 8개의 프로세스 사용
        futures = {executor.submit(process_stock_data, ticker_info): ticker_info for ticker_info in ticker_list}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                all_output.extend(result)
            except Exception as e:
                ticker_info = futures[future]
                print(f"[오류] {ticker_info['name']}({ticker_info['ticker']}): {e}")
    
    return build_result_message(all_output, start_time, formatted_time)

# 결과 메시지 전송
def build_result_message(output, start_time, formatted_time):
    # 예측별 상승/하락 횟수 계산
    category_count = defaultdict(lambda: {'강한상승': 0, '상승': 0, '하락': 0, '강한하락': 0})
    date_count = defaultdict(lambda: {
        '강한상승': 0, '상승': 0, '하락': 0, '강한하락': 0,
        '종목': defaultdict(lambda: {
            '강한상승': 0, '상승': 0, '하락': 0, '강한하락': 0,
            'ticker': None, '이격도수치': None
        })
    })

    for record in output:
        date = record["날짜"]
        stock = record["종목명"]
        ticker = record["종목코드"]

        ALL_SIGNALS = {'강한상승', '상승', '하락', '강한하락'}

        for key in ['초단기예측', '단기예측', '전환예측', '이격도예측']:
            signal = record[key]
            if signal in ALL_SIGNALS:
                category_count[key][signal] += 1
                date_count[date][signal] += 1
                date_count[date]['종목'][stock][signal] += 1
                date_count[date]['종목'][stock]['ticker'] = ticker
                date_count[date]['종목'][stock]['이격도수치'] = record.get('이격도수치')

    # 결과 메시지 생성
    end_time = time.time()  # 실행 끝 시간 기록
    execution_time = end_time - start_time  # 실행 시간 계산

    result_message = f"분석 시간 : {formatted_time} (Execution time: {execution_time:.2f} seconds)\n\n"
    result_message += "예측별 신호 횟수:\n"
    for category, counts in category_count.items():
        result_message += (
            f"{category}: "
            f"강한상승: {counts['강한상승']}, 상승: {counts['상승']}, "
            f"하락: {counts['하락']}, 강한하락: {counts['강한하락']}\n"
        )

    # 종목별 출력
    result_message += "\n최근 3일 기준 종목별 신호\n"
    for date in sorted(date_count.keys(), reverse=True):
        counts = date_count[date]
        formatted_date = date.strftime("%Y-%m-%d")
        result_message += (
            f"날짜: {formatted_date} - "
            f"강한상승: {counts['강한상승']}, 상승: {counts['상승']}, "
            f"하락: {counts['하락']}, 강한하락: {counts['강한하락']}\n"
        )

        # 종목 정렬 기준: 강한상승 → 상승 → 하락 → 강한하락 순
        sorted_stocks = sorted(
            counts['종목'].items(),
            key=lambda x: (x[1]['강한상승'], x[1]['상승'], -x[1]['하락'], -x[1]['강한하락']),
            reverse=True
        )

        for stock, sc in sorted_stocks:
            disparity = f", 이격도: {sc['이격도수치']}" if sc['이격도수치'] else ""
            result_message += (
                f" - {stock}({sc['ticker']}) "
                f"강한상승: {sc['강한상승']}, 상승: {sc['상승']}, "
                f"하락: {sc['하락']}, 강한하락: {sc['강한하락']}"
                f"{disparity}\n"
            )
    
    return result_message