
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup as bs
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import time
import csv

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
    disparity_up = (df["Disparity"] > 98) & (df["Disparity"].shift(1) <= 98)
    disparity_down = (df["Disparity"] < 102) & (df["Disparity"].shift(1) >= 102)

    df.loc[condition_down, "초단기예측"] = "하락"
    df.loc[condition_up, "초단기예측"] = "상승"
    df.loc[condition2_down, "단기예측"] = "하락"
    df.loc[condition2_up, "단기예측"] = "상승"
    df.loc[condition3_down, "전환예측"] = "하락"
    df.loc[condition3_up, "전환예측"] = "상승"
    df.loc[disparity_up, "이격도예측"] = "상승"
    df.loc[disparity_down, "이격도예측"] = "하락"

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
                "이격도예측": row["이격도예측"]
            })
    
    return output

# 병렬 처리
def fetch_and_process_data(start_time, formatted_time):
    ticker_list = get_ticker_list()
    all_output = []
    with ProcessPoolExecutor(max_workers=8) as executor:  # 최대 8개의 프로세스 사용
        futures = {executor.submit(process_stock_data, ticker_info): ticker_info for ticker_info in ticker_list}
        
        for future in as_completed(futures):
            result = future.result()
            all_output.extend(result)
    
    return result_message(all_output, start_time, formatted_time)

# 결과 메시지 전송
def result_message(output, start_time, formatted_time):
    # 예측별 상승/하락 횟수 계산
    category_count = defaultdict(lambda: {'상승': 0, '하락': 0})
    date_count = defaultdict(lambda: {'상승': 0, '하락': 0, '종목': defaultdict(lambda: {'상승': 0, '하락': 0, 'ticker': None})})

    for record in output:
        date = record["날짜"]
        stock = record["종목명"]
        ticker = record["종목코드"]
        for key in ['초단기예측', '단기예측', '전환예측', '이격도예측']:
            if record[key] == '상승':
                category_count[key]['상승'] += 1
                date_count[date]['상승'] += 1
                date_count[date]['종목'][stock]['상승'] += 1
                date_count[date]['종목'][stock]['ticker'] = ticker
            elif record[key] == '하락':
                category_count[key]['하락'] += 1
                date_count[date]['하락'] += 1
                date_count[date]['종목'][stock]['하락'] += 1
                date_count[date]['종목'][stock]['ticker'] = ticker

    # 결과 메시지 생성
    end_time = time.time()  # 실행 끝 시간 기록
    execution_time = end_time - start_time  # 실행 시간 계산

    result_message = f"분석 시간 : {formatted_time} (Execution time: {execution_time:.2f} seconds)\n\n"
    result_message += "예측별 상승, 하락 횟수:\n"
    for category, counts in category_count.items():
        result_message += f"{category}: 상승: {counts['상승']}, 하락: {counts['하락']}\n"

    result_message += "\n최근 3일 기준 종목별 상승, 하락 신호\n"
    for date in sorted(date_count.keys(), reverse=True):
        counts = date_count[date]
        formatted_date = date.strftime("%Y-%m-%d")
        result_message += f"날짜: {formatted_date} - 상승: {counts['상승']}, 하락: {counts['하락']}\n"
        for stock, stock_counts in sorted(counts['종목'].items(), key=lambda x: x[1]['상승'], reverse=True):
            result_message += f" - 종목: {stock} - 상승: {stock_counts['상승']}, 하락: {stock_counts['하락']}\n"
    
    return result_message