import requests
from bs4 import BeautifulSoup
from datetime import datetime

KOREAN_INDICATORS = {
    "GDP Growth Rate": "GDP 성장률",
    "GDP Annual Growth Rate": "연간 GDP 성장률",
    "Unemployment Rate": "실업률",
    "Non Farm Payrolls": "비농업 신규 고용자 수",
    "Inflation Rate": "소비자물가 상승률 (YoY)",
    "Inflation Rate MoM": "소비자물가 상승률 (MoM)",
    "Interest Rate": "연방 기준금리",
    "Balance of Trade": "무역수지",
    "Current Account": "경상수지",
    "Current Account to GDP": "경상수지 / GDP 비율",
    "Government Debt to GDP": "정부 부채 / GDP 비율",
    "Government Budget": "정부 재정 수지",
    "Business Confidence": "기업 경기 신뢰지수",
    "Manufacturing PMI": "제조업 구매관리자지수 (PMI)",
    "Consumer Confidence": "소비자 신뢰지수",
    "Retail Sales MoM": "소매판매 증감률 (전월대비)",
    "Building Permits": "주택 허가 건수"
}
INDICATOR_KOR_MAP = {k.strip(): v for k, v in KOREAN_INDICATORS.items()}

# 미국 경제지표 (TradingEconomics에서 미국 경제 관련 지표 일부 스크래핑)

def get_us_economic_indicators_text():
    url = "https://tradingeconomics.com/united-states/indicators"
    headers = {"User-Agent": "Mozilla/5.0"}

    result = []
    result.append(f"📅 미국 경제 지표")
    result.append(f"기준일: {datetime.now().strftime('%Y-%m-%d')}")
    result.append("")

    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()

        soup = BeautifulSoup(res.text, "html.parser")

        table = soup.find("table", {"class": "table"})
        if table is None:
            return "경제 지표 테이블을 찾지 못했습니다."

        rows = table.find_all("tr")[1:]

        found_count = 0

        for row in rows:
            cols = row.find_all("td")

            if len(cols) >= 5:
                indicator = cols[0].text.strip()
                actual = cols[1].text.strip()
                previous = cols[2].text.strip()

                if indicator in INDICATOR_KOR_MAP:
                    kor_name = INDICATOR_KOR_MAP[indicator]

                    try:
                        a_val = float(actual.replace(",", ""))
                        p_val = float(previous.replace(",", ""))

                        if a_val > p_val:
                            emoji = "🔺"
                        elif a_val < p_val:
                            emoji = "🔻"
                        else:
                            emoji = "➖"
                    except ValueError:
                        emoji = ""

                    result.append(
                        f"- {kor_name}: 현재 {actual}, 이전 {previous} {emoji}"
                    )

                    found_count += 1

        if found_count == 0:
            result.append("조회된 주요 경제지표가 없습니다.")

    except Exception as e:
        result.append(f"경제 지표 스크래핑 오류: {e}")

    return "\n".join(result)

def get_fear_and_greed_index_text():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://edition.cnn.com/markets/fear-and-greed",
        "Origin": "https://edition.cnn.com",
    }

    try:
        res = requests.get(url, headers=headers, timeout=15)
        data = res.json()

        score = round(float(data["fear_and_greed"]["score"]), 1)

        if score < 25:
            status = "Extreme Fear"
            kor_status = "극도의 공포"
        elif score < 45:
            status = "Fear"
            kor_status = "공포"
        elif score < 55:
            status = "Neutral"
            kor_status = "중립"
        elif score < 75:
            status = "Greed"
            kor_status = "탐욕"
        else:
            status = "Extreme Greed"
            kor_status = "극도의 탐욕"

        return (
            "\n\n🧠 CNN 공포와 탐욕 지수\n"
            f"- 점수: {score}점\n"
            f"- 상태: {kor_status} ({status})"
        )

    except Exception as e:
        return f"\n\n공포와 탐욕 지수 스크래핑 오류: {e}"

def get_market_indicator_report():
    result = []
    result.append(f"📅 데이터 기준일: {datetime.now().strftime('%Y-%m-%d')}")
    result.append("")
    result.append(get_us_economic_indicators_text())
    result.append(get_fear_and_greed_index_text())

    return "\n".join(result)