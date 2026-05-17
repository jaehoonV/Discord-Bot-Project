import requests
from bs4 import BeautifulSoup
from datetime import datetime

CURRENCIES = {
    "미국 USD": "🇺🇸 달러 (USD)",
    "일본 JPY (100엔)": "🇯🇵 엔화 (JPY/100)",
    "유럽연합 EUR": "🇪🇺 유로 (EUR)",
    "중국 CNY": "🇨🇳 위안 (CNY)",
}

NAVER_URL = "https://finance.naver.com/marketindex/exchangeList.naver"

def parse_float(text: str) -> float:
    return float(text.strip().replace(",", ""))

def get_exchange_rate_report() -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.naver.com/marketindex/",
        }

        response = requests.get(NAVER_URL, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = "euc-kr"

        soup = BeautifulSoup(response.text, "html.parser")

        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [f"💱 **원화(KRW) 기준 환율** | {now}\n"]

        rows = soup.select("table.tbl_exchange tbody tr")

        if not rows:
            return "❌ 환율 데이터 테이블을 찾지 못했습니다."

        found = set()

        for row in rows:
            tds = row.select("td")

            if len(tds) < 4:
                continue

            name = tds[0].get_text(" ", strip=True)
            current_text = tds[1].get_text(strip=True)

            if name not in CURRENCIES:
                continue

            label = CURRENCIES[name]
            current = parse_float(current_text)

            lines.append(
                f"{label}\n"
                f"  매매기준율: **{current:,.2f} 원**\n"
            )

            found.add(name)

        for name, label in CURRENCIES.items():
            if name not in found:
                lines.append(f"{label}\n  ❌ 데이터 없음\n")

        lines.append("📡 *네이버 금융*")
        return "\n".join(lines)

    except Exception as e:
        return f"❌ 환율 조회 중 오류가 발생했습니다.\n```{type(e).__name__}: {e}```"