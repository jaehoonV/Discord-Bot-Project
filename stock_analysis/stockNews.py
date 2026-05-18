import requests
import xml.etree.ElementTree as ET
from datetime import datetime

GOOGLE_RSS_URL = "https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko&sort=date"


def get_stock_news(query: str, limit: int = 5) -> str:
    try:
        stock_query = f"{query} 주가 OR 증시 OR 실적 OR 주식"
        url = GOOGLE_RSS_URL.format(query=requests.utils.quote(stock_query, safe='+').replace('%20', '+'))
        print(url)
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)

        root = ET.fromstring(response.content)
        items = root.findall(".//item")

        if not items:
            return f"❌ **{query}** 관련 뉴스를 찾을 수 없습니다."

        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [f"📰 **{query} 관련 최신 뉴스** | {now}\n"]

        for i, item in enumerate(items[:limit], 1):
            title = item.findtext("title", "제목 없음").strip()
            link = item.findtext("link", "").strip()
            pub_date = item.findtext("pubDate", "").strip()

            # 날짜 포맷 변환 (RFC 822 → 보기 좋게)
            try:
                dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")
                pub_date = dt.strftime("%Y-%m-%d %H:%M")
            except:
                pass

            lines.append(f"`{i}.` {title}")
            lines.append(f"     🔗 <{link}>")
            lines.append(f"     🕐 {pub_date}\n")

        return "\n".join(lines)

    except Exception as e:
        return f"❌ 뉴스 조회 중 오류가 발생했습니다.\n```{type(e).__name__}: {e}```"