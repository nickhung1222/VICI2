"""Taiwan financial news scraper.

Primary: cnyes.com (鉅亨網) JSON API — returns structured data without HTML parsing.
Fallback: MoneyDJ RSS feed.
"""

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# Rate limiting
_DELAY_SECONDS = 1.5

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.cnyes.com/",
}


def search_news(
    query: str,
    date_from: str = None,
    date_to: str = None,
    max_results: int = 20,
) -> list[dict]:
    """Search Taiwan financial news articles.

    Args:
        query: Search keywords in Chinese or English, e.g. '台積電法說會'
        date_from: Start date YYYY-MM-DD (optional)
        date_to: End date YYYY-MM-DD (optional)
        max_results: Maximum articles to return (default 20)

    Returns:
        List of article dicts with keys: title, date, source, url, snippet, news_id
    """
    results = _search_cnyes(query, date_from, date_to, max_results)

    if len(results) < max_results // 2:
        moneydj_results = _search_moneydj(query, date_from, date_to, max_results - len(results))
        results.extend(moneydj_results)

    return results[:max_results]


def fetch_article_content(url: str, news_id: str = None) -> str:
    """Fetch full text content of a news article.

    Args:
        url: Article URL
        news_id: cnyes news ID (if available, uses API for cleaner text)

    Returns:
        Full article text
    """
    if news_id:
        content = _fetch_cnyes_article(news_id)
        if content:
            return content

    return _scrape_article_html(url)


# ---------------------------------------------------------------------------
# cnyes.com (鉅亨網)
# ---------------------------------------------------------------------------

def _search_cnyes(query: str, date_from: str, date_to: str, max_results: int) -> list[dict]:
    """Search cnyes.com via their JSON API."""
    results = []
    page = 1
    per_page = min(max_results, 30)

    while len(results) < max_results:
        try:
            url = "https://api.cnyes.com/media/api/v1/search"
            params = {
                "q": query,
                "page": page,
                "limit": per_page,
                "type": "news",
            }
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠ cnyes search failed (page {page}): {e}")
            break

        items = (
            data.get("data", {}).get("items", [])
            or data.get("items", [])
            or []
        )

        if not items:
            break

        for item in items:
            pub_date = _parse_cnyes_date(item.get("publishAt") or item.get("publish_at") or "")

            if date_from and pub_date and pub_date < date_from:
                continue
            if date_to and pub_date and pub_date > date_to:
                continue

            news_id = str(item.get("newsId") or item.get("news_id") or "")
            title = item.get("title", "")
            snippet = item.get("summary") or item.get("content", "")[:200]

            article_url = (
                item.get("url")
                or (f"https://news.cnyes.com/news/id/{news_id}" if news_id else "")
            )

            results.append({
                "title": title,
                "date": pub_date or "",
                "source": "cnyes",
                "url": article_url,
                "snippet": snippet[:300] if snippet else "",
                "news_id": news_id,
            })

            if len(results) >= max_results:
                break

        page += 1
        time.sleep(_DELAY_SECONDS)

        # Stop if we got fewer results than requested (last page)
        if len(items) < per_page:
            break

    return results


def _fetch_cnyes_article(news_id: str) -> str:
    """Fetch full article content from cnyes API."""
    try:
        url = f"https://api.cnyes.com/media/api/v1/newsdetail/{news_id}"
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        content = (
            data.get("data", {}).get("content")
            or data.get("content")
            or ""
        )
        # Strip HTML tags if present
        if "<" in content:
            soup = BeautifulSoup(content, "lxml")
            content = soup.get_text(separator="\n", strip=True)
        return content
    except Exception as e:
        print(f"  ⚠ cnyes article fetch failed for {news_id}: {e}")
        return ""


def _parse_cnyes_date(raw: str | int) -> str:
    """Parse cnyes date (Unix timestamp or ISO string) to YYYY-MM-DD."""
    if not raw:
        return ""
    try:
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw).strftime("%Y-%m-%d")
        raw_str = str(raw)
        if raw_str.isdigit():
            return datetime.fromtimestamp(int(raw_str)).strftime("%Y-%m-%d")
        return raw_str[:10]
    except Exception:
        return str(raw)[:10]


# ---------------------------------------------------------------------------
# MoneyDJ RSS fallback
# ---------------------------------------------------------------------------

def _search_moneydj(query: str, date_from: str, date_to: str, max_results: int) -> list[dict]:
    """Search MoneyDJ via RSS feed (keyword-based)."""
    results = []
    try:
        # MoneyDJ news search RSS
        encoded_query = quote(query)
        rss_url = f"https://www.moneydj.com/KMDJ/News/NewsRSS.aspx?sSubType=mb15&sSearch={encoded_query}"
        resp = requests.get(rss_url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date_raw = item.findtext("pubDate") or ""
            description = (item.findtext("description") or "").strip()

            pub_date = _parse_moneydj_date(pub_date_raw)

            if date_from and pub_date and pub_date < date_from:
                continue
            if date_to and pub_date and pub_date > date_to:
                continue

            results.append({
                "title": title,
                "date": pub_date,
                "source": "moneydj",
                "url": link,
                "snippet": description[:300],
                "news_id": "",
            })

            if len(results) >= max_results:
                break

    except Exception as e:
        print(f"  ⚠ MoneyDJ RSS search failed: {e}")

    return results


def _scrape_article_html(url: str) -> str:
    """Generic HTML scraper for article content."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "lxml")

        # Remove nav, header, footer, ads
        for tag in soup.select("nav, header, footer, .ad, .advertisement, script, style"):
            tag.decompose()

        # Try common article body selectors
        for selector in [
            "article",
            ".article-body",
            ".news-content",
            ".content-body",
            "#article-body",
            ".articleContent",
            "main",
        ]:
            element = soup.select_one(selector)
            if element:
                return element.get_text(separator="\n", strip=True)

        # Fallback: get all paragraph text
        paragraphs = soup.find_all("p")
        return "\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)

    except Exception as e:
        return f"Error fetching article: {e}"


def _parse_moneydj_date(raw: str) -> str:
    """Parse RFC 822 date string to YYYY-MM-DD."""
    if not raw:
        return ""
    try:
        from email.utils import parsedate
        parsed = parsedate(raw)
        if parsed:
            return datetime(*parsed[:3]).strftime("%Y-%m-%d")
    except Exception:
        pass
    return raw[:10] if raw else ""
