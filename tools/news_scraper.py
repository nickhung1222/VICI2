"""Taiwan financial news scraper.

Primary path now prefers normalized archive ingestion and discovery sources,
while preserving legacy adapter helpers for fallback and compatibility.
"""

from __future__ import annotations

import time
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Union
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from tools.news_archive import fetch_news_archive

# Rate limiting
_DELAY_SECONDS = 1.5

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.cnyes.com/",
}
_WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def search_news(
    query: str,
    date_from: str = None,
    date_to: str = None,
    max_results: int = 20,
    *,
    stock_code: str = "",
    stock_name: str = "",
    event_type: str = "",
    queries: list[str] | None = None,
    source_policy: str = "archive_first",
    primary_source: str = "cnyes",
    allow_secondary_sources: bool = True,
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
    archive_queries = queries or [query]
    if source_policy == "archive_first" and stock_code:
        archive_payload = fetch_news_archive(
            stock_code=stock_code,
            stock_name=stock_name,
            event_type=event_type,
            date_from=date_from or "",
            date_to=date_to or "",
            queries=archive_queries,
            max_results=max_results,
            primary_source=primary_source,
            source_policy=source_policy,
            allow_secondary_sources=allow_secondary_sources,
        )
        normalized_records = archive_payload.get("records", [])
        if normalized_records:
            return [_legacy_article_shape(record) for record in normalized_records[:max_results]]

    query_plans = _build_query_variants(query, date_from=date_from, date_to=date_to)
    results: list[dict] = []
    prioritize_web = _should_prioritize_web_search(query=query, date_to=date_to)
    search_order = (
        (_search_web, _search_cnyes, _search_moneydj)
        if prioritize_web
        else (_search_cnyes, _search_moneydj, _search_web)
    )

    for index, search_fn in enumerate(search_order):
        if len(results) >= max_results:
            break
        results.extend(
            _search_with_variants(
                search_fn=search_fn,
                query_plans=query_plans,
                date_from=date_from,
                date_to=date_to,
                max_results=max_results - len(results),
            )
        )
        results = _dedupe_articles(results)
        if prioritize_web and index == 0 and results:
            break

    return results[:max_results]


def _legacy_article_shape(record: dict[str, Any]) -> dict[str, Any]:
    """Convert normalized archive records back into the collector's current article shape."""
    return {
        "title": record.get("headline", ""),
        "date": record.get("published_at", ""),
        "source": record.get("source", ""),
        "url": record.get("url", ""),
        "snippet": record.get("snippet", ""),
        "content": record.get("content", ""),
        "news_id": record.get("source_article_id", "") if record.get("source") == "cnyes" else "",
        "source_article_id": record.get("source_article_id", ""),
        "retrieval_method": record.get("retrieval_method", ""),
        "is_primary_source": record.get("is_primary_source", False),
        "dedupe_key": record.get("dedupe_key", ""),
    }


def _should_prioritize_web_search(query: str, date_to: str = None) -> bool:
    """Prefer general web search for older/historical event lookups."""
    normalized = " ".join(query.split())
    if re.search(r"\b20\d{2}Q[1-4]\b", normalized, flags=re.IGNORECASE):
        return True
    if re.search(r"\bQ[1-4]\b", normalized, flags=re.IGNORECASE):
        return True
    if date_to:
        try:
            target = datetime.strptime(date_to, "%Y-%m-%d")
            if datetime.now() - target > timedelta(days=90):
                return True
        except ValueError:
            return False
    return False


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

        items_container = data.get("data") or data.get("items") or []
        if isinstance(items_container, dict):
            items = (
                items_container.get("items")
                or items_container.get("data")
                or []
            )
        elif isinstance(items_container, list):
            items = items_container
        else:
            items = []

        if not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
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


def _search_web(query: str, date_from: str, date_to: str, max_results: int) -> list[dict]:
    """Search the web via Google News RSS, with DuckDuckGo HTML as a last fallback."""
    rss_results = _search_google_news_rss(query, date_from=date_from, date_to=date_to, max_results=max_results)
    if rss_results:
        return rss_results
    return _search_duckduckgo_html(query, date_from=date_from, date_to=date_to, max_results=max_results)


def _search_google_news_rss(query: str, date_from: str, date_to: str, max_results: int) -> list[dict]:
    """Search Google News RSS for stable web-search fallback results."""
    results: list[dict] = []
    feed_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    response = requests.get(feed_url, headers=_WEB_HEADERS, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "xml")

    for item in soup.find_all("item"):
        title_raw = item.title.get_text(strip=True) if item.title else ""
        article_url = item.link.get_text(strip=True) if item.link else ""
        description_raw = item.description.get_text(" ", strip=True) if item.description else ""
        pub_date_raw = item.pubDate.get_text(strip=True) if item.pubDate else ""
        article_date = _parse_rfc822_date(pub_date_raw)

        if date_from and article_date and article_date < date_from:
            continue
        if date_to and article_date and article_date > date_to:
            continue

        title, source_name = _split_google_news_title(title_raw)
        snippet = BeautifulSoup(description_raw, "lxml").get_text(" ", strip=True)
        results.append(
            {
                "title": title,
                "date": article_date,
                "source": source_name or _infer_source_name(article_url),
                "url": article_url,
                "snippet": snippet[:300],
                "news_id": "",
            }
        )
        if len(results) >= max_results:
            break

    return results


def _search_duckduckgo_html(query: str, date_from: str, date_to: str, max_results: int) -> list[dict]:
    """Search DuckDuckGo HTML as a last-resort generic fallback."""
    results: list[dict] = []

    response = requests.post(
        "https://html.duckduckgo.com/html/",
        data={"q": query},
        headers=_WEB_HEADERS,
        timeout=10,
    )
    if response.status_code == 403:
        response = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers=_WEB_HEADERS,
            timeout=10,
        )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    for index, result in enumerate(soup.select(".result")):
        link = result.select_one(".result__a")
        if link is None:
            continue

        article_url = _normalize_search_result_url(link.get("href", ""))
        title = link.get_text(" ", strip=True)
        snippet_node = result.select_one(".result__snippet")
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        article_date = _extract_date_from_url(article_url)
        if not article_date:
            article_date = _extract_date_from_text(f"{title} {snippet}")
        if not article_date and index < 2:
            article_date = _infer_article_date(article_url)

        if date_from and article_date and article_date < date_from:
            continue
        if date_to and article_date and article_date > date_to:
            continue

        results.append(
            {
                "title": title,
                "date": article_date,
                "source": _infer_source_name(article_url),
                "url": article_url,
                "snippet": snippet[:300],
                "news_id": "",
            }
        )
        if len(results) >= max_results:
            break

    return results


def _split_google_news_title(title: str) -> tuple[str, str]:
    """Split Google News RSS titles into headline and source when possible."""
    if " - " not in title:
        return title, ""
    headline, source = title.rsplit(" - ", 1)
    return headline.strip(), source.strip()


def _parse_rfc822_date(raw: str) -> str:
    """Parse RFC 822 dates into YYYY-MM-DD."""
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).strftime("%Y-%m-%d")
    except Exception:
        return raw[:10]


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


def _parse_cnyes_date(raw: Union[str, int]) -> str:
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

        soup = BeautifulSoup(resp.content, "xml")

        for item in soup.find_all("item"):
            title = (item.find("title").get_text(strip=True) if item.find("title") else "")
            link = (item.find("link").get_text(strip=True) if item.find("link") else "")
            pub_date_raw = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""
            description = item.find("description").get_text(strip=True) if item.find("description") else ""

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


def _search_with_variants(
    *,
    search_fn,
    query_plans: list[dict[str, list[str] | str]],
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict]:
    """Search a source with progressively broader query variants."""
    results: list[dict] = []

    for plan in query_plans:
        if len(results) >= max_results:
            break
        try:
            fetched = search_fn(
                query=str(plan["query"]),
                date_from=date_from,
                date_to=date_to,
                max_results=max_results,
            )
        except Exception as exc:
            print(f"  ⚠ source search failed for {plan['query']}: {exc}")
            continue

        filtered = _filter_articles_by_terms(fetched, required_terms=list(plan["required_terms"]))
        results.extend(filtered)
        results = _dedupe_articles(results)

    return results[:max_results]


def _build_query_variants(query: str, date_from: str = None, date_to: str = None) -> list[dict[str, list[str] | str]]:
    """Build increasingly broad query variants plus local filter terms."""
    normalized = " ".join(query.split())
    tokens = [token for token in normalized.split(" ") if token]
    plans: list[dict[str, list[str] | str]] = []

    def add_plan(candidate: str, required_terms: list[str]) -> None:
        candidate = candidate.strip()
        if not candidate:
            return
        if any(existing["query"] == candidate for existing in plans):
            return
        plans.append({"query": candidate, "required_terms": required_terms})

    add_plan(normalized, tokens)
    if len(tokens) > 1:
        add_plan("".join(tokens), tokens)
        add_plan(tokens[0], tokens[1:])
    if date_from and date_to:
        year = date_from[:4]
        month = date_from[5:7]
        add_plan(f"{normalized} {year}", tokens)
        add_plan(f"{normalized} {year}{month}", tokens)

    return plans


def _filter_articles_by_terms(articles: list[dict], required_terms: list[str]) -> list[dict]:
    """Keep only articles that contain the key query terms in title/snippet."""
    if not required_terms:
        return articles

    filtered: list[dict] = []
    for article in articles:
        text = f"{article.get('title', '')} {article.get('snippet', '')}".lower()
        matched_terms = [term for term in required_terms if term.lower() in text]
        if matched_terms:
            filtered.append(article)
    return filtered


def _dedupe_articles(articles: list[dict]) -> list[dict]:
    """Deduplicate articles while preserving order."""
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict] = []
    for article in articles:
        key = (
            str(article.get("url", "")),
            str(article.get("title", "")),
            str(article.get("date", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(article)
    return deduped


def _normalize_search_result_url(url: str) -> str:
    """Decode DuckDuckGo redirect URLs when necessary."""
    parsed = urlparse(url)
    if "duckduckgo.com" not in parsed.netloc:
        return url
    params = parse_qs(parsed.query)
    target = params.get("uddg", [])
    if target:
        return unquote(target[0])
    return url


def _extract_date_from_url(url: str) -> str:
    """Infer a YYYY-MM-DD date from common news URL patterns."""
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    for idx in range(len(parts) - 2):
        if len(parts[idx]) == 4 and parts[idx].isdigit():
            year, month, day = parts[idx : idx + 3]
            if month.isdigit() and day.isdigit():
                try:
                    return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return ""


def _infer_article_date(url: str) -> str:
    """Fetch an article page and infer the publish date from common metadata."""
    try:
        response = requests.get(url, headers=_WEB_HEADERS, timeout=5)
        response.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(response.text, "lxml")
    selectors = [
        ("meta", {"property": "article:published_time"}, "content"),
        ("meta", {"name": "pubdate"}, "content"),
        ("meta", {"name": "publish-date"}, "content"),
        ("meta", {"itemprop": "datePublished"}, "content"),
        ("time", {}, "datetime"),
    ]
    for tag_name, attrs, attr_name in selectors:
        node = soup.find(tag_name, attrs=attrs) if attrs else soup.find(tag_name)
        if node is None:
            continue
        raw = node.get(attr_name, "") if attr_name else node.get_text(" ", strip=True)
        parsed = _normalize_possible_date(raw)
        if parsed:
            return parsed
    return ""


def _normalize_possible_date(raw: str) -> str:
    """Normalize common datetime strings into YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    for candidate in (raw.replace("Z", "+00:00"), raw):
        try:
            return datetime.fromisoformat(candidate).strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return ""


def _extract_date_from_text(text: str) -> str:
    """Extract a calendar date from title/snippet text."""
    if not text:
        return ""

    patterns = [
        r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})",
        r"(\d{4})年(\d{1,2})月(\d{1,2})日",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _infer_source_name(url: str) -> str:
    """Map a URL to a readable source label."""
    host = urlparse(url).netloc.lower()
    if "cnyes.com" in host:
        return "cnyes"
    if "moneydj.com" in host:
        return "moneydj"
    if "ctee.com.tw" in host:
        return "ctee"
    if "udn.com" in host:
        return "udn"
    if "yahoo.com" in host:
        return "yahoo"
    if "stockfeel.com.tw" in host:
        return "stockfeel"
    return host.replace("www.", "")


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
