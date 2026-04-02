"""Normalized news archive helpers and primary/secondary source adapters."""

from __future__ import annotations

import json
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests
import yfinance as yf
from bs4 import BeautifulSoup

_ARCHIVE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

_GOODINFO_BASE = "https://goodinfo.tw/tw/"
_GOODINFO_NEWS_URL = "https://goodinfo.tw/tw/StockAnnounceList.asp"
_CNYES_CATEGORY_URLS = (
    "https://news.cnyes.com/news/cat/tw_stock",
    "https://news.cnyes.com/news/cat/headline",
)


def normalize_news_article(
    *,
    source: str,
    source_article_id: str = "",
    published_at: str = "",
    headline: str = "",
    url: str = "",
    category: str = "",
    snippet: str = "",
    content: str = "",
    raw_payload: Any = None,
    retrieval_method: str = "",
    language: str = "zh-TW",
    is_primary_source: bool = False,
) -> dict[str, Any]:
    """Build a canonical news article record."""
    normalized_url = url.strip()
    normalized_source = source.strip()
    dedupe_key = build_news_dedupe_key(
        canonical_url=normalized_url,
        source=normalized_source,
        source_article_id=source_article_id,
        published_at=published_at,
        headline=headline,
    )
    return {
        "source": normalized_source,
        "source_article_id": str(source_article_id or "").strip(),
        "published_at": published_at.strip(),
        "headline": headline.strip(),
        "url": normalized_url,
        "category": category.strip(),
        "snippet": snippet.strip(),
        "content": content.strip(),
        "language": language,
        "retrieval_method": retrieval_method.strip(),
        "is_primary_source": bool(is_primary_source),
        "raw_payload": raw_payload if raw_payload is not None else {},
        "dedupe_key": dedupe_key,
    }


def build_news_dedupe_key(
    *,
    canonical_url: str,
    source: str,
    source_article_id: str,
    published_at: str,
    headline: str,
) -> str:
    """Build the normalized dedupe key for merged news records."""
    if canonical_url:
        return canonical_url.strip()
    if source and source_article_id:
        return f"{source.strip()}::{source_article_id.strip()}"
    normalized_title = normalize_headline(headline)
    return f"{published_at.strip()}::{normalized_title}"


def normalize_headline(headline: str) -> str:
    """Normalize a headline for stable dedupe fallback."""
    lowered = headline.strip().lower()
    lowered = re.sub(r"\s+", "", lowered)
    lowered = re.sub(r"[^\w\u4e00-\u9fff]", "", lowered)
    return lowered


def dedupe_news_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate normalized article records by canonical dedupe key."""
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for article in articles:
        dedupe_key = str(article.get("dedupe_key", "")).strip()
        if not dedupe_key:
            dedupe_key = build_news_dedupe_key(
                canonical_url=article.get("url", ""),
                source=article.get("source", ""),
                source_article_id=article.get("source_article_id", ""),
                published_at=article.get("published_at", ""),
                headline=article.get("headline", ""),
            )
            article["dedupe_key"] = dedupe_key
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(article)
    return deduped


def fetch_news_archive(
    *,
    stock_code: str,
    stock_name: str,
    event_type: str,
    date_from: str,
    date_to: str,
    queries: list[str],
    max_results: int,
    primary_source: str = "cnyes",
    source_policy: str = "archive_first",
    allow_secondary_sources: bool = True,
) -> dict[str, Any]:
    """Fetch normalized archive records from primary and secondary sources."""
    primary_records: list[dict[str, Any]] = []
    secondary_records: list[dict[str, Any]] = []
    data_gaps: list[str] = []

    if primary_source == "cnyes":
        try:
            primary_records = fetch_cnyes_primary_records(
                stock_code=stock_code,
                stock_name=stock_name,
                date_from=date_from,
                date_to=date_to,
                max_results=max_results,
            )
        except Exception as exc:
            data_gaps.append(f"cnyes_primary_unavailable:{type(exc).__name__}")
            primary_records = []

    if source_policy == "archive_first" and allow_secondary_sources:
        need_secondary = not primary_records or len(primary_records) < max_results
        if need_secondary:
            secondary_records.extend(
                fetch_goodinfo_discovery_records(
                    stock_code=stock_code,
                    date_from=date_from,
                    date_to=date_to,
                    max_results=max_results,
                )
            )
            secondary_records.extend(
                fetch_google_news_rss_records(
                    queries=queries,
                    date_from=date_from,
                    date_to=date_to,
                    max_results=max_results,
                )
            )
            secondary_records.extend(
                fetch_yfinance_news_records(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    date_from=date_from,
                    date_to=date_to,
                    max_results=max_results,
                )
            )

    primary_records = dedupe_news_articles(primary_records)
    secondary_records = dedupe_news_articles(secondary_records)
    merged_records = dedupe_news_articles(primary_records + secondary_records)
    if len(merged_records) > max_results:
        merged_records = merged_records[:max_results]

    return {
        "records": merged_records,
        "primary_records": primary_records,
        "secondary_records": [row for row in secondary_records if row.get("dedupe_key") not in {item.get("dedupe_key") for item in primary_records}],
        "data_gaps": data_gaps,
        "source_breakdown": {
            "primary_count": len(primary_records),
            "secondary_count": len(secondary_records),
            "merged_count": len(merged_records),
        },
    }


def fetch_cnyes_primary_records(
    *,
    stock_code: str,
    stock_name: str,
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """Fetch cnyes article index records from category/list pages."""
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for url in _CNYES_CATEGORY_URLS:
        html = requests.get(url, headers=_ARCHIVE_HEADERS, timeout=10).text
        page_records = _extract_cnyes_records_from_category_html(
            html=html,
            category=url.rsplit("/", 1)[-1],
            stock_code=stock_code,
            stock_name=stock_name,
            date_from=date_from,
            date_to=date_to,
            max_results=max_results,
        )
        for record in page_records:
            article_id = record.get("source_article_id", "")
            if article_id and article_id in seen_ids:
                continue
            if article_id:
                seen_ids.add(article_id)
            records.append(record)
            if len(records) >= max_results:
                return dedupe_news_articles(records)

    return dedupe_news_articles(records)


def _extract_cnyes_records_from_category_html(
    *,
    html: str,
    category: str,
    stock_code: str,
    stock_name: str,
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """Parse cnyes category HTML into normalized article records."""
    matches = re.finditer(r'\{\\\"newsId\\\":', html)
    records: list[dict[str, Any]] = []

    for match in matches:
        start = match.start()
        decoded = html[start : start + 4000].replace('\\"', '"').replace("\\/", "/")
        try:
            payload, _ = json.JSONDecoder().raw_decode(decoded)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        news_id = str(payload.get("newsId") or "").strip()
        title = str(payload.get("title") or "").strip()
        href = str(payload.get("href") or "").strip()
        if not news_id or not title or not href:
            continue
        article_url = urljoin("https://news.cnyes.com", href)
        detail = fetch_cnyes_article_detail(article_url=article_url, news_id=news_id)
        published_at = detail.get("published_at", "")
        if date_from and published_at and published_at < date_from:
            continue
        if date_to and published_at and published_at > date_to:
            continue
        text_blob = f"{title} {detail.get('snippet', '')} {detail.get('content', '')}"
        if stock_code and stock_code not in text_blob and stock_name and stock_name not in text_blob:
            continue
        records.append(
            normalize_news_article(
                source="cnyes",
                source_article_id=news_id,
                published_at=published_at,
                headline=title,
                url=article_url,
                category=detail.get("category") or category,
                snippet=detail.get("snippet", ""),
                content=detail.get("content", ""),
                raw_payload=payload,
                retrieval_method="cnyes_category",
                language="zh-TW",
                is_primary_source=True,
            )
        )
        if len(records) >= max_results:
            break

    return records


def fetch_cnyes_article_detail(*, article_url: str, news_id: str = "") -> dict[str, str]:
    """Fetch publish time, description and content from a cnyes article page."""
    response = requests.get(article_url, headers=_ARCHIVE_HEADERS, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    title = _meta_content(soup, "og:title")
    description = _meta_content(soup, "og:description")
    published_raw = _meta_content(soup, "article:published_time")
    category = _meta_name_content(soup, "category")
    published_at = _normalize_possible_date(published_raw)

    content_json_match = re.search(r'"contentJson":(\[.*?\])\s*,\s*"market"', response.text)
    content = ""
    if content_json_match:
        try:
            content_payload = json.loads(content_json_match.group(1))
            content = " ".join(
                str(item.get("content", "")).strip()
                for item in content_payload
                if isinstance(item, dict) and item.get("content")
            )
        except json.JSONDecodeError:
            content = ""
    if not content:
        content = _extract_article_body_text(soup)

    return {
        "headline": title,
        "snippet": description,
        "published_at": published_at,
        "category": category,
        "content": content.strip(),
        "article_url": article_url,
        "source_article_id": news_id,
    }


def fetch_goodinfo_discovery_records(
    *,
    stock_code: str,
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """Fetch Goodinfo stock/date filtered news index records."""
    if not stock_code or not date_from or not date_to:
        return []

    session = requests.Session()
    session.cookies.set("CLIENT_KEY", "codex", domain="goodinfo.tw", path="/")
    response = session.get(
        _GOODINFO_NEWS_URL,
        params={
            "STOCK_ID": stock_code,
            "START_DT": _format_goodinfo_date(date_from),
            "END_DT": _format_goodinfo_date(date_to),
        },
        headers=_ARCHIVE_HEADERS,
        timeout=10,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    records: list[dict[str, Any]] = []
    for row in soup.select("tr"):
        text = row.get_text(" ", strip=True)
        if not text or "新聞" not in text:
            continue
        links = row.select("a[href]")
        if not links:
            continue
        external_link = next((link for link in links if "StockAnnounceList.asp" not in link.get("href", "")), None)
        if external_link is None:
            continue
        headline = external_link.get_text(" ", strip=True)
        article_url = urljoin(_GOODINFO_BASE, external_link.get("href", "").strip())
        if not headline or not article_url:
            continue
        published_at = _extract_date_from_text(text)
        source_name = _infer_source_name(article_url)
        records.append(
            normalize_news_article(
                source=source_name or "goodinfo_discovery",
                published_at=published_at,
                headline=headline,
                url=article_url,
                category="goodinfo_discovery",
                snippet=text[:300],
                raw_payload={"row_text": text},
                retrieval_method="goodinfo_stock_date_index",
                language="zh-TW",
                is_primary_source=False,
            )
        )
        if len(records) >= max_results:
            break
    return records


def fetch_google_news_rss_records(
    *,
    queries: list[str],
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """Fetch normalized records from Google News RSS discovery."""
    records: list[dict[str, Any]] = []
    for query in queries:
        feed_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        response = requests.get(feed_url, headers=_ARCHIVE_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")
        for item in soup.find_all("item"):
            title_raw = item.title.get_text(strip=True) if item.title else ""
            article_url = item.link.get_text(strip=True) if item.link else ""
            if not title_raw or not article_url:
                continue
            published_at = _parse_rfc822_date(item.pubDate.get_text(strip=True) if item.pubDate else "")
            if date_from and published_at and published_at < date_from:
                continue
            if date_to and published_at and published_at > date_to:
                continue
            headline, source_name = _split_google_news_title(title_raw)
            description_raw = item.description.get_text(" ", strip=True) if item.description else ""
            snippet = BeautifulSoup(description_raw, "lxml").get_text(" ", strip=True)
            records.append(
                normalize_news_article(
                    source=source_name or _infer_source_name(article_url),
                    published_at=published_at,
                    headline=headline,
                    url=article_url,
                    category="google_news_rss",
                    snippet=snippet[:300],
                    raw_payload={"query": query},
                    retrieval_method="google_news_rss",
                    language="zh-TW",
                    is_primary_source=False,
                )
            )
            if len(records) >= max_results:
                return dedupe_news_articles(records)
    return dedupe_news_articles(records)


def fetch_yfinance_news_records(
    *,
    stock_code: str,
    stock_name: str,
    date_from: str,
    date_to: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """Fetch recent news records from yfinance as a secondary source."""
    if not stock_code:
        return []
    ticker = yf.Ticker(f"{stock_code}.TW")
    try:
        raw_news = ticker.get_news(count=max_results)
    except Exception:
        return []

    records: list[dict[str, Any]] = []
    for item in raw_news or []:
        content = item.get("content", {}) if isinstance(item, dict) else {}
        headline = str(content.get("title") or "").strip()
        article_url = (
            content.get("clickThroughUrl", {}) or {}
        ).get("url") or (
            content.get("canonicalUrl", {}) or {}
        ).get("url") or ""
        if not headline or not article_url:
            continue
        published_at = _normalize_possible_date(str(content.get("pubDate") or ""))
        if date_from and published_at and published_at < date_from:
            continue
        if date_to and published_at and published_at > date_to:
            continue
        source_name = ((content.get("provider") or {}).get("displayName") or "").strip()
        text_blob = f"{headline} {content.get('summary','')} {stock_name} {stock_code}"
        if stock_name and stock_name not in text_blob and stock_code not in text_blob:
            continue
        records.append(
            normalize_news_article(
                source=source_name or "yfinance",
                source_article_id=str(content.get("id") or item.get("id") or "").strip(),
                published_at=published_at,
                headline=headline,
                url=article_url,
                category="yfinance_news",
                snippet=str(content.get("summary") or "").strip()[:300],
                raw_payload=item,
                retrieval_method="yfinance_get_news",
                language="en-US",
                is_primary_source=False,
            )
        )
        if len(records) >= max_results:
            break
    return dedupe_news_articles(records)


def _meta_content(soup: BeautifulSoup, prop: str) -> str:
    node = soup.find("meta", attrs={"property": prop})
    return node.get("content", "").strip() if node else ""


def _meta_name_content(soup: BeautifulSoup, name: str) -> str:
    node = soup.find("meta", attrs={"name": name})
    return node.get("content", "").strip() if node else ""


def _extract_article_body_text(soup: BeautifulSoup) -> str:
    for selector in ("article", ".article-body", ".news-content", "main"):
        node = soup.select_one(selector)
        if node:
            return node.get_text(" ", strip=True)
    return " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p")[:10])


def _split_google_news_title(title: str) -> tuple[str, str]:
    if " - " not in title:
        return title, ""
    headline, source = title.rsplit(" - ", 1)
    return headline.strip(), source.strip()


def _parse_rfc822_date(raw: str) -> str:
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).strftime("%Y-%m-%d")
    except Exception:
        return raw[:10]


def _format_goodinfo_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{dt.year}/{dt.month}/{dt.day}"


def _normalize_possible_date(raw: str) -> str:
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
