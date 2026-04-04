"""Normalized news archive helpers and primary/secondary source adapters."""

from __future__ import annotations

import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urljoin, urlparse

import requests
import yfinance as yf
from bs4 import BeautifulSoup

_ARCHIVE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://goodinfo.tw/tw/StockAnnounceList.asp",
}

_GOODINFO_BASE = "https://goodinfo.tw/tw/"
_GOODINFO_NEWS_URL = "https://goodinfo.tw/tw/StockAnnounceList.asp"
_GOODINFO_DATA_URL = "https://goodinfo.tw/tw/data/StockAnnounceList.asp"
_CNYES_CATEGORY_URLS = (
    "https://news.cnyes.com/news/cat/tw_stock",
    "https://news.cnyes.com/news/cat/headline",
)
_GOODINFO_DEFAULT_SOURCES = (
    "ETtoday新聞雲",
    "Anue鉅亨",
    "PR Newswire",
    "Investing.com",
)
_REQUEST_RETRY_DELAYS = (0.0, 0.8, 1.6)
_GOODINFO_MAX_PAGES = 10
_GOODINFO_BROWSER_MAX_SECONDS = 12
_GOODINFO_HTTP_TIMEOUT = 8
_GOODINFO_FALLBACK_MIN_ROWS = 2
_NEWS_ARCHIVE_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_PWCLI_PATH = Path.home() / ".codex" / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
_GOODINFO_BROWSER_EVAL_JS = (
    "Array.from(document.querySelectorAll('tr'))"
    ".map((row) => {"
    "  const detail = Array.from(row.querySelectorAll('a[href]')).find((anchor) => anchor.textContent.includes('詳全文'));"
    "  if (!detail) return null;"
    "  const links = Array.from(row.querySelectorAll('a[href]'));"
    "  const source = links[0]?.textContent?.replace(/\\s+/g, ' ').trim() || '';"
    "  const headlineAnchor = links.find((anchor) => {"
    "    const text = anchor.textContent.replace(/\\s+/g, ' ').trim();"
    "    return text && !text.includes('詳全文') && text !== source;"
    "  }) || links[0];"
    "  return {"
    "    source_name: source,"
    "    headline: headlineAnchor?.textContent?.replace(/\\s+/g, ' ').trim() || '',"
    "    article_url: headlineAnchor?.href || detail.href || '',"
    "    raw_row_text: row.innerText.replace(/\\s+/g, ' ').trim(),"
    "  };"
    "})"
    ".filter(Boolean)"
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
    stock_code: str = "",
    goodinfo_url: str = "",
    page: int | str = "",
    raw_row_text: str = "",
    query_signature: str = "",
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
        "stock_code": stock_code.strip(),
        "goodinfo_url": goodinfo_url.strip(),
        "page": page,
        "raw_row_text": raw_row_text.strip(),
        "query_signature": query_signature.strip(),
    }


class GoodinfoDiscoveryAdapter:
    """Fetch Goodinfo stock/date filtered rows with HTTP-first, browser-fallback logic."""

    def __init__(
        self,
        *,
        stock_code: str,
        queries: list[str] | None,
        date_from: str,
        date_to: str,
        max_results: int,
        data_gaps: list[str] | None = None,
    ) -> None:
        self.stock_code = stock_code.strip()
        self.queries = list(queries or [])
        self.date_from = date_from
        self.date_to = date_to
        self.max_results = max_results
        self.data_gaps = data_gaps if data_gaps is not None else []
        self.query_signature = _build_goodinfo_query_signature(
            stock_code=self.stock_code,
            date_from=self.date_from,
            date_to=self.date_to,
            keyword="",
            sources=_GOODINFO_DEFAULT_SOURCES,
        )

    def fetch_records(self) -> list[dict[str, Any]]:
        started_at = time.monotonic()
        http_records = self._fetch_http_records()
        if not self._should_fallback(http_records):
            return http_records[: self.max_results]

        if not http_records:
            self._append_gap("goodinfo_http_empty")
        browser_pages = min(_GOODINFO_MAX_PAGES, max(1, (self.max_results + 9) // 10))
        browser_budget_seconds = max(20.0, browser_pages * 10.0)
        browser_records = self._fetch_browser_records(
            time_budget_seconds=max(0.0, browser_budget_seconds - (time.monotonic() - started_at))
        )
        merged = dedupe_news_articles(http_records + browser_records)
        if browser_records and http_records:
            self._append_gap("goodinfo_partial_results")
        return merged[: self.max_results]

    def _fetch_http_records(self) -> list[dict[str, Any]]:
        session = requests.Session()
        session.cookies.set("CLIENT_KEY", "codex", domain="goodinfo.tw", path="/")
        records: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        match_tokens = _build_goodinfo_match_tokens(stock_code=self.stock_code, queries=self.queries)

        max_pages = min(_GOODINFO_MAX_PAGES, max(1, (self.max_results + 9) // 10))
        for page in range(1, max_pages + 1):
            try:
                response = _request_with_retry(
                    session.get,
                    _GOODINFO_NEWS_URL,
                    params=_build_goodinfo_url_params(
                        stock_code=self.stock_code,
                        date_from=self.date_from,
                        date_to=self.date_to,
                        page=page,
                        keyword="",
                        sources=_GOODINFO_DEFAULT_SOURCES,
                    ),
                    headers=_ARCHIVE_HEADERS,
                    timeout=_GOODINFO_HTTP_TIMEOUT,
                )
            except requests.Timeout:
                self._append_gap("goodinfo_http_timeout")
                break
            except requests.RequestException as exc:
                self._append_gap(f"goodinfo_page_{page}_unavailable:{type(exc).__name__}")
                break

            response.encoding = "utf-8"
            page_records = _extract_goodinfo_records_from_html(
                html=response.text,
                stock_code=self.stock_code,
                query_signature=self.query_signature,
                match_tokens=match_tokens,
                page=page,
                goodinfo_url=_build_goodinfo_page_url(
                    stock_code=self.stock_code,
                    date_from=self.date_from,
                    date_to=self.date_to,
                    page=page,
                    keyword="",
                    sources=_GOODINFO_DEFAULT_SOURCES,
                ),
                date_from=self.date_from,
                date_to=self.date_to,
            )
            if page == 1 and not page_records:
                self._append_gap("goodinfo_http_empty")
            if page_records and any(
                record.get("published_at", "") and (
                    record["published_at"] < self.date_from or record["published_at"] > self.date_to
                )
                for record in page_records
            ):
                self._append_gap("goodinfo_http_mismatch")
                page_records = [
                    record
                    for record in page_records
                    if not record.get("published_at")
                    or (self.date_from <= record["published_at"] <= self.date_to)
                ]

            added_this_page = 0
            for record in page_records:
                dedupe_key = record.get("dedupe_key", "")
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                records.append(record)
                added_this_page += 1
                if len(records) >= self.max_results:
                    return records[: self.max_results]
            if added_this_page == 0:
                break
        return records[: self.max_results]

    def _should_fallback(self, http_records: list[dict[str, Any]]) -> bool:
        if not http_records:
            return True
        if any(gap.startswith("goodinfo_http_") for gap in self.data_gaps):
            return True
        return len(http_records) < min(self.max_results, _GOODINFO_FALLBACK_MIN_ROWS)

    def _fetch_browser_records(self, *, time_budget_seconds: float) -> list[dict[str, Any]]:
        if time_budget_seconds <= 0 or not _PWCLI_PATH.exists():
            self._append_gap("goodinfo_browser_timeout")
            return []

        records: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        session_name = f"gi{self.stock_code}{int(time.time() * 1000) % 1000000}"
        started_at = time.monotonic()
        try:
            max_pages = min(_GOODINFO_MAX_PAGES, max(1, (self.max_results + 9) // 10))
            init_url = _build_goodinfo_page_url(
                stock_code=self.stock_code,
                date_from=self.date_from,
                date_to=self.date_to,
                page=1,
                keyword="",
                sources=_GOODINFO_DEFAULT_SOURCES,
            )
            self._run_playwright_command(session_name, ["open", init_url], timeout_seconds=min(12.0, max(4.0, time_budget_seconds)))
            self._run_playwright_command(session_name, ["snapshot"], timeout_seconds=min(8.0, max(2.0, time_budget_seconds)))

            for page in range(1, max_pages + 1):
                page_budget_seconds = min(time_budget_seconds - (time.monotonic() - started_at), _GOODINFO_BROWSER_MAX_SECONDS)
                if page_budget_seconds <= 0:
                    self._append_gap("goodinfo_browser_timeout")
                    break
                goodinfo_url = _build_goodinfo_page_url(
                    stock_code=self.stock_code,
                    date_from=self.date_from,
                    date_to=self.date_to,
                    page=page,
                    keyword="",
                    sources=_GOODINFO_DEFAULT_SOURCES,
                )
                result = self._run_playwright_command(
                    session_name,
                    [
                        "eval",
                        _build_goodinfo_browser_data_eval_js(
                            stock_code=self.stock_code,
                            date_from=self.date_from,
                            date_to=self.date_to,
                            page=page,
                            sources=_GOODINFO_DEFAULT_SOURCES,
                        ),
                    ],
                    timeout_seconds=max(3.0, min(10.0, page_budget_seconds)),
                )
                page_rows = _parse_playwright_eval_rows(
                    output=result,
                    stock_code=self.stock_code,
                    query_signature=self.query_signature,
                    match_tokens=[],
                    page=page,
                    goodinfo_url=goodinfo_url,
                    date_from=self.date_from,
                    date_to=self.date_to,
                )
                if not page_rows and page == 1:
                    self._append_gap("goodinfo_browser_empty")
                for record in page_rows:
                    dedupe_key = record.get("dedupe_key", "")
                    if dedupe_key in seen_keys:
                        continue
                    seen_keys.add(dedupe_key)
                    records.append(record)
                    if len(records) >= self.max_results:
                        return records[: self.max_results]
                if not page_rows:
                    break
        except subprocess.TimeoutExpired:
            self._append_gap("goodinfo_browser_timeout")
        except Exception:
            self._append_gap("goodinfo_browser_empty")
        finally:
            try:
                self._run_playwright_command(session_name, ["close"], timeout_seconds=1.5)
            except Exception:
                pass
        return records[: self.max_results]

    def _poll_goodinfo_browser_rows(self, *, session_name: str, time_budget_seconds: float, expected_page: int) -> str:
        """Wait until Goodinfo's filtered result table is rendered before parsing it."""
        deadline = time.monotonic() + max(1.0, time_budget_seconds)
        snapshot_attempted = False
        last_result = ""
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            eval_timeout = max(1.0, min(4.0, remaining))
            try:
                result = self._run_playwright_command(
                    session_name,
                    ["eval", _GOODINFO_BROWSER_EVAL_JS],
                    timeout_seconds=eval_timeout,
                )
                if "### Error" not in result:
                    current_page = self._fetch_goodinfo_browser_page_number(
                        session_name=session_name,
                        timeout_seconds=min(2.0, eval_timeout),
                    )
                    parsed = _parse_playwright_eval_rows(
                        output=result,
                        stock_code=self.stock_code,
                        query_signature=self.query_signature,
                        match_tokens=_build_goodinfo_match_tokens(stock_code=self.stock_code, queries=self.queries),
                        page=1,
                        goodinfo_url="",
                        date_from=self.date_from,
                        date_to=self.date_to,
                    )
                    if parsed and current_page == expected_page:
                        return result
                    last_result = result
            except subprocess.CalledProcessError:
                pass
            except subprocess.TimeoutExpired:
                pass

            if not snapshot_attempted and remaining > 2.0:
                try:
                    self._run_playwright_command(session_name, ["snapshot"], timeout_seconds=min(6.0, remaining))
                    snapshot_attempted = True
                    continue
                except Exception:
                    snapshot_attempted = True
            time.sleep(0.5)
        self._append_gap("goodinfo_browser_timeout")
        return last_result

    def _fetch_goodinfo_browser_page_number(self, *, session_name: str, timeout_seconds: float) -> int:
        """Read the current Goodinfo page number from the rendered DOM."""
        try:
            output = self._run_playwright_command(
                session_name,
                [
                    "eval",
                    "(() => { const el = Array.from(document.querySelectorAll('*')).find((node) => /第\\d+頁\\/ 共\\d+頁/.test((node.innerText || '').trim())); const m = el ? (el.innerText || '').match(/第(\\d+)頁/) : null; return m ? Number(m[1]) : 0; })()",
                ],
                timeout_seconds=max(1.0, timeout_seconds),
            )
            payload = _parse_playwright_result(output)
            return int(payload) if isinstance(payload, (int, float, str)) and str(payload).isdigit() else 0
        except Exception:
            return 0

    def _run_playwright_command(self, session_name: str, args: list[str], *, timeout_seconds: float) -> str:
        command = ["bash", str(_PWCLI_PATH), "--session", session_name, *args]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(1.0, timeout_seconds),
            check=True,
        )
        return completed.stdout

    def _append_gap(self, gap: str) -> None:
        if gap and gap not in self.data_gaps:
            self.data_gaps.append(gap)


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


def _fetch_cnyes_symbol_news_as_normalized(
    *,
    stock_code: str,
    stock_name: str,
    date_from: str,
    date_to: str,
    max_results: int,
    data_gaps: list[str],
) -> list[dict[str, Any]]:
    """Call cnyes_stock_news and convert results to normalized archive record format."""
    from tools.cnyes_stock_news import fetch_cnyes_stock_news
    try:
        result = fetch_cnyes_stock_news(
            stock=stock_code,
            date_from=date_from,
            date_to=date_to,
            stock_name=stock_name,
            match_mode="balanced",
            max_results=max_results,
        )
    except Exception as exc:
        data_gaps.append(f"cnyes_symbol_news_unavailable:{type(exc).__name__}")
        return []

    for gap in result.get("data_gaps", []):
        if gap and gap not in data_gaps:
            data_gaps.append(gap)

    records: list[dict[str, Any]] = []
    for item in result.get("records", []):
        records.append(
            normalize_news_article(
                source="cnyes",
                source_article_id=item.get("news_id", ""),
                published_at=item.get("published_at", "")[:10],
                headline=item.get("title", ""),
                url=item.get("url", ""),
                retrieval_method="cnyes_symbol_news",
                is_primary_source=True,
                stock_code=stock_code,
            )
        )
    return records


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
    cache_key = (
        stock_code,
        stock_name,
        event_type,
        date_from,
        date_to,
        tuple(queries or []),
        max_results,
        primary_source,
        source_policy,
        allow_secondary_sources,
    )
    cached = _NEWS_ARCHIVE_CACHE.get(cache_key)
    if cached is not None:
        return {
            "records": list(cached.get("records", [])),
            "primary_records": list(cached.get("primary_records", [])),
            "secondary_records": list(cached.get("secondary_records", [])),
            "data_gaps": list(cached.get("data_gaps", [])),
            "source_breakdown": dict(cached.get("source_breakdown", {})),
        }

    primary_records: list[dict[str, Any]] = []
    secondary_records: list[dict[str, Any]] = []
    data_gaps: list[str] = []

    if primary_source == "cnyes":
        if stock_code:
            primary_records = _fetch_cnyes_symbol_news_as_normalized(
                stock_code=stock_code,
                stock_name=stock_name,
                date_from=date_from,
                date_to=date_to,
                max_results=max_results,
                data_gaps=data_gaps,
            )
        else:
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
    elif primary_source == "goodinfo":
        try:
            primary_records = fetch_goodinfo_discovery_records(
                stock_code=stock_code,
                queries=queries,
                date_from=date_from,
                date_to=date_to,
                max_results=max_results,
                data_gaps=data_gaps,
            )
        except Exception as exc:
            data_gaps.append(f"goodinfo_primary_unavailable:{type(exc).__name__}")
            primary_records = []
        else:
            primary_records = [{**record, "is_primary_source": True} for record in primary_records]

    if source_policy == "archive_first" and allow_secondary_sources:
        need_secondary = not primary_records or len(primary_records) < max_results
        if need_secondary:
            if primary_source != "goodinfo":
                try:
                    secondary_records.extend(
                        fetch_goodinfo_discovery_records(
                            stock_code=stock_code,
                            queries=queries,
                            date_from=date_from,
                            date_to=date_to,
                            max_results=max_results,
                            data_gaps=data_gaps,
                        )
                    )
                except Exception as exc:
                    data_gaps.append(f"goodinfo_secondary_unavailable:{type(exc).__name__}")

            executor = ThreadPoolExecutor(max_workers=2)
            futures = {
                "google_news": executor.submit(
                    fetch_google_news_rss_records,
                    queries=queries,
                    date_from=date_from,
                    date_to=date_to,
                    max_results=max_results,
                ),
                "yfinance": executor.submit(
                    fetch_yfinance_news_records,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    date_from=date_from,
                    date_to=date_to,
                    max_results=max_results,
                ),
            }
            try:
                for source_name, future in futures.items():
                    try:
                        secondary_records.extend(future.result(timeout=15))
                    except Exception as exc:
                        data_gaps.append(f"{source_name}_secondary_unavailable:{type(exc).__name__}")
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

    primary_records = dedupe_news_articles(primary_records)
    secondary_records = dedupe_news_articles(secondary_records)
    merged_records = dedupe_news_articles(primary_records + secondary_records)
    if len(merged_records) > max_results:
        merged_records = merged_records[:max_results]

    payload = {
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
    _NEWS_ARCHIVE_CACHE[cache_key] = {
        "records": list(payload["records"]),
        "primary_records": list(payload["primary_records"]),
        "secondary_records": list(payload["secondary_records"]),
        "data_gaps": list(payload["data_gaps"]),
        "source_breakdown": dict(payload["source_breakdown"]),
    }
    return payload


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
        try:
            html = _request_text(url, headers=_ARCHIVE_HEADERS, timeout=10)
        except Exception:
            continue
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
    response_text = _request_text(article_url, headers=_ARCHIVE_HEADERS, timeout=10)
    response = _build_response_proxy(response_text)
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
    queries: list[str] | None = None,
    date_from: str,
    date_to: str,
    max_results: int,
    data_gaps: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch Goodinfo stock/date filtered news index records."""
    if not stock_code or not date_from or not date_to:
        return []
    adapter = GoodinfoDiscoveryAdapter(
        stock_code=stock_code,
        queries=queries,
        date_from=date_from,
        date_to=date_to,
        max_results=max_results,
        data_gaps=data_gaps,
    )
    return adapter.fetch_records()


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
        try:
            response_text = _request_text(feed_url, headers=_ARCHIVE_HEADERS, timeout=10)
        except Exception:
            continue
        response = _build_response_proxy(response_text)
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


def _parse_goodinfo_data_row(row: Any) -> dict[str, str] | None:
    """Parse one Goodinfo stock announcement row into normalized fields."""
    source_anchor = row.select_one("th a[href]")
    headline_anchor = row.select_one("td a.link_black[href]")
    detail_anchor = row.find("a", string=lambda value: isinstance(value, str) and "詳全文" in value)
    if not source_anchor or not headline_anchor:
        return None

    source_name = source_anchor.get_text(" ", strip=True)
    headline = headline_anchor.get_text(" ", strip=True)
    article_url = headline_anchor.get("href", "").strip() or source_anchor.get("href", "").strip()
    if not article_url:
        return None

    row_text = row.get_text(" ", strip=True)
    published_at = _extract_date_from_text(row_text)
    snippet = row_text
    if detail_anchor and detail_anchor.parent:
        snippet = detail_anchor.parent.get_text(" ", strip=True)

    return {
        "source_name": source_name,
        "headline": headline,
        "article_url": article_url,
        "published_at": published_at,
        "snippet": snippet,
        "raw_row_text": row_text,
    }


def _extract_goodinfo_records_from_html(
    *,
    html: str,
    stock_code: str,
    query_signature: str,
    match_tokens: list[str],
    page: int,
    goodinfo_url: str,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    records: list[dict[str, Any]] = []
    for row in soup.select("tr"):
        text = row.get_text(" ", strip=True)
        if "詳全文" not in text:
            continue
        row_result = _parse_goodinfo_data_row(row)
        if row_result is None:
            continue
        record = _build_goodinfo_record(
            row_result=row_result,
            stock_code=stock_code,
            query_signature=query_signature,
            page=page,
            goodinfo_url=goodinfo_url,
            retrieval_method="goodinfo_http_index",
            date_from=date_from,
            date_to=date_to,
            match_tokens=match_tokens,
        )
        if record is not None:
            records.append(record)
    return records


def _parse_playwright_eval_rows(
    *,
    output: str,
    stock_code: str,
    query_signature: str,
    match_tokens: list[str],
    page: int,
    goodinfo_url: str,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    payload = _parse_playwright_result(output)
    if not isinstance(payload, list):
        return []
    records: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        raw_row_text = str(item.get("raw_row_text", "")).strip()
        record = _build_goodinfo_record(
            row_result={
                "source_name": str(item.get("source_name", "")).strip(),
                "headline": str(item.get("headline", "")).strip(),
                "article_url": str(item.get("article_url", "")).strip(),
                "published_at": _extract_date_from_text(raw_row_text),
                "snippet": raw_row_text,
                "raw_row_text": raw_row_text,
            },
            stock_code=stock_code,
            query_signature=query_signature,
            page=page,
            goodinfo_url=goodinfo_url,
            retrieval_method="goodinfo_browser_index",
            date_from=date_from,
            date_to=date_to,
            match_tokens=match_tokens,
        )
        if record is not None:
            records.append(record)
    return records


def _build_goodinfo_record(
    *,
    row_result: dict[str, str],
    stock_code: str,
    query_signature: str,
    page: int,
    goodinfo_url: str,
    retrieval_method: str,
    date_from: str,
    date_to: str,
    match_tokens: list[str],
) -> dict[str, Any] | None:
    published_at = row_result.get("published_at", "")
    if date_from and published_at and published_at < date_from:
        return None
    if date_to and published_at and published_at > date_to:
        return None
    normalized_text = normalize_headline(
        f"{row_result.get('source_name', '')} {row_result.get('headline', '')} {row_result.get('snippet', '')}"
    )
    if retrieval_method not in {"goodinfo_http_index", "goodinfo_browser_index"} and match_tokens and not any(
        token in normalized_text for token in match_tokens
    ):
        return None
    return normalize_news_article(
        source=row_result.get("source_name") or "goodinfo_discovery",
        published_at=published_at,
        headline=row_result.get("headline", ""),
        url=row_result.get("article_url", ""),
        category="goodinfo_discovery",
        snippet=row_result.get("snippet", ""),
        raw_payload=row_result,
        retrieval_method=retrieval_method,
        language="zh-TW",
        is_primary_source=False,
        stock_code=stock_code,
        goodinfo_url=goodinfo_url,
        page=page,
        raw_row_text=row_result.get("raw_row_text", row_result.get("snippet", "")),
        query_signature=query_signature,
    )


def _build_goodinfo_page_url(
    *,
    stock_code: str,
    date_from: str,
    date_to: str,
    page: int,
    keyword: str,
    sources: tuple[str, ...] | list[str],
) -> str:
    query = urlencode(
        _build_goodinfo_url_params(
            stock_code=stock_code,
            date_from=date_from,
            date_to=date_to,
            page=page,
            keyword=keyword,
            sources=sources,
        ),
        doseq=True,
    )
    return f"{_GOODINFO_NEWS_URL}?{query}"


def _build_goodinfo_browser_data_eval_js(
    *,
    stock_code: str,
    date_from: str,
    date_to: str,
    page: int,
    sources: tuple[str, ...] | list[str],
) -> str:
    start_dt = quote(_format_goodinfo_date(date_from), safe="")
    end_dt = quote(_format_goodinfo_date(date_to), safe="")
    stock_code_q = quote(stock_code, safe="")
    news_src_q = quote(", ".join(str(item).strip() for item in sources if str(item).strip()), safe="")
    return (
        "(async () => {"
        f" const url = '/tw/data/StockAnnounceList.asp?STEP=DATA&START_DT={start_dt}&END_DT={end_dt}&STOCK_ID={stock_code_q}&KEY_WORD=&NEWS_SRC={news_src_q}&PAGE={page}';"
        " const resp = await fetch(url, { credentials: 'include' });"
        " const html = await resp.text();"
        " const doc = new DOMParser().parseFromString(html, 'text/html');"
        f" return Array.from(doc.querySelectorAll('tr')).map((row) => {{"
        "   const detail = Array.from(row.querySelectorAll('a[href]')).find((anchor) => anchor.textContent.includes('詳全文'));"
        "   if (!detail) return null;"
        "   const links = Array.from(row.querySelectorAll('a[href]'));"
        "   const source = links[0]?.textContent?.replace(/\\s+/g, ' ').trim() || '';"
        "   const headlineAnchor = links.find((anchor) => {"
        "     const text = anchor.textContent.replace(/\\s+/g, ' ').trim();"
        "     return text && !text.includes('詳全文') && text !== source;"
        "   }) || links[0];"
        f"   return {{ source_name: source, headline: headlineAnchor?.textContent?.replace(/\\s+/g, ' ').trim() || '', article_url: headlineAnchor?.href || detail.href || '', raw_row_text: row.innerText.replace(/\\s+/g, ' ').trim(), page: {page} }};"
        " }).filter(Boolean);"
        "})()"
    )


def _build_goodinfo_url_params(
    *,
    stock_code: str,
    date_from: str,
    date_to: str,
    page: int,
    keyword: str,
    sources: tuple[str, ...] | list[str],
) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = [
        ("PAGE", str(page)),
        ("START_DT", date_from),
        ("END_DT", date_to),
        ("STOCK_ID", stock_code),
        ("KEY_WORD", keyword),
    ]
    for source in sources:
        params.append(("NEWS_SRC", source))
    return params


def _build_goodinfo_query_signature(
    *,
    stock_code: str,
    date_from: str,
    date_to: str,
    keyword: str,
    sources: tuple[str, ...] | list[str],
) -> str:
    sources_blob = ",".join(str(item).strip() for item in sources if str(item).strip())
    return f"{stock_code}|{date_from}|{date_to}|{keyword}|{sources_blob}"


def _parse_playwright_result(output: str) -> Any:
    marker = "### Result"
    if marker not in output:
        return None
    tail = output.split(marker, 1)[1]
    lines = []
    for line in tail.splitlines()[1:]:
        if line.startswith("### "):
            break
        lines.append(line)
    body = "\n".join(lines).strip()
    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _build_goodinfo_match_tokens(*, stock_code: str, queries: list[str] | None) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for query in queries or []:
        for raw_token in re.split(r"[\s/,_\-]+", query):
            token = raw_token.strip()
            if not token:
                continue
            lower_token = token.lower()
            if re.fullmatch(r"20\d{2}", token):
                continue
            if re.fullmatch(r"20\d{2}q[1-4]", lower_token):
                continue
            if re.fullmatch(r"q[1-4]", lower_token):
                continue
            normalized = normalize_headline(token)
            if len(normalized) < 2 or normalized in seen:
                continue
            tokens.append(normalized)
            seen.add(normalized)
    return tokens


def _parse_rfc822_date(raw: str) -> str:
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).strftime("%Y-%m-%d")
    except Exception:
        return raw[:10]


def _format_goodinfo_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"


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


def _request_with_retry(request_fn: Any, url: str, *, headers: dict[str, str], timeout: int, **kwargs: Any) -> requests.Response:
    """Call a request function with a small retry/backoff policy."""
    last_exc: Exception | None = None
    for delay in _REQUEST_RETRY_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            response = request_fn(url, headers=headers, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            continue
    assert last_exc is not None
    raise last_exc


def _request_text(url: str, *, headers: dict[str, str], timeout: int, **kwargs: Any) -> str:
    """Return response text with retry/backoff."""
    response = _request_with_retry(requests.get, url, headers=headers, timeout=timeout, **kwargs)
    return response.text


def _build_response_proxy(text: str) -> Any:
    """Provide the tiny subset of the Response interface used by parsers."""
    class _Proxy:
        def __init__(self, body: str) -> None:
            self.text = body
            self.content = body.encode("utf-8")

    return _Proxy(text)


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
