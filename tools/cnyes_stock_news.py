"""Standalone Cnyes stock-news interval fetcher.

This module is intentionally isolated from the main event-study flow.
It provides a direct HTTP-based path for fetching stock-scoped Cnyes news
within a requested Taipei-local date interval.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import unescape
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from tools.schemas import build_stock_target

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
_REQUEST_TIMEOUT = 20
_SYMBOL_NEWS_PAGE_SIZE = 25
_KEYWORD_PAGE_SIZE = 20
_MAX_KEYWORD_PAGES = 100
_MATCH_MODES = {"strict", "balanced", "broad"}
_RELEVANCE_RANK = {"direct": 0, "tagged": 1, "unmatched": 2}
_MATCHED_BY_ORDER = {
    "title_alias": 0,
    "summary_alias": 1,
    "keyword_tag_alias": 2,
    "article_page_alias": 3,
    "market_symbol": 4,
    "other_product_symbol": 5,
    "article_page_other_product": 6,
}
_SYMBOL_NEWS_URL = "https://api.cnyes.com/media/api/v1/newslist/{symbol}/symbolNews"
_KEYWORD_URL = "https://ess.api.cnyes.com/ess/api/v1/news/keyword"
_STOCK_PAGE_URL = "https://www.cnyes.com/twstock/{stock_code}"
_ARTICLE_URL_TEMPLATE = "https://news.cnyes.com/news/id/{news_id}"
_CODE_TEXT_PATTERN = r"(?<!\d){code}(?!\d)"

_API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}
_ARTICLE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


@dataclass
class SourceFetchState:
    records: list[dict[str, Any]]
    pages_fetched: int
    oldest_local_date: date | None
    oldest_published_at: str
    total_available: int | None
    request_failed: bool = False


def fetch_cnyes_stock_news(
    stock: str,
    date_from: str,
    date_to: str,
    stock_name: str = "",
    match_mode: str = "balanced",
    max_results: int = 200,
) -> dict[str, Any]:
    """Fetch stock-related Cnyes news within a Taipei-local date interval."""

    if match_mode not in _MATCH_MODES:
        raise ValueError(f"Unsupported match_mode: {match_mode!r}")
    if max_results <= 0:
        raise ValueError("max_results must be greater than 0")

    start_date = _parse_date(date_from)
    end_date = _parse_date(date_to)
    if start_date > end_date:
        raise ValueError("date_from must be earlier than or equal to date_to")

    stock_code, symbol = _normalize_stock(stock)
    session = requests.Session()
    data_gaps: list[str] = []

    try:
        resolved_name = stock_name.strip() or _infer_stock_name(session, stock_code, data_gaps)

        primary_state = _collect_symbol_news(
            session=session,
            symbol=symbol,
            stock_code=stock_code,
            stock_name=resolved_name,
            start_date=start_date,
            end_date=end_date,
            data_gaps=data_gaps,
        )

        merged_records: dict[str, dict[str, Any]] = {}
        for record in primary_state.records:
            _merge_record(merged_records, record)

        keyword_queries: list[str] = []
        fallback_used = primary_state.oldest_local_date is None or primary_state.oldest_local_date > start_date
        keyword_records: list[dict[str, Any]] = []

        if fallback_used:
            keyword_queries = _build_keyword_queries(resolved_name, stock_code)
            for query_rank, query in enumerate(keyword_queries):
                keyword_records.extend(
                    _collect_keyword_records(
                        session=session,
                        query=query,
                        query_rank=query_rank,
                        stock_code=stock_code,
                        stock_name=resolved_name,
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        data_gaps=data_gaps,
                    )
                )
            for record in keyword_records:
                _merge_record(merged_records, record)

        merged_list = list(merged_records.values())
        final_records = _apply_match_mode(merged_list, match_mode=match_mode, max_results=max_results)

        if not final_records and not _has_request_failure(data_gaps):
            _append_gap(data_gaps, "no_news_in_interval")

        return {
            "stock_code": stock_code,
            "stock_name": resolved_name,
            "symbol": symbol,
            "date_from": start_date.isoformat(),
            "date_to": end_date.isoformat(),
            "match_mode": match_mode,
            "records": [_serialize_record(record) for record in final_records],
            "coverage": {
                "timezone": "Asia/Taipei",
                "symbol_news_pages_fetched": primary_state.pages_fetched,
                "symbol_news_total_available": primary_state.total_available,
                "symbol_news_oldest_date": (
                    primary_state.oldest_local_date.isoformat() if primary_state.oldest_local_date else None
                ),
                "symbol_news_oldest_published_at": primary_state.oldest_published_at or None,
                "keyword_fallback_used": fallback_used,
                "keyword_queries": keyword_queries,
            },
            "source_summary": {
                "symbol_news_candidates": len(primary_state.records),
                "keyword_fallback_candidates": len(keyword_records),
                "merged_candidates": len(merged_list),
                "returned_records": len(final_records),
            },
            "data_gaps": data_gaps,
        }
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()


def _normalize_stock(stock: str) -> tuple[str, str]:
    cleaned = stock.strip()
    if not cleaned:
        raise ValueError("stock is required")

    symbol_match = re.fullmatch(r"TWS:(\d+):STOCK", cleaned, flags=re.IGNORECASE)
    if symbol_match:
        stock_code = symbol_match.group(1)
        return stock_code, f"TWS:{stock_code}:STOCK"

    target = build_stock_target(cleaned)
    stock_code = target.get("code", "").strip()
    if not stock_code:
        raise ValueError(f"Unable to normalize Taiwan stock code from: {stock!r}")
    return stock_code, f"TWS:{stock_code}:STOCK"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_article_url(news_id: str) -> str:
    return _ARTICLE_URL_TEMPLATE.format(news_id=str(news_id).strip())


def _infer_stock_name(session: requests.Session, stock_code: str, data_gaps: list[str]) -> str:
    url = _STOCK_PAGE_URL.format(stock_code=stock_code)

    try:
        response = session.get(url, headers=_ARTICLE_HEADERS, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        _append_gap(data_gaps, "stock_name_inference_failed")
        return ""

    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title_match = re.match(rf"\s*(.+?)\s+{re.escape(stock_code)}\b", title)
    if title_match:
        return title_match.group(1).strip()

    description = _extract_meta_content(soup, "description")
    desc_match = re.search(rf"([^\s()（）]+)\s*\({re.escape(stock_code)}\)", description)
    if desc_match:
        return desc_match.group(1).strip()

    _append_gap(data_gaps, "stock_name_inference_failed")
    return ""


def _collect_symbol_news(
    *,
    session: requests.Session,
    symbol: str,
    stock_code: str,
    stock_name: str,
    start_date: date,
    end_date: date,
    data_gaps: list[str],
) -> SourceFetchState:
    records: list[dict[str, Any]] = []
    oldest_local_date: date | None = None
    oldest_published_at = ""
    page = 1
    pages_fetched = 0
    total_available: int | None = None

    while True:
        try:
            response = session.get(
                _SYMBOL_NEWS_URL.format(symbol=symbol),
                params={"page": page, "limit": _SYMBOL_NEWS_PAGE_SIZE},
                headers={**_API_HEADERS, "Referer": _STOCK_PAGE_URL.format(stock_code=stock_code)},
                timeout=_REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            _append_gap(data_gaps, "symbol_news_request_failed")
            return SourceFetchState(
                records=records,
                pages_fetched=pages_fetched,
                oldest_local_date=oldest_local_date,
                oldest_published_at=oldest_published_at,
                total_available=total_available,
                request_failed=True,
            )

        items_meta = payload.get("items") or {}
        if total_available is None:
            total_available = items_meta.get("total")
        items = items_meta.get("data") or []
        if not items:
            if page == 1:
                _append_gap(data_gaps, "symbol_news_empty")
            break

        pages_fetched += 1
        page_oldest_date: date | None = None

        for item in items:
            record = _normalize_record(
                item=item,
                source="cnyes_symbol_news",
                source_rank=0,
                stock_code=stock_code,
                stock_name=stock_name,
                symbol=symbol,
            )
            if record is None:
                continue
            local_date = record["_local_date"]
            if oldest_local_date is None or local_date < oldest_local_date:
                oldest_local_date = local_date
                oldest_published_at = record["published_at"]
            if page_oldest_date is None or local_date < page_oldest_date:
                page_oldest_date = local_date
            if start_date <= local_date <= end_date:
                records.append(record)

        last_page = items_meta.get("last_page") or page
        if page_oldest_date is not None and page_oldest_date <= start_date:
            break
        if page >= last_page:
            break
        page += 1

    return SourceFetchState(
        records=records,
        pages_fetched=pages_fetched,
        oldest_local_date=oldest_local_date,
        oldest_published_at=oldest_published_at,
        total_available=total_available,
        request_failed=False,
    )


def _collect_keyword_records(
    *,
    session: requests.Session,
    query: str,
    query_rank: int,
    stock_code: str,
    stock_name: str,
    symbol: str,
    start_date: date,
    end_date: date,
    data_gaps: list[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    page = 1
    found_interval_record = False

    while page <= _MAX_KEYWORD_PAGES:
        try:
            response = session.get(
                _KEYWORD_URL,
                params={"q": query, "page": page, "limit": _KEYWORD_PAGE_SIZE},
                headers={
                    **_API_HEADERS,
                    "Referer": f"https://www.cnyes.com/search/news?keyword={requests.utils.quote(query)}",
                },
                timeout=_REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            _append_gap(data_gaps, "keyword_fallback_request_failed")
            break

        items = ((payload.get("data") or {}).get("items") or [])
        if not items:
            break

        oldest_page_date: date | None = None

        for item in items:
            record = _normalize_record(
                item=item,
                source="cnyes_keyword_fallback",
                source_rank=1,
                stock_code=stock_code,
                stock_name=stock_name,
                symbol=symbol,
            )
            if record is None:
                continue
            local_date = record["_local_date"]
            if oldest_page_date is None or local_date < oldest_page_date:
                oldest_page_date = local_date
            if not (start_date <= local_date <= end_date):
                continue
            found_interval_record = True
            if record["relevance"] == "unmatched":
                verified = _verify_record_with_article_page(
                    session=session,
                    news_id=record["news_id"],
                    stock_code=stock_code,
                    stock_name=stock_name,
                    symbol=symbol,
                    data_gaps=data_gaps,
                )
                if verified["matched_by"]:
                    record["matched_by"] = _merge_matched_by(record["matched_by"], verified["matched_by"])
                    record["relevance"] = verified["relevance"]
                record["url"] = verified["url"] or record["url"]
            records.append(record)

        if oldest_page_date is not None and oldest_page_date <= start_date:
            break
        # Historical coverage for stock-name search can drift slowly and create
        # many low-value requests. Switch to the stock-code query once it is
        # clear the name query is not converging toward the requested interval.
        if (
            query_rank == 0
            and not found_interval_record
            and oldest_page_date is not None
            and oldest_page_date > end_date
            and page >= 10
        ):
            break
        if len(items) < _KEYWORD_PAGE_SIZE:
            break
        page += 1

    return records


def _normalize_record(
    *,
    item: dict[str, Any],
    source: str,
    source_rank: int,
    stock_code: str,
    stock_name: str,
    symbol: str,
) -> dict[str, Any] | None:
    news_id = str(item.get("newsId") or "").strip()
    published_at = _timestamp_to_local_iso(item.get("publishAt"))
    if not news_id or not published_at:
        return None

    title = _clean_text(item.get("title", ""))
    summary = _clean_text(item.get("summary") or item.get("content") or "")
    keyword_tags = [_clean_text(tag) for tag in (item.get("keywordForTag") or []) if _clean_text(tag)]

    matched_by: list[str] = []
    if _text_contains_alias(title, stock_code=stock_code, stock_name=stock_name):
        matched_by.append("title_alias")
    if summary and _text_contains_alias(summary, stock_code=stock_code, stock_name=stock_name):
        matched_by.append("summary_alias")
    if keyword_tags and any(
        _text_contains_alias(tag, stock_code=stock_code, stock_name=stock_name) for tag in keyword_tags
    ):
        matched_by.append("keyword_tag_alias")

    tagged_hits: list[str] = []
    if _item_contains_target_symbol(item.get("market"), stock_code=stock_code, symbol=symbol):
        tagged_hits.append("market_symbol")
    if _item_contains_target_symbol(item.get("otherProduct"), stock_code=stock_code, symbol=symbol):
        tagged_hits.append("other_product_symbol")

    relevance = "unmatched"
    if matched_by:
        relevance = "direct"
    elif tagged_hits:
        relevance = "tagged"

    local_dt = datetime.fromisoformat(published_at)
    return {
        "news_id": news_id,
        "published_at": published_at,
        "title": title,
        "url": _build_article_url(news_id),
        "source": source,
        "relevance": relevance,
        "matched_by": _merge_matched_by(matched_by, tagged_hits),
        "_source_rank": source_rank,
        "_published_ts": int(item.get("publishAt") or 0),
        "_local_date": local_dt.date(),
    }


def _verify_record_with_article_page(
    *,
    session: requests.Session,
    news_id: str,
    stock_code: str,
    stock_name: str,
    symbol: str,
    data_gaps: list[str],
) -> dict[str, Any]:
    url = _build_article_url(news_id)

    try:
        response = session.get(
            url,
            headers={**_ARTICLE_HEADERS, "Referer": "https://news.cnyes.com/"},
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException:
        _append_gap(data_gaps, "article_verification_failed")
        return {"url": url, "relevance": "unmatched", "matched_by": []}

    soup = BeautifulSoup(response.text, "html.parser")
    canonical_url = _extract_canonical_url(soup) or url
    article_text = " ".join(
        part
        for part in [
            soup.title.get_text(" ", strip=True) if soup.title else "",
            _extract_meta_content(soup, "description"),
            soup.get_text(" ", strip=True),
        ]
        if part
    )

    if _text_contains_alias(article_text, stock_code=stock_code, stock_name=stock_name):
        return {"url": canonical_url, "relevance": "direct", "matched_by": ["article_page_alias"]}

    if _html_contains_target_symbol(response.text, stock_code=stock_code, symbol=symbol):
        return {"url": canonical_url, "relevance": "tagged", "matched_by": ["article_page_other_product"]}

    return {"url": canonical_url, "relevance": "unmatched", "matched_by": []}


def _extract_meta_content(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": f"og:{name}"})
    if tag and tag.get("content"):
        return _clean_text(tag["content"])
    return ""


def _extract_canonical_url(soup: BeautifulSoup) -> str:
    link = soup.find("link", attrs={"rel": "canonical"})
    if link and link.get("href"):
        return str(link["href"]).strip()
    return ""


def _timestamp_to_local_iso(value: Any) -> str:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).astimezone(TAIPEI_TZ).isoformat()


def _clean_text(value: Any) -> str:
    text = unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", "", text)
    return " ".join(text.split())


def _text_contains_alias(text: str, *, stock_code: str, stock_name: str) -> bool:
    cleaned = _clean_text(text)
    if not cleaned:
        return False

    lowered = cleaned.casefold()
    if stock_name and stock_name.casefold() in lowered:
        return True

    if stock_code and re.search(_CODE_TEXT_PATTERN.format(code=re.escape(stock_code)), cleaned):
        return True

    return False


def _item_contains_target_symbol(value: Any, *, stock_code: str, symbol: str) -> bool:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                item_code = str(item.get("code") or "").replace("US-", "").strip()
                item_symbol = str(item.get("symbol") or "").strip()
                if item_code == stock_code or item_symbol == symbol:
                    return True
            elif isinstance(item, str):
                normalized = item.strip()
                if normalized == symbol or normalized.startswith(f"{symbol}:"):
                    return True
                if re.search(_CODE_TEXT_PATTERN.format(code=re.escape(stock_code)), normalized):
                    return True
    return False


def _html_contains_target_symbol(html: str, *, stock_code: str, symbol: str) -> bool:
    if symbol in html or f"{symbol}:COMMON" in html:
        return True
    return bool(re.search(_CODE_TEXT_PATTERN.format(code=re.escape(stock_code)), html))


def _build_keyword_queries(stock_name: str, stock_code: str) -> list[str]:
    queries: list[str] = []
    if stock_name:
        queries.append(stock_name.strip())
    queries.append(stock_code.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if not query or query in seen:
            continue
        deduped.append(query)
        seen.add(query)
    return deduped


def _merge_record(records: dict[str, dict[str, Any]], record: dict[str, Any]) -> None:
    news_id = record["news_id"]
    existing = records.get(news_id)
    if existing is None:
        records[news_id] = record
        return

    preferred = existing
    secondary = record
    if record["_source_rank"] < existing["_source_rank"]:
        preferred = record
        secondary = existing

    merged = preferred.copy()
    merged["matched_by"] = _merge_matched_by(preferred["matched_by"], secondary["matched_by"])
    if _RELEVANCE_RANK[secondary["relevance"]] < _RELEVANCE_RANK[merged["relevance"]]:
        merged["relevance"] = secondary["relevance"]
    if not merged.get("title"):
        merged["title"] = secondary.get("title", "")
    if not merged.get("url"):
        merged["url"] = secondary.get("url", "")
    records[news_id] = merged


def _merge_matched_by(*groups: list[str]) -> list[str]:
    values = {value for group in groups for value in group if value}
    return sorted(values, key=lambda value: (_MATCHED_BY_ORDER.get(value, 99), value))


def _apply_match_mode(records: list[dict[str, Any]], *, match_mode: str, max_results: int) -> list[dict[str, Any]]:
    if match_mode == "strict":
        filtered = [record for record in records if record["relevance"] == "direct"]
        sort_key = lambda record: (-record["_published_ts"], record["news_id"])
    elif match_mode == "balanced":
        filtered = [record for record in records if record["relevance"] in {"direct", "tagged"}]
        sort_key = lambda record: (
            _RELEVANCE_RANK[record["relevance"]],
            -record["_published_ts"],
            record["news_id"],
        )
    else:
        filtered = list(records)
        sort_key = lambda record: (-record["_published_ts"], record["news_id"])

    return sorted(filtered, key=sort_key)[:max_results]


def _serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "news_id": record["news_id"],
        "published_at": record["published_at"],
        "title": record["title"],
        "url": record["url"],
        "source": record["source"],
        "relevance": record["relevance"],
        "matched_by": list(record["matched_by"]),
    }


def _append_gap(data_gaps: list[str], gap: str) -> None:
    if gap and gap not in data_gaps:
        data_gaps.append(gap)


def _has_request_failure(data_gaps: list[str]) -> bool:
    return any(gap.endswith("_request_failed") for gap in data_gaps)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch stock-scoped Cnyes news within a date interval.")
    parser.add_argument("--stock", required=True, help="Taiwan stock code, e.g. 2330 or 2330.TW")
    parser.add_argument("--date-from", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--stock-name", default="", help="Optional stock name, e.g. 台積電")
    parser.add_argument(
        "--match-mode",
        default="balanced",
        choices=sorted(_MATCH_MODES),
        help="Relevance filter mode",
    )
    parser.add_argument("--max-results", type=int, default=200, help="Maximum number of returned records")

    args = parser.parse_args(argv)
    payload = fetch_cnyes_stock_news(
        stock=args.stock,
        date_from=args.date_from,
        date_to=args.date_to,
        stock_name=args.stock_name,
        match_mode=args.match_mode,
        max_results=args.max_results,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
