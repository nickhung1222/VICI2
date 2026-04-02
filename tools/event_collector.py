"""Structured event collector for Taiwan stock event research.

Phase 1 design:
- accept explicit event-oriented inputs
- wrap existing news adapters behind a canonical schema
- return stable JSON-friendly records for downstream analysis
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from tools.event_sources import collect_official_event_records
from tools.news_scraper import fetch_article_content, search_news
from tools.schemas import (
    build_comparison_strategy,
    build_stock_target,
    classify_event_phase,
    compact_text,
    dedupe_records,
    infer_record_flags,
)

_PREVIEW_KEYWORDS = ("預期", "展望", "前瞻", "市場看", "估", "法說前", "財測", "指引")
_ANALYST_KEYWORDS = ("分析師", "法人", "外資", "目標價", "評等")


def collect_event_records(
    symbol: str,
    event_type: str,
    start_date: str,
    end_date: str,
    stock_name: str = "",
    stock_code: str = "",
    event_date: str = "",
    event_key: str = "",
    max_results: int = 12,
) -> dict[str, Any]:
    """Collect structured event-related records for a Taiwan stock.

    Returns a JSON-serializable payload that downstream tools can consume.
    """
    target = build_stock_target(symbol=symbol, code=stock_code, name=stock_name)
    comparison_strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    official_payload = collect_official_event_records(
        stock_code=target["code"],
        stock_name=target["name"],
        symbol=target["symbol"],
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        event_date=event_date,
        event_key=comparison_strategy["event_key"],
    )
    queries = build_collection_queries(
        target=target,
        event_type=event_type,
        event_key=comparison_strategy["event_key"],
        event_date=event_date,
    )

    if not queries:
        raise ValueError("Unable to build collection queries. Provide a valid symbol or stock name.")

    per_query = max(4, math.ceil(max_results / len(queries)) + 1)
    ranked_articles: list[dict[str, Any]] = []
    archive_count = 0
    secondary_count = 0
    live_fetched_count = 0

    for query in queries:
        articles = search_news(
            query=query,
            date_from=start_date,
            date_to=end_date,
            max_results=per_query,
            stock_code=target["code"],
            stock_name=target["name"],
            event_type=event_type,
            queries=queries,
            source_policy="archive_first",
            primary_source="cnyes",
            allow_secondary_sources=True,
        )
        for article in articles:
            article_copy = dict(article)
            article_copy["matched_query"] = query
            article_copy["_score"] = _score_article(
                article=article_copy,
                target=target,
                event_type=event_type,
                event_date=event_date,
            )
            ranked_articles.append(article_copy)
            if article_copy.get("is_primary_source"):
                archive_count += 1
            elif article_copy.get("retrieval_method") in {"google_news_rss", "goodinfo_stock_date_index", "yfinance_get_news"}:
                secondary_count += 1
            else:
                live_fetched_count += 1
        if len(dedupe_records(ranked_articles, ["url", "title", "date"])) >= max_results:
            break

    ranked_articles.sort(key=lambda item: (-item["_score"], item.get("date", ""), item.get("title", "")))
    ranked_articles = dedupe_records(ranked_articles, ["url", "title", "date"])
    selected_articles = ranked_articles[:max_results]

    records: list[dict[str, Any]] = list(official_payload["records"])
    for index, article in enumerate(selected_articles):
        content = ""
        # Avoid long report latency when fallback results come from arbitrary web pages.
        # Only fetch full text for the first few articles when we have a direct cnyes id.
        if index < 2 and (article.get("news_id") or article.get("source") in {"cnyes", "moneydj"}):
            content = fetch_article_content(
                url=article.get("url", ""),
                news_id=article.get("news_id", ""),
            )

        summary_source = content or article.get("snippet", "")
        article_type = _classify_article_type(
            event_type=event_type,
            title=article.get("title", ""),
            snippet=article.get("snippet", ""),
        )
        article_date = article.get("date", "")
        event_phase = classify_event_phase(article_date=article_date, event_date=event_date)
        record_flags = infer_record_flags(
            event_phase=event_phase,
            article_type=article_type,
            source_type="media",
        )
        records.append(
            {
                "stock_code": target["code"],
                "stock_name": target["name"],
                "symbol": target["symbol"],
                "event_type": event_type,
                "event_date": event_date,
                "event_key": comparison_strategy["event_key"],
                "event_phase": event_phase,
                "article_date": article_date,
                "article_type": article_type,
                "source_type": "media",
                "source_name": article.get("source", ""),
                "source_url": article.get("url", ""),
                "headline": article.get("title", ""),
                "summary": compact_text(summary_source, max_length=200),
                "language": "zh-TW",
                "matched_query": article.get("matched_query", ""),
                "source_article_id": article.get("source_article_id", article.get("news_id", "")),
                "published_at": article.get("date", ""),
                "retrieval_method": article.get("retrieval_method", ""),
                "is_primary_source": article.get("is_primary_source", False),
                "dedupe_key": article.get("dedupe_key", ""),
                **record_flags,
            }
        )

    records = dedupe_records(records, ["stock_code", "event_date", "article_date", "headline", "source_url"])
    records.sort(key=lambda item: (item.get("article_date", ""), item.get("headline", "")))

    return {
        "query": {
            "event_type": event_type,
            "time_range": {
                "start": start_date,
                "end": end_date,
            },
            "stock": target,
            "event_date": event_date,
            "event_key": comparison_strategy["event_key"],
            "max_results": max_results,
        },
        "collection_plan": {
            "queries": queries,
            "sources": _build_source_list(official_payload["records"]),
            "mode": "重點整理",
            "comparison_strategy": comparison_strategy,
            "source_policy": "archive_first",
            "primary_source": "cnyes",
        },
        "data_completeness": {
            "mode": "重點整理",
            "official_sources_included": bool(official_payload["records"]),
            "heat_analysis_included": False,
            "comparison_strategy": comparison_strategy["comparison_mode"],
            "comparison_ready": comparison_strategy["comparison_ready"],
            "data_gaps": _merge_data_gaps(comparison_strategy["data_gaps"], official_payload["data_gaps"]),
            "notes": (
                "Collector wraps media search adapters into a structured event schema "
                "and now attempts MOPS official records for supported event types."
            ),
        },
        "record_count": len(records),
        "record_breakdown": {
            "archive_records": archive_count,
            "secondary_source_records": secondary_count,
            "live_fetched_records": live_fetched_count,
        },
        "records": records,
    }


def _build_source_list(official_records: list[dict[str, Any]]) -> list[str]:
    """Return ordered source names used by the collector."""
    sources = ["cnyes", "moneydj", "web_search"]
    if official_records:
        sources.insert(0, "mops")
    return sources


def _merge_data_gaps(*gap_lists: list[str]) -> list[str]:
    """Merge data-gap strings while preserving order."""
    merged: list[str] = []
    for gap_list in gap_lists:
        for gap in gap_list:
            if gap and gap not in merged:
                merged.append(gap)
    return merged


def build_collection_queries(
    target: dict[str, str],
    event_type: str,
    event_key: str = "",
    event_date: str = "",
) -> list[str]:
    """Build event-first search queries from normalized stock metadata."""
    anchors = [target.get("name", ""), target.get("code", ""), target.get("symbol", "").split(".", 1)[0]]
    year = event_date[:4] if event_date else ""

    queries: list[str] = []
    seen: set[str] = set()
    for anchor in anchors:
        anchor = anchor.strip()
        if not anchor:
            continue
        candidates: list[str] = []
        if event_key:
            candidates.append(f"{anchor} {event_key} {event_type}")
        candidates.append(f"{anchor} {event_type}")
        if year:
            candidates.append(f"{anchor} {event_type} {year}")
        for query in candidates:
            if query in seen:
                continue
            seen.add(query)
            queries.append(query)

    return queries


def _classify_article_type(event_type: str, title: str, snippet: str) -> str:
    """Infer a downstream-friendly article type from title and snippet."""
    text = f"{title} {snippet}"

    if event_type == "法說會":
        if any(keyword in title for keyword in _ANALYST_KEYWORDS):
            return "分析師觀點"
        if any(keyword in text for keyword in _PREVIEW_KEYWORDS):
            return "法說前預期"
        if any(keyword in text for keyword in _ANALYST_KEYWORDS):
            return "分析師觀點"

    return "媒體報導"


def _score_article(article: dict[str, Any], target: dict[str, str], event_type: str, event_date: str) -> float:
    """Rank articles by stock/event match strength and date proximity."""
    title = article.get("title", "")
    snippet = article.get("snippet", "")
    article_date = article.get("date", "")
    text = f"{title} {snippet}"
    score = 0.0

    if target.get("name") and target["name"] in text:
        score += 4.0
    if target.get("code") and target["code"] in text:
        score += 3.0
    if event_type in text:
        score += 4.0

    if event_date and article_date:
        try:
            event_dt = datetime.strptime(event_date, "%Y-%m-%d")
            article_dt = datetime.strptime(article_date, "%Y-%m-%d")
            day_gap = abs((event_dt - article_dt).days)
            score += max(0.0, 3.0 - min(day_gap, 7) * 0.4)
        except ValueError:
            pass

    return score
