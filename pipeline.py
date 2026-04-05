"""Deterministic event pipeline entry points.

These functions run structured, step-by-step workflows without going through
the LLM tool-use loop. Each function calls the relevant tool modules directly
and produces JSON / Markdown outputs.

This module is the project's primary execution path.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from tools.event_collector import collect_event_records
from tools.event_sources import resolve_earnings_event_date
from tools.event_study import run_event_study
from tools.heat_analysis import scan_event_heat
from tools.post_event_analysis import build_post_event_analysis
from tools.report import build_event_report_payload, save_event_record, save_report
from tools.schemas import dedupe_records
from tools.stock_data import fetch_stock_data


# ---------------------------------------------------------------------------
# event_collect
# ---------------------------------------------------------------------------

def event_collect(
    stock: str,
    event_type: str,
    start_date: str,
    end_date: str,
    stock_name: str = "",
    event_date: str = "",
    event_key: str = "",
    max_results: int = 12,
) -> str:
    """Collect structured event-oriented records and save them as JSON.

    Args:
        stock: Yahoo Finance symbol or Taiwan stock code, e.g. '2330.TW' or '2330'
        event_type: Event label, e.g. '法說會'
        start_date: Collection start date YYYY-MM-DD
        end_date: Collection end date YYYY-MM-DD
        stock_name: Optional Chinese stock name for query expansion
        event_date: Optional specific event date YYYY-MM-DD
        event_key: Optional recurring event key, e.g. '2025Q4' for earnings-call comparisons
        max_results: Maximum number of records to save

    Returns:
        Path to the saved JSON record file.
    """
    print(f"[pipeline] mode: event_collect")
    print(f"[pipeline] stock: {stock}")
    print(f"[pipeline] event type: {event_type}")
    print(f"[pipeline] range: {start_date} -> {end_date}")
    if event_date:
        print(f"[pipeline] event date: {event_date}")
    if event_key:
        print(f"[pipeline] event key: {event_key}")
    print()

    resolved_event_date = _resolve_pipeline_event_date(
        stock=stock,
        stock_name=stock_name,
        event_type=event_type,
        event_date=event_date,
        event_key=event_key,
        start_date=start_date,
        end_date=end_date,
    )
    payload = collect_event_records(
        symbol=stock,
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        stock_name=stock_name,
        event_date=resolved_event_date,
        event_key=event_key,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
    )
    topic = f"{payload['query']['stock']['code'] or stock}_{event_type}"
    return save_event_record(payload, topic=topic)


# ---------------------------------------------------------------------------
# heat_scan
# ---------------------------------------------------------------------------

def heat_scan(
    stock: str,
    event_type: str,
    event_date: str,
    stock_name: str = "",
    event_key: str = "",
    comparison_event_date: str = "",
    max_results: int = 24,
    phase: str = "both",
) -> str:
    """Run structured heat analysis and save it as JSON."""
    print(f"[pipeline] mode: heat_scan")
    print(f"[pipeline] stock: {stock}")
    print(f"[pipeline] event type: {event_type}")
    print(f"[pipeline] event date: {event_date}")
    if event_key:
        print(f"[pipeline] event key: {event_key}")
    if comparison_event_date:
        print(f"[pipeline] comparison event date: {comparison_event_date}")
    print(f"[pipeline] phase: {phase}")
    print()

    resolved_event_date = _resolve_pipeline_event_date(
        stock=stock,
        stock_name=stock_name,
        event_type=event_type,
        event_date=event_date,
        event_key=event_key,
    )
    payload = scan_event_heat(
        symbol=stock,
        event_type=event_type,
        event_date=resolved_event_date,
        stock_name=stock_name,
        event_key=event_key,
        comparison_event_date=comparison_event_date,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
        phase=phase,
    )
    topic = f"{payload['stock']['code'] or stock}_{event_type}_heat"
    return save_event_record(payload, topic=topic)


# ---------------------------------------------------------------------------
# event_report
# ---------------------------------------------------------------------------

def event_report(
    stock: str,
    event_type: str,
    start_date: str,
    end_date: str,
    event_date: str,
    stock_name: str = "",
    event_key: str = "",
    comparison_event_date: str = "",
    max_results: int = 24,
    include_event_study: bool = False,
    topic: str = "",
) -> dict[str, str]:
    """Build and save an integrated event report in JSON and Markdown."""
    print(f"[pipeline] mode: event_report")
    print(f"[pipeline] stock: {stock}")
    print(f"[pipeline] event type: {event_type}")
    print(f"[pipeline] range: {start_date} -> {end_date}")
    print(f"[pipeline] event date: {event_date}")
    if event_key:
        print(f"[pipeline] event key: {event_key}")
    if comparison_event_date:
        print(f"[pipeline] comparison event date: {comparison_event_date}")
    print(f"[pipeline] include event study: {include_event_study}")
    print()
    resolved_event_date = _resolve_pipeline_event_date(
        stock=stock,
        stock_name=stock_name,
        event_type=event_type,
        event_date=event_date,
        event_key=event_key,
        start_date=start_date,
        end_date=end_date,
    )

    event_collection = _collect_phase_partitioned_event_records(
        stock=stock,
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        event_date=resolved_event_date,
        stock_name=stock_name,
        event_key=event_key,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
    )
    heat_payload = scan_event_heat(
        symbol=stock,
        event_type=event_type,
        event_date=resolved_event_date,
        stock_name=stock_name,
        event_key=event_key,
        comparison_event_date=comparison_event_date,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
        current_pre_event_payload=(event_collection.get("phase_collections", {}) or {}).get("pre_event"),
        current_post_event_payload=(event_collection.get("phase_collections", {}) or {}).get("post_event"),
    )
    post_event_analysis = (
        build_post_event_analysis(
            event_collection=event_collection,
            source_records=((heat_payload.get("post_event_heat_scan") or {}).get("current_records", [])),
        )
        if event_type == "法說會"
        else {}
    )
    event_study_payload = (
        _build_event_study_payload(
            stock=stock,
            event_date=resolved_event_date,
            end_date=end_date,
            reaction_shift_trading_days=1 if event_type == "法說會" else 0,
        )
        if include_event_study
        else None
    )

    report_payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=heat_payload,
        post_event_analysis=post_event_analysis,
        event_study=event_study_payload,
        title=topic or event_collection["query"]["stock"].get("name") or event_type,
    )
    safe_topic = topic or f"{event_collection['query']['stock']['code'] or stock}_{event_type}_event_report"
    json_path = save_event_record(report_payload, topic=safe_topic)
    markdown_path = save_report(report_payload["markdown"], topic=safe_topic)
    return {
        "json_path": json_path,
        "markdown_path": markdown_path,
    }


def _resolve_pipeline_event_date(
    *,
    stock: str,
    stock_name: str,
    event_type: str,
    event_date: str,
    event_key: str,
    start_date: str = "",
    end_date: str = "",
) -> str:
    if event_type != "法說會":
        return event_date
    stock_code = stock.split(".", 1)[0] if stock else ""
    resolution = resolve_earnings_event_date(
        stock_code=stock_code,
        stock_name=stock_name,
        symbol=stock,
        start_date=start_date,
        end_date=end_date,
        event_date=event_date,
        event_key=event_key,
    )
    return resolution.get("resolved_event_date", event_date) or event_date


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_event_study_payload(
    stock: str,
    event_date: str,
    end_date: str,
    reaction_shift_trading_days: int = 0,
) -> dict[str, Any]:
    """Run deterministic event study for a single event date."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    requested_end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Event study needs data after the event to fill the full event window.
    # Extend the requested range when the report end date stops too close to t=0.
    effective_end_dt = max(requested_end_dt, event_dt + timedelta(days=15))
    effective_start_date = (event_dt - timedelta(days=180)).strftime("%Y-%m-%d")
    effective_end_date = effective_end_dt.strftime("%Y-%m-%d")

    price_data = fetch_stock_data(
        symbol=stock,
        start_date=effective_start_date,
        end_date=effective_end_date,
    )
    result = run_event_study(
        stock_returns=price_data["stock_returns"],
        market_returns=price_data["market_returns"],
        dates=price_data["dates"],
        event_dates=[event_date],
        reaction_shift_trading_days=reaction_shift_trading_days,
    )
    full_window_car = result["avg_car"][-1] if result.get("avg_car") else 0.0
    reaction_dates = result.get("reaction_dates_used", [])
    reaction_date = reaction_dates[0] if reaction_dates else ""
    summary = (
        f"事件研究完成，單一事件樣本 {result.get('n_events', 0)} 筆，"
        f"以市場反應日 {reaction_date or event_date} 作為 t=0，[-5,+5] CAR {full_window_car:.4f}"
    )
    data_gaps = []
    if result.get("error"):
        data_gaps.append(result["error"])
    if result.get("skipped_events"):
        data_gaps.append("some_events_skipped")

    return {
        **result,
        "event_date": event_date,
        "reaction_date": reaction_date or event_date,
        "summary": summary,
        "n_skipped": len(result.get("skipped_events", [])),
        "data_window": {
            "start": effective_start_date,
            "end": effective_end_date,
        },
        "data_gaps": data_gaps,
    }


def _collect_phase_partitioned_event_records(
    *,
    stock: str,
    event_type: str,
    start_date: str,
    end_date: str,
    event_date: str,
    stock_name: str = "",
    event_key: str = "",
    max_results: int = 24,
    primary_source: str = "cnyes",
    allow_secondary_sources: bool = True,
) -> dict[str, Any]:
    """Collect pre-event, event-day, and post-event windows separately, then merge them."""
    if event_type != "法說會":
        return collect_event_records(
            symbol=stock,
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            stock_name=stock_name,
            event_date=event_date,
            event_key=event_key,
            max_results=max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
        )

    windows = _build_phase_windows(event_date=event_date, request_start=start_date, request_end=end_date)
    phase_payloads: dict[str, dict[str, Any]] = {}
    for phase_name, window in windows.items():
        if not window["start"] or not window["end"]:
            phase_payloads[phase_name] = _build_empty_phase_payload(
                stock=stock,
                stock_name=stock_name,
                event_type=event_type,
                event_date=event_date,
                event_key=event_key,
                start_date=window["start"],
                end_date=window["end"],
            )
            continue
        phase_payloads[phase_name] = collect_event_records(
            symbol=stock,
            event_type=event_type,
            start_date=window["start"],
            end_date=window["end"],
            stock_name=stock_name,
            event_date=event_date,
            event_key=event_key,
            max_results=max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
        )

    return _merge_phase_event_collections(
        phase_payloads=phase_payloads,
        start_date=start_date,
        end_date=end_date,
        event_date=event_date,
    )


def _build_phase_windows(*, event_date: str, request_start: str, request_end: str) -> dict[str, dict[str, str]]:
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    request_start_dt = datetime.strptime(request_start, "%Y-%m-%d")
    request_end_dt = datetime.strptime(request_end, "%Y-%m-%d")

    pre_start_dt = max(request_start_dt, event_dt - timedelta(days=7))
    pre_end_dt = min(request_end_dt, event_dt - timedelta(days=1))
    event_day_dt = event_dt
    post_start_dt = max(request_start_dt, event_dt + timedelta(days=1))
    post_end_dt = min(request_end_dt, event_dt + timedelta(days=7))

    return {
        "pre_event": {
            "start": pre_start_dt.strftime("%Y-%m-%d"),
            "end": pre_end_dt.strftime("%Y-%m-%d") if pre_start_dt <= pre_end_dt else "",
        },
        "event_day": {
            "start": event_day_dt.strftime("%Y-%m-%d") if request_start_dt <= event_day_dt <= request_end_dt else "",
            "end": event_day_dt.strftime("%Y-%m-%d") if request_start_dt <= event_day_dt <= request_end_dt else "",
        },
        "post_event": {
            "start": post_start_dt.strftime("%Y-%m-%d"),
            "end": post_end_dt.strftime("%Y-%m-%d") if post_start_dt <= post_end_dt else "",
        },
    }


def _merge_phase_event_collections(
    *,
    phase_payloads: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    event_date: str,
) -> dict[str, Any]:
    base_payload = next((payload for payload in phase_payloads.values() if payload), {})
    merged_records: list[dict[str, Any]] = []
    for phase_name, payload in phase_payloads.items():
        for record in payload.get("records", []):
            if not isinstance(record, dict):
                continue
            record_copy = dict(record)
            record_copy["source_window"] = phase_name
            merged_records.append(record_copy)
    merged_records = dedupe_records(
        merged_records,
        ["stock_code", "event_date", "article_date", "headline", "source_url"],
    )
    merged_records.sort(key=lambda item: (item.get("article_date", ""), item.get("headline", "")))

    official_artifacts = _dedupe_dict_rows(
        [
            artifact
            for payload in phase_payloads.values()
            for artifact in payload.get("official_artifacts", [])
            if isinstance(artifact, dict)
        ],
        keys=("artifact_type", "url", "source_name"),
    )
    todo_items = _dedupe_dict_rows(
        [
            todo
            for payload in phase_payloads.values()
            for todo in payload.get("todo_items", [])
            if isinstance(todo, dict)
        ],
        keys=("id",),
    )
    data_gaps = _dedupe_strings(
        [
            str(gap)
            for payload in phase_payloads.values()
            for gap in payload.get("data_gaps", [])
            if str(gap).strip()
        ]
    )
    earnings_digest = _pick_earnings_digest(phase_payloads)
    record_breakdown = _build_merged_record_breakdown(merged_records)
    phase_record_counts = {
        phase_name: payload.get("record_count", 0)
        for phase_name, payload in phase_payloads.items()
    }

    query = dict(base_payload.get("query", {}))
    query["time_range"] = {
        "start": start_date,
        "end": end_date,
        "effective_start": start_date,
        "effective_end": end_date,
    }
    query["event_date"] = event_date

    collection_plan = dict(base_payload.get("collection_plan", {}))
    collection_plan["phase_windows"] = {
        phase_name: (payload.get("query", {}).get("time_range", {}) if isinstance(payload, dict) else {})
        for phase_name, payload in phase_payloads.items()
    }
    collection_plan["phase_record_counts"] = phase_record_counts
    collection_plan["mode"] = "phase_partitioned"

    data_completeness = dict(base_payload.get("data_completeness", {}))
    data_completeness["data_gaps"] = data_gaps
    data_completeness["notes"] = (
        "Phase-partitioned event collection: pre_event, event_day, and post_event "
        "were collected separately and merged for reporting."
    )

    return {
        **base_payload,
        "query": query,
        "collection_plan": collection_plan,
        "data_completeness": data_completeness,
        "data_gaps": data_gaps,
        "record_count": len(merged_records),
        "record_breakdown": record_breakdown,
        "official_artifacts": official_artifacts,
        "earnings_digest": earnings_digest,
        "todo_items": todo_items,
        "records": merged_records,
        "phase_collections": phase_payloads,
    }


def _build_merged_record_breakdown(records: list[dict[str, Any]]) -> dict[str, int]:
    archive_records = 0
    secondary_records = 0
    live_records = 0
    for record in records:
        if str(record.get("source_type", "")) != "media":
            continue
        if record.get("is_primary_source"):
            archive_records += 1
        elif record.get("retrieval_method") in {"google_news_rss", "goodinfo_http_index", "goodinfo_browser_index", "yfinance_get_news"}:
            secondary_records += 1
        else:
            live_records += 1
    return {
        "archive_records": archive_records,
        "secondary_source_records": secondary_records,
        "live_fetched_records": live_records,
    }


def _pick_earnings_digest(phase_payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for phase_name in ("event_day", "post_event", "pre_event"):
        payload = phase_payloads.get(phase_name, {})
        digest = payload.get("earnings_digest", {})
        if isinstance(digest, dict) and digest:
            return dict(digest)
    return {}


def _dedupe_dict_rows(rows: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row.get(item) for item in keys)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _dedupe_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _build_empty_phase_payload(
    *,
    stock: str,
    stock_name: str,
    event_type: str,
    event_date: str,
    event_key: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    return {
        "query": {
            "event_type": event_type,
            "time_range": {
                "start": start_date,
                "end": end_date,
                "effective_start": start_date,
                "effective_end": end_date,
            },
            "stock": {
                "symbol": stock,
                "code": stock.split(".", 1)[0],
                "name": stock_name,
            },
            "event_date": event_date,
            "event_key": event_key,
        },
        "collection_plan": {
            "sources": [],
            "mode": "phase_partitioned",
        },
        "data_completeness": {
            "mode": "phase_partitioned",
            "official_sources_included": False,
            "heat_analysis_included": False,
            "comparison_strategy": "",
            "comparison_ready": False,
            "data_gaps": [],
            "notes": "phase window outside requested range",
        },
        "data_gaps": [],
        "record_count": 0,
        "record_breakdown": {
            "archive_records": 0,
            "secondary_source_records": 0,
            "live_fetched_records": 0,
        },
        "official_artifacts": [],
        "earnings_digest": {},
        "todo_items": [],
        "records": [],
    }
