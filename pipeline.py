"""Deterministic event pipeline entry points.

These functions run structured, step-by-step workflows without going through
the LLM tool-use loop. Each function calls the relevant tool modules directly
and produces JSON / Markdown outputs.

LLM-driven modes (event_study, news_scan) remain in agent.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from tools.event_collector import collect_event_records
from tools.event_study import run_event_study
from tools.expectation_analysis import analyze_expectation_vs_actual
from tools.heat_analysis import scan_event_heat
from tools.report import build_event_report_payload, save_event_record, save_report
from tools.schemas import build_comparison_strategy
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

    payload = collect_event_records(
        symbol=stock,
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        stock_name=stock_name,
        event_date=event_date,
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
    print()

    payload = scan_event_heat(
        symbol=stock,
        event_type=event_type,
        event_date=event_date,
        stock_name=stock_name,
        event_key=event_key,
        comparison_event_date=comparison_event_date,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
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

    event_collection = collect_event_records(
        symbol=stock,
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        stock_name=stock_name,
        event_date=event_date,
        event_key=event_key,
        max_results=max_results,
        pre_event_report_days=7 if event_type == "法說會" else None,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
    )
    heat_payload = scan_event_heat(
        symbol=stock,
        event_type=event_type,
        event_date=event_date,
        stock_name=stock_name,
        event_key=event_key,
        comparison_event_date=comparison_event_date,
        max_results=max_results,
        primary_source="goodinfo" if event_type == "法說會" else "cnyes",
        allow_secondary_sources=False if event_type == "法說會" else True,
    )
    expectation_payload = _build_expectation_payload(
        records=event_collection["records"],
        event_type=event_type,
        event_key=event_collection["query"].get("event_key", ""),
    )
    event_study_payload = (
        _build_event_study_payload(
            stock=stock,
            event_date=event_date,
            end_date=end_date,
            reaction_shift_trading_days=1 if event_type == "法說會" else 0,
        )
        if include_event_study
        else None
    )

    report_payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=heat_payload,
        expectation_analysis=expectation_payload,
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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_expectation_payload(records: list[dict[str, Any]], event_type: str, event_key: str) -> dict[str, Any]:
    """Run expectation analysis when the event supports it; otherwise return structured gaps."""
    strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    if strategy["comparison_mode"] != "same_event_last_year":
        return {
            "analysis_target": {
                "event_type": event_type,
                "event_key": event_key,
            },
            "comparison_mode": "expectation_vs_actual",
            "metrics": [],
            "status_counts": {},
            "data_gaps": ["expectation_analysis_only_supported_for_recurring_events"],
        }
    if not event_key:
        return {
            "analysis_target": {
                "event_type": event_type,
                "event_key": event_key,
            },
            "comparison_mode": "expectation_vs_actual",
            "metrics": [],
            "status_counts": {},
            "data_gaps": ["event_key_missing_for_expectation_analysis"],
        }
    return analyze_expectation_vs_actual(records=records, event_key=event_key, event_type=event_type)


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
