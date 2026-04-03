"""Heat analysis helpers and scan flow for recurring and one-off stock events."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from tools.event_collector import collect_event_records
from tools.schemas import build_comparison_strategy, build_stock_target


def scan_event_heat(
    *,
    symbol: str,
    event_type: str,
    event_date: str,
    stock_name: str = "",
    stock_code: str = "",
    event_key: str = "",
    comparison_event_date: str = "",
    max_results: int = 24,
    primary_source: str = "cnyes",
    allow_secondary_sources: bool = True,
) -> dict[str, Any]:
    """Collect event records and compute structured heat analysis."""
    target = build_stock_target(symbol=symbol, code=stock_code, name=stock_name)
    strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    current_window = derive_pre_event_window(event_date)

    current_records = collect_event_records(
        symbol=target["symbol"],
        stock_code=target["code"],
        stock_name=target["name"],
        event_type=event_type,
        start_date=current_window["start"],
        end_date=current_window["end"],
        event_date=event_date,
        event_key=strategy["event_key"],
        max_results=max_results,
        primary_source=primary_source,
        allow_secondary_sources=allow_secondary_sources,
    )
    current_total = current_records["record_count"]
    current_primary_total = current_records.get("record_breakdown", {}).get("archive_records", current_total)

    if strategy["comparison_mode"] == "same_event_last_year":
        comparison_date = comparison_event_date or shift_date_by_year(event_date, years=-1)
        comparison_window = derive_pre_event_window(comparison_date) if comparison_date else {"start": "", "end": ""}
        comparison_records = (
            collect_event_records(
                symbol=target["symbol"],
                stock_code=target["code"],
                stock_name=target["name"],
                event_type=event_type,
                start_date=comparison_window["start"],
                end_date=comparison_window["end"],
                event_date=comparison_date,
                event_key=strategy["comparison_event_key"],
                max_results=max_results,
                primary_source=primary_source,
                allow_secondary_sources=allow_secondary_sources,
            )
            if comparison_date and strategy["comparison_event_key"]
            else None
        )
        result = analyze_news_heat(
            analysis_target=_format_analysis_target(target),
            event_type=event_type,
            event_date=event_date,
            event_key=strategy["event_key"],
            current_window_total=current_total,
            comparison_event_total=(comparison_records["record_count"] if comparison_records else None),
        )
        result["source_breakdown"] = {
            "current_primary_only_count": current_primary_total,
            "current_merged_count": current_total,
            "comparison_primary_only_count": (
                comparison_records.get("record_breakdown", {}).get("archive_records", comparison_records["record_count"])
                if comparison_records
                else None
            ),
            "comparison_merged_count": comparison_records["record_count"] if comparison_records else None,
        }
        result["comparison_window"] = {
            "start": comparison_window["start"],
            "end": comparison_window["end"],
            "event_date": comparison_date,
        }
        result["comparison_record_count"] = comparison_records["record_count"] if comparison_records else None
        result["comparison_records"] = comparison_records["records"] if comparison_records else []
    else:
        baseline_window = derive_baseline_window(event_date)
        baseline_records = collect_event_records(
            symbol=target["symbol"],
            stock_code=target["code"],
            stock_name=target["name"],
            event_type=event_type,
            start_date=baseline_window["start"],
            end_date=baseline_window["end"],
            event_date=event_date,
            event_key=strategy["event_key"],
            max_results=max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
        )
        result = analyze_news_heat(
            analysis_target=_format_analysis_target(target),
            event_type=event_type,
            event_date=event_date,
            event_key=strategy["event_key"],
            current_window_total=current_total,
            baseline_window_total=baseline_records["record_count"],
        )
        result["source_breakdown"] = {
            "current_primary_only_count": current_primary_total,
            "current_merged_count": current_total,
            "comparison_primary_only_count": baseline_records.get("record_breakdown", {}).get("archive_records", baseline_records["record_count"]),
            "comparison_merged_count": baseline_records["record_count"],
        }
        result["comparison_window"] = baseline_window
        result["comparison_record_count"] = baseline_records["record_count"]
        result["comparison_records"] = baseline_records["records"]

    result["current_window"] = current_window
    result["current_records"] = current_records["records"]
    result["current_record_count"] = current_total
    result["stock"] = target
    return result


def analyze_news_heat(
    analysis_target: str,
    event_type: str,
    event_date: str = "",
    event_key: str = "",
    current_window_total: int = 0,
    baseline_window_total: int | None = None,
    comparison_event_total: int | None = None,
) -> dict[str, Any]:
    """Analyze news heat using the configured comparison strategy."""
    strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    data_gaps = list(strategy["data_gaps"])

    if strategy["comparison_mode"] == "same_event_last_year":
        ratio = _safe_ratio(current_window_total, comparison_event_total)
        if comparison_event_total is None:
            data_gaps.append("comparison_event_total_missing")
        comparison_value = comparison_event_total
        comparison_basis = "same_event_last_year"
    else:
        ratio = _safe_ratio(current_window_total, _baseline_weekly_average(baseline_window_total))
        if baseline_window_total is None:
            data_gaps.append("baseline_window_total_missing")
        comparison_value = _baseline_weekly_average(baseline_window_total)
        comparison_basis = "recent_baseline"

    return {
        "analysis_target": analysis_target,
        "event_type": event_type,
        "event_date": event_date,
        "comparison_mode": strategy["comparison_mode"],
        "event_key": strategy["event_key"],
        "comparison_event_key": strategy["comparison_event_key"],
        "comparison_ready": strategy["comparison_ready"],
        "comparison_basis": comparison_basis,
        "current_window_total": current_window_total,
        "comparison_value": comparison_value,
        "news_heat_ratio": ratio,
        "news_heat_label": classify_heat_ratio(ratio),
        "data_gaps": data_gaps,
    }


def derive_pre_event_window(event_date: str) -> dict[str, str]:
    """Return the pre-event scan window D-7 to D-1."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    return {
        "start": (event_dt - timedelta(days=7)).strftime("%Y-%m-%d"),
        "end": (event_dt - timedelta(days=1)).strftime("%Y-%m-%d"),
    }


def derive_baseline_window(event_date: str) -> dict[str, str]:
    """Return the baseline window D-37 to D-8."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    return {
        "start": (event_dt - timedelta(days=37)).strftime("%Y-%m-%d"),
        "end": (event_dt - timedelta(days=8)).strftime("%Y-%m-%d"),
    }


def shift_date_by_year(date_str: str, *, years: int) -> str:
    """Shift a YYYY-MM-DD date by whole years, handling leap days conservatively."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    try:
        shifted = dt.replace(year=dt.year + years)
    except ValueError:
        shifted = dt.replace(month=2, day=28, year=dt.year + years)
    return shifted.strftime("%Y-%m-%d")


def classify_heat_ratio(ratio: float | None) -> str:
    """Convert a ratio into a stable heat label."""
    if ratio is None:
        return "資料不足"
    if ratio > 2.5:
        return "極高"
    if ratio >= 1.5:
        return "高"
    if ratio >= 0.8:
        return "正常"
    return "冷清"


def _baseline_weekly_average(total: int | None) -> float | None:
    """Convert a four-week baseline window total into a weekly average."""
    if total is None:
        return None
    if total <= 0:
        return 0.0
    return total / 4


def _safe_ratio(numerator: int | float, denominator: int | float | None) -> float | None:
    """Return a rounded ratio when both operands are usable."""
    if denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _format_analysis_target(target: dict[str, str]) -> str:
    """Build a readable analysis target label."""
    parts = [target.get("code", "").strip(), target.get("name", "").strip()]
    return " ".join(part for part in parts if part).strip() or target.get("symbol", "").strip()
