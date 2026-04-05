"""Heat analysis helpers and scan flow for recurring and one-off stock events."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from tools.event_collector import collect_event_records
from tools.event_sources import resolve_earnings_event_date
from tools.schemas import build_comparison_strategy, build_stock_target

_HEAT_SCAN_MIN_RESULTS = 200
_VALID_HEAT_PHASES = {"pre_event", "post_event", "both"}


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
    phase: str = "both",
    current_pre_event_payload: dict[str, Any] | None = None,
    current_post_event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect event records and compute structured heat analysis."""
    normalized_phase = _normalize_phase(phase)
    target = build_stock_target(symbol=symbol, code=stock_code, name=stock_name)
    strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    resolved_event_date = _resolve_heat_event_date(
        target=target,
        event_type=event_type,
        event_date=event_date,
        event_key=strategy["event_key"],
    )
    effective_max_results = max(max_results, _HEAT_SCAN_MIN_RESULTS)

    comparison_date = ""
    comparison_window: dict[str, str] = {}
    comparison_post_event_window: dict[str, str] = {}
    comparison_records: dict[str, Any] | None = None
    comparison_post_event_records: dict[str, Any] | None = None
    baseline_records: dict[str, Any] | None = None
    baseline_post_event_records: dict[str, Any] | None = None

    current_window = derive_pre_event_window(resolved_event_date)
    current_records = (
        current_pre_event_payload
        if normalized_phase in {"pre_event", "both"} and current_pre_event_payload is not None
        else _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=current_window["start"],
            end_date=current_window["end"],
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=normalized_phase in {"pre_event", "both"},
        )
    )
    current_breakdown = _normalize_record_breakdown(
        current_records,
        fallback_total=(current_records or {}).get("record_count"),
    )

    current_post_event_window = derive_post_event_window(resolved_event_date)
    current_post_event_records = (
        current_post_event_payload
        if normalized_phase in {"post_event", "both"} and current_post_event_payload is not None
        else _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=current_post_event_window["start"],
            end_date=current_post_event_window["end"],
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=normalized_phase in {"post_event", "both"},
        )
    )

    if strategy["comparison_mode"] == "same_event_last_year":
        comparison_date = comparison_event_date or shift_date_by_year(resolved_event_date, years=-1)
        if comparison_date:
            comparison_window = derive_pre_event_window(comparison_date)
            comparison_post_event_window = derive_post_event_window(comparison_date)
        comparison_records = _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=comparison_window.get("start", ""),
            end_date=comparison_window.get("end", ""),
            event_date=comparison_date,
            event_key=strategy["comparison_event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=(
                normalized_phase in {"pre_event", "both"}
                and bool(comparison_date and strategy["comparison_event_key"])
            ),
        )
        comparison_post_event_records = _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=comparison_post_event_window.get("start", ""),
            end_date=comparison_post_event_window.get("end", ""),
            event_date=comparison_date,
            event_key=strategy["comparison_event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=(
                normalized_phase in {"post_event", "both"}
                and bool(comparison_date and strategy["comparison_event_key"])
            ),
        )
        comparison_breakdown = _normalize_record_breakdown(
            comparison_records,
            fallback_total=(comparison_records or {}).get("record_count"),
        )
        comparison_anchor_date = comparison_date
        pre_event_comparison_value = (comparison_records or {}).get("record_count")
        pre_event_comparison_basis = "same_event_last_year"
        post_event_comparison_value = (comparison_post_event_records or {}).get("record_count")
        post_event_comparison_basis = "same_event_last_year"
    else:
        comparison_window = derive_baseline_window(resolved_event_date)
        comparison_post_event_window = derive_post_event_baseline_window(resolved_event_date)
        baseline_records = _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=comparison_window["start"],
            end_date=comparison_window["end"],
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=normalized_phase in {"pre_event", "both"},
        )
        baseline_post_event_records = _collect_window_records(
            target=target,
            event_type=event_type,
            start_date=comparison_post_event_window["start"],
            end_date=comparison_post_event_window["end"],
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            max_results=effective_max_results,
            primary_source=primary_source,
            allow_secondary_sources=allow_secondary_sources,
            enabled=normalized_phase in {"post_event", "both"},
        )
        comparison_breakdown = _normalize_record_breakdown(
            baseline_records,
            fallback_total=(baseline_records or {}).get("record_count"),
        )
        comparison_anchor_date = _derive_baseline_anchor_date(comparison_window)
        pre_event_comparison_value = _baseline_weekly_average((baseline_records or {}).get("record_count"))
        pre_event_comparison_basis = "recent_baseline"
        post_event_comparison_value = _baseline_weekly_average((baseline_post_event_records or {}).get("record_count"))
        post_event_comparison_basis = "post_event_baseline"

    pre_event_heat_scan = (
        analyze_news_heat(
            analysis_target=_format_analysis_target(target),
            event_type=event_type,
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            current_window_total=(current_records or {}).get("record_count", 0),
            baseline_window_total=(baseline_records or {}).get("record_count") if baseline_records else None,
            comparison_event_total=(comparison_records or {}).get("record_count") if comparison_records else None,
            current_records=(current_records or {}).get("records", []),
            comparison_records=(
                (comparison_records or {}).get("records", [])
                if strategy["comparison_mode"] == "same_event_last_year"
                else (baseline_records or {}).get("records", [])
            ),
            current_record_breakdown=current_breakdown,
            comparison_record_breakdown=comparison_breakdown,
            comparison_anchor_date=comparison_anchor_date,
        )
        if normalized_phase in {"pre_event", "both"}
        else None
    )
    if pre_event_heat_scan:
        pre_event_heat_scan.update(
            {
                "phase": "pre_event",
                "current_window": current_window,
                "current_record_count": (current_records or {}).get("record_count", 0),
                "current_records": (current_records or {}).get("records", []),
                "comparison_window": {
                    "start": comparison_window.get("start", ""),
                    "end": comparison_window.get("end", ""),
                    "event_date": comparison_date if strategy["comparison_mode"] == "same_event_last_year" else comparison_anchor_date,
                },
                "comparison_record_count": (
                    (comparison_records or {}).get("record_count")
                    if strategy["comparison_mode"] == "same_event_last_year"
                    else (baseline_records or {}).get("record_count")
                ),
                "comparison_records": (
                    (comparison_records or {}).get("records", [])
                    if strategy["comparison_mode"] == "same_event_last_year"
                    else (baseline_records or {}).get("records", [])
                ),
                "source_breakdown": {
                    "current_primary_only_count": current_breakdown["archive_records"],
                    "current_merged_count": (current_records or {}).get("record_count", 0),
                    "comparison_primary_only_count": comparison_breakdown["archive_records"],
                    "comparison_merged_count": (
                        (comparison_records or {}).get("record_count")
                        if strategy["comparison_mode"] == "same_event_last_year"
                        else (baseline_records or {}).get("record_count")
                    ),
                },
            }
        )

    post_event_heat_scan = (
        analyze_post_event_heat(
            analysis_target=_format_analysis_target(target),
            event_type=event_type,
            event_date=resolved_event_date,
            event_key=strategy["event_key"],
            comparison_mode=strategy["comparison_mode"],
            comparison_event_key=strategy["comparison_event_key"],
            comparison_ready=strategy["comparison_ready"],
            current_window_total=(current_post_event_records or {}).get("record_count", 0),
            comparison_value=post_event_comparison_value,
            comparison_basis=post_event_comparison_basis,
            strategy_data_gaps=list(strategy["data_gaps"]),
        )
        if normalized_phase in {"post_event", "both"}
        else None
    )
    if post_event_heat_scan:
        post_event_heat_scan.update(
            {
                "phase": "post_event",
                "current_window": current_post_event_window,
                "current_record_count": (current_post_event_records or {}).get("record_count", 0),
                "current_records": (current_post_event_records or {}).get("records", []),
                "comparison_window": {
                    "start": comparison_post_event_window.get("start", ""),
                    "end": comparison_post_event_window.get("end", ""),
                    "event_date": comparison_date if strategy["comparison_mode"] == "same_event_last_year" else "",
                },
                "comparison_record_count": (
                    (comparison_post_event_records or {}).get("record_count")
                    if strategy["comparison_mode"] == "same_event_last_year"
                    else (baseline_post_event_records or {}).get("record_count")
                ),
                "comparison_records": (
                    (comparison_post_event_records or {}).get("records", [])
                    if strategy["comparison_mode"] == "same_event_last_year"
                    else (baseline_post_event_records or {}).get("records", [])
                ),
            }
        )

    primary_scan = pre_event_heat_scan or post_event_heat_scan or {}
    result = {
        "analysis_target": _format_analysis_target(target),
        "event_type": event_type,
        "event_date": resolved_event_date,
        "heat_version": "v2",
        "requested_phase": normalized_phase,
        "comparison_mode": strategy["comparison_mode"],
        "event_key": strategy["event_key"],
        "comparison_event_key": strategy["comparison_event_key"],
        "comparison_ready": strategy["comparison_ready"],
        "stock": target,
        "pre_event_heat_scan": pre_event_heat_scan,
        "post_event_heat_scan": post_event_heat_scan,
        "available_heat_scans": [scan["phase"] for scan in (pre_event_heat_scan, post_event_heat_scan) if scan],
        "data_gaps": _merge_section_data_gaps(pre_event_heat_scan, post_event_heat_scan),
        # Legacy compatibility mirrors the primary requested scan.
        "comparison_basis": primary_scan.get("comparison_basis"),
        "current_window_total": primary_scan.get("current_window_total"),
        "comparison_value": primary_scan.get("comparison_value"),
        "news_heat_ratio": primary_scan.get("news_heat_ratio"),
        "news_heat_label": primary_scan.get("news_heat_label"),
        "panels": primary_scan.get("panels", []),
        "panel_interpretation": primary_scan.get("panel_interpretation", []),
        "current_window": primary_scan.get("current_window"),
        "current_records": primary_scan.get("current_records", []),
        "current_record_count": primary_scan.get("current_record_count"),
        "comparison_window": primary_scan.get("comparison_window"),
        "comparison_record_count": primary_scan.get("comparison_record_count"),
        "comparison_records": primary_scan.get("comparison_records", []),
    }
    if pre_event_heat_scan:
        result["source_breakdown"] = pre_event_heat_scan.get("source_breakdown", {})
    return result


def analyze_news_heat(
    analysis_target: str,
    event_type: str,
    event_date: str = "",
    event_key: str = "",
    current_window_total: int = 0,
    baseline_window_total: int | None = None,
    comparison_event_total: int | None = None,
    current_records: list[dict[str, Any]] | None = None,
    comparison_records: list[dict[str, Any]] | None = None,
    current_record_breakdown: dict[str, int] | None = None,
    comparison_record_breakdown: dict[str, int] | None = None,
    comparison_anchor_date: str = "",
) -> dict[str, Any]:
    """Analyze pre-event news heat using the configured comparison strategy."""
    strategy = build_comparison_strategy(event_type=event_type, event_key=event_key)
    data_gaps = list(strategy["data_gaps"])
    current_records = current_records or []
    comparison_records = comparison_records or []
    current_record_breakdown = current_record_breakdown or _normalize_record_breakdown(
        None,
        fallback_total=current_window_total,
    )
    comparison_record_breakdown = comparison_record_breakdown or _normalize_record_breakdown(
        None,
        fallback_total=comparison_event_total if strategy["comparison_mode"] == "same_event_last_year" else baseline_window_total,
    )

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

    panels = _build_pre_event_heat_panels(
        current_records=current_records,
        comparison_records=comparison_records,
        event_date=event_date,
        comparison_anchor_date=comparison_anchor_date,
        current_window_total=current_window_total,
        comparison_value=comparison_value,
        current_record_breakdown=current_record_breakdown,
        comparison_record_breakdown=comparison_record_breakdown,
    )
    panel_interpretation = _build_pre_event_panel_interpretation(panels)

    return {
        "analysis_target": analysis_target,
        "event_type": event_type,
        "event_date": event_date,
        "heat_version": "v2",
        "comparison_mode": strategy["comparison_mode"],
        "event_key": strategy["event_key"],
        "comparison_event_key": strategy["comparison_event_key"],
        "comparison_ready": strategy["comparison_ready"],
        "comparison_basis": comparison_basis,
        "current_window_total": current_window_total,
        "comparison_value": comparison_value,
        "news_heat_ratio": ratio,
        "news_heat_label": classify_heat_ratio(ratio),
        "panels": panels,
        "panel_interpretation": panel_interpretation,
        "data_gaps": data_gaps,
    }


def analyze_post_event_heat(
    *,
    analysis_target: str,
    event_type: str,
    event_date: str,
    event_key: str,
    comparison_mode: str,
    comparison_event_key: str,
    comparison_ready: bool,
    current_window_total: int,
    comparison_value: int | float | None,
    comparison_basis: str,
    strategy_data_gaps: list[str] | None = None,
) -> dict[str, Any]:
    """Analyze post-event heat as a dedicated scan."""
    ratio = _safe_ratio(current_window_total, comparison_value)
    data_gaps = list(strategy_data_gaps or [])
    if comparison_value is None:
        data_gaps.append("post_event_comparison_total_missing")
    panel = _build_coverage_panel(
        panel_id="coverage_panel",
        label="Coverage",
        summary_subject="事件後覆蓋量",
        unavailable_gap="coverage_comparison_unavailable",
        current_window_total=current_window_total,
        comparison_value=comparison_value,
    )
    return {
        "analysis_target": analysis_target,
        "event_type": event_type,
        "event_date": event_date,
        "heat_version": "v2",
        "comparison_mode": comparison_mode,
        "event_key": event_key,
        "comparison_event_key": comparison_event_key,
        "comparison_ready": comparison_ready,
        "comparison_basis": comparison_basis,
        "current_window_total": current_window_total,
        "comparison_value": comparison_value,
        "news_heat_ratio": ratio,
        "news_heat_label": classify_heat_ratio(ratio),
        "panels": [panel],
        "panel_interpretation": _build_post_event_panel_interpretation([panel]),
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


def derive_post_event_window(event_date: str) -> dict[str, str]:
    """Return the post-event scan window D+1 to D+7."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    return {
        "start": (event_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        "end": (event_dt + timedelta(days=7)).strftime("%Y-%m-%d"),
    }


def derive_post_event_baseline_window(event_date: str) -> dict[str, str]:
    """Return the post-event baseline window D+8 to D+14."""
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    return {
        "start": (event_dt + timedelta(days=8)).strftime("%Y-%m-%d"),
        "end": (event_dt + timedelta(days=14)).strftime("%Y-%m-%d"),
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


def _normalize_phase(phase: str) -> str:
    value = str(phase or "both").strip().lower()
    if value not in _VALID_HEAT_PHASES:
        raise ValueError(f"Unsupported heat phase: {phase}")
    return value


def _collect_window_records(
    *,
    target: dict[str, str],
    event_type: str,
    start_date: str,
    end_date: str,
    event_date: str,
    event_key: str,
    max_results: int,
    primary_source: str,
    allow_secondary_sources: bool,
    enabled: bool,
) -> dict[str, Any] | None:
    if not enabled or not start_date or not end_date:
        return None
    return collect_event_records(
        symbol=target["symbol"],
        stock_code=target["code"],
        stock_name=target["name"],
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        event_date=event_date,
        event_key=event_key,
        max_results=max_results,
        primary_source=primary_source,
        allow_secondary_sources=allow_secondary_sources,
    )


def _resolve_heat_event_date(
    *,
    target: dict[str, str],
    event_type: str,
    event_date: str,
    event_key: str,
) -> str:
    if event_type != "法說會" or not target.get("code"):
        return event_date
    resolution = resolve_earnings_event_date(
        stock_code=target["code"],
        stock_name=target["name"],
        symbol=target["symbol"],
        event_date=event_date,
        event_key=event_key,
    )
    return resolution.get("resolved_event_date", event_date) or event_date


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


def _build_pre_event_heat_panels(
    *,
    current_records: list[dict[str, Any]],
    comparison_records: list[dict[str, Any]],
    event_date: str,
    comparison_anchor_date: str,
    current_window_total: int,
    comparison_value: int | float | None,
    current_record_breakdown: dict[str, int],
    comparison_record_breakdown: dict[str, int],
) -> list[dict[str, Any]]:
    coverage_panel = _build_coverage_panel(
        panel_id="coverage_panel",
        label="Coverage",
        summary_subject="事件前覆蓋量",
        unavailable_gap="coverage_comparison_unavailable",
        current_window_total=current_window_total,
        comparison_value=comparison_value,
    )
    recency_panel = _build_recency_panel(
        current_records=current_records,
        comparison_records=comparison_records,
        event_date=event_date,
        comparison_anchor_date=comparison_anchor_date,
    )
    source_mix_panel = _build_source_mix_panel(
        current_record_breakdown=current_record_breakdown,
        comparison_record_breakdown=comparison_record_breakdown,
    )
    return [coverage_panel, recency_panel, source_mix_panel]


def _build_coverage_panel(
    *,
    panel_id: str,
    label: str,
    summary_subject: str,
    unavailable_gap: str,
    current_window_total: int | float,
    comparison_value: int | float | None,
) -> dict[str, Any]:
    ratio = _safe_ratio(current_window_total, comparison_value)
    absolute_delta = None if comparison_value is None else round(float(current_window_total) - float(comparison_value), 4)
    status = "insufficient_data"
    if ratio is not None:
        if ratio > 2.0:
            status = "surging"
        elif ratio >= 1.25:
            status = "elevated"
        elif ratio >= 0.8:
            status = "steady"
        else:
            status = "subdued"
    summary = (
        f"資料不足，無法比較{summary_subject}。"
        if ratio is None
        else f"{summary_subject}為 {current_window_total}，相較對照值 {comparison_value}，比值 {ratio}。"
    )
    return {
        "panel_id": panel_id,
        "label": label,
        "current_value": current_window_total,
        "comparison_value": comparison_value,
        "delta": {
            "absolute": absolute_delta,
            "ratio": ratio,
        },
        "status": status,
        "summary": summary,
        "data_gaps": [] if ratio is not None else [unavailable_gap],
    }


def _build_recency_panel(
    *,
    current_records: list[dict[str, Any]],
    comparison_records: list[dict[str, Any]],
    event_date: str,
    comparison_anchor_date: str,
) -> dict[str, Any]:
    current_value = _calculate_recency_index(current_records, event_date)
    comparison_value = _calculate_recency_index(comparison_records, comparison_anchor_date)
    delta = None
    status = "insufficient_data"
    if current_value is not None and comparison_value is not None:
        delta = round(current_value - comparison_value, 4)
        if delta >= 0.75:
            status = "late_build"
        elif delta <= -0.75:
            status = "front_loaded"
        else:
            status = "steady"
    summary = (
        "資料不足，無法比較事件前新聞時點集中度。"
        if delta is None
        else f"事件前新聞集中度指數為 {current_value}，對照值為 {comparison_value}，差值 {delta}。"
    )
    return {
        "panel_id": "recency_panel",
        "label": "Recency",
        "current_value": current_value,
        "comparison_value": comparison_value,
        "delta": delta,
        "status": status,
        "summary": summary,
        "data_gaps": [] if delta is not None else ["recency_comparison_unavailable"],
    }


def _build_source_mix_panel(
    *,
    current_record_breakdown: dict[str, int],
    comparison_record_breakdown: dict[str, int],
) -> dict[str, Any]:
    current_mix = _calculate_source_mix(current_record_breakdown)
    comparison_mix = _calculate_source_mix(comparison_record_breakdown)
    current_primary_share = current_mix.get("primary_share")
    comparison_primary_share = comparison_mix.get("primary_share")
    primary_delta = None
    secondary_delta = None
    status = "insufficient_data"
    if current_primary_share is not None and comparison_primary_share is not None:
        primary_delta = round(current_primary_share - comparison_primary_share, 4)
        current_secondary_share = current_mix.get("secondary_share")
        comparison_secondary_share = comparison_mix.get("secondary_share")
        if current_secondary_share is not None and comparison_secondary_share is not None:
            secondary_delta = round(current_secondary_share - comparison_secondary_share, 4)
        if primary_delta >= 0.15:
            status = "primary_heavier"
        elif primary_delta <= -0.15:
            status = "secondary_heavier"
        else:
            status = "similar"
    summary = (
        "資料不足，無法比較來源結構。"
        if primary_delta is None
        else (
            "目前 primary share "
            f"{current_primary_share}，對照值 {comparison_primary_share}，差值 {primary_delta}。"
        )
    )
    return {
        "panel_id": "source_mix_panel",
        "label": "Source Mix",
        "current_value": current_mix,
        "comparison_value": comparison_mix,
        "delta": {
            "primary_share": primary_delta,
            "secondary_share": secondary_delta,
        },
        "status": status,
        "summary": summary,
        "data_gaps": [] if primary_delta is not None else ["source_mix_comparison_unavailable"],
    }


def _normalize_record_breakdown(
    payload: dict[str, Any] | None,
    fallback_total: int | float | None = None,
) -> dict[str, int]:
    record_breakdown = payload.get("record_breakdown", {}) if isinstance(payload, dict) else {}
    fallback = int(fallback_total or 0)
    archive_records = int(record_breakdown.get("archive_records", 0))
    secondary_records = int(record_breakdown.get("secondary_source_records", 0))
    live_records = int(record_breakdown.get("live_fetched_records", 0))
    merged_total = archive_records + secondary_records + live_records
    if merged_total <= 0 and fallback > 0:
        merged_total = fallback
        archive_records = fallback
    return {
        "archive_records": archive_records,
        "secondary_source_records": secondary_records,
        "live_fetched_records": live_records,
        "merged_count": merged_total,
    }


def _calculate_recency_index(records: list[dict[str, Any]], anchor_date: str) -> float | None:
    if not records or not anchor_date:
        return None
    try:
        anchor_dt = datetime.strptime(anchor_date, "%Y-%m-%d")
    except ValueError:
        return None

    weights: list[int] = []
    for record in records:
        article_date = str(record.get("article_date") or record.get("published_at") or "").strip()
        try:
            article_dt = datetime.strptime(article_date, "%Y-%m-%d")
        except ValueError:
            continue
        days_before = (anchor_dt - article_dt).days
        if days_before < 1:
            continue
        if days_before <= 7:
            weights.append(8 - days_before)
        else:
            weights.append(1)

    if not weights:
        return None
    return round(sum(weights) / len(weights), 4)


def _derive_baseline_anchor_date(window: dict[str, str]) -> str:
    end_date = str(window.get("end", "")).strip()
    if not end_date:
        return ""
    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return ""
    return (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")


def _calculate_source_mix(record_breakdown: dict[str, int]) -> dict[str, float | int | None]:
    merged_count = int(record_breakdown.get("merged_count", 0))
    archive_records = int(record_breakdown.get("archive_records", 0))
    secondary_records = int(record_breakdown.get("secondary_source_records", 0))
    live_records = int(record_breakdown.get("live_fetched_records", 0))
    if merged_count <= 0:
        return {
            "primary_share": None,
            "secondary_share": None,
            "live_share": None,
            "primary_count": archive_records,
            "secondary_count": secondary_records,
            "live_count": live_records,
            "merged_count": merged_count,
        }
    return {
        "primary_share": round(archive_records / merged_count, 4),
        "secondary_share": round(secondary_records / merged_count, 4),
        "live_share": round(live_records / merged_count, 4),
        "primary_count": archive_records,
        "secondary_count": secondary_records,
        "live_count": live_records,
        "merged_count": merged_count,
    }


def _build_pre_event_panel_interpretation(panels: list[dict[str, Any]]) -> list[str]:
    by_id = {
        panel.get("panel_id"): panel
        for panel in panels
        if isinstance(panel, dict) and panel.get("panel_id")
    }
    lines: list[str] = []

    coverage_panel = by_id.get("coverage_panel", {})
    coverage_status = coverage_panel.get("status")
    coverage_ratio = (coverage_panel.get("delta") or {}).get("ratio") if isinstance(coverage_panel.get("delta"), dict) else None
    if coverage_status in {"surging", "elevated"}:
        lines.append(f"事件前新聞覆蓋量明顯升高，coverage ratio 約為 {coverage_ratio}。")
    elif coverage_status == "subdued":
        lines.append(f"事件前新聞覆蓋量低於對照值，coverage ratio 約為 {coverage_ratio}。")
    elif coverage_status == "steady":
        lines.append("事件前新聞覆蓋量與對照值接近。")

    recency_panel = by_id.get("recency_panel", {})
    recency_status = recency_panel.get("status")
    if recency_status == "late_build":
        lines.append("熱度較集中在事件前最後幾天才拉高。")
    elif recency_status == "front_loaded":
        lines.append("熱度較早發酵，並未集中在事件前最後幾天。")
    elif recency_status == "steady":
        lines.append("事件前新聞時點集中度與對照期相近。")

    source_panel = by_id.get("source_mix_panel", {})
    source_status = source_panel.get("status")
    if source_status == "primary_heavier":
        lines.append("本次來源結構更偏向 primary / archive 路徑。")
    elif source_status == "secondary_heavier":
        lines.append("本次來源結構較偏向 secondary 或補充來源。")
    elif source_status == "similar":
        lines.append("本次來源結構與對照期相近。")

    return lines[:3]


def _build_post_event_panel_interpretation(panels: list[dict[str, Any]]) -> list[str]:
    panel = panels[0] if panels else {}
    status = panel.get("status")
    ratio = (panel.get("delta") or {}).get("ratio") if isinstance(panel.get("delta"), dict) else None
    if status in {"surging", "elevated"}:
        return [f"事件後 coverage 高於對照期，ratio 約為 {ratio}。"]
    if status == "subdued":
        return [f"事件後 coverage 低於對照期，ratio 約為 {ratio}。"]
    if status == "steady":
        return ["事件後 coverage 與對照期接近。"]
    return []


def _merge_section_data_gaps(*sections: dict[str, Any] | None) -> list[str]:
    merged: list[str] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        for gap in section.get("data_gaps", []):
            gap_value = str(gap).strip()
            if gap_value and gap_value not in merged:
                merged.append(gap_value)
    return merged


def _format_analysis_target(target: dict[str, str]) -> str:
    """Build a readable analysis target label."""
    parts = [target.get("code", "").strip(), target.get("name", "").strip()]
    return " ".join(part for part in parts if part).strip() or target.get("symbol", "").strip()
