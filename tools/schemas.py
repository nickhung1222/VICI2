"""Canonical schemas and normalization helpers for event collection."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from typing import Any

_RECURRING_EVENT_TYPES = {"法說會"}
_QUARTER_KEY_PATTERN = re.compile(r"^\s*(\d{4})\s*[-_/ ]?\s*[Qq]([1-4])\s*$")


def normalize_symbol(symbol: str) -> str:
    """Normalize a Taiwan stock symbol to Yahoo Finance style when possible."""
    normalized = symbol.strip().upper()
    if normalized.isdigit():
        return f"{normalized}.TW"
    return normalized


def stock_code_from_symbol(symbol: str) -> str:
    """Extract the numeric Taiwan stock code from a normalized symbol."""
    base = normalize_symbol(symbol).split(".", 1)[0]
    return base if base.isdigit() else ""


def build_stock_target(symbol: str, code: str = "", name: str = "") -> dict[str, str]:
    """Build the canonical stock target object."""
    normalized_symbol = normalize_symbol(symbol)
    normalized_code = (code or stock_code_from_symbol(normalized_symbol)).strip()
    return {
        "symbol": normalized_symbol,
        "code": normalized_code,
        "name": name.strip(),
    }


def is_recurring_event(event_type: str) -> bool:
    """Return whether the event type is expected to recur across comparable periods."""
    return event_type.strip() in _RECURRING_EVENT_TYPES


def normalize_event_key(event_type: str, event_key: str) -> str:
    """Normalize recurring-event identifiers such as quarterly earnings labels."""
    cleaned = " ".join(event_key.split())
    if not cleaned:
        return ""
    if not is_recurring_event(event_type):
        return cleaned

    match = _QUARTER_KEY_PATTERN.match(cleaned)
    if not match:
        raise ValueError(f"Invalid recurring event key for {event_type}: {event_key!r}")
    year, quarter = match.groups()
    return f"{year}Q{quarter}"


def previous_year_event_key(event_type: str, event_key: str) -> str:
    """Return the same recurring event key for the prior year when applicable."""
    normalized_key = normalize_event_key(event_type, event_key)
    if not normalized_key or not is_recurring_event(event_type):
        return ""

    year = int(normalized_key[:4]) - 1
    return f"{year}{normalized_key[4:]}"


def build_comparison_strategy(event_type: str, event_key: str = "") -> dict[str, Any]:
    """Build comparison metadata for heat analysis and downstream reporting."""
    recurring = is_recurring_event(event_type)
    normalized_event_key = normalize_event_key(event_type, event_key) if event_key else ""
    data_gaps: list[str] = []

    if recurring and not normalized_event_key:
        data_gaps.append("event_key_missing_for_same_event_comparison")

    comparison_event_key = (
        previous_year_event_key(event_type, normalized_event_key)
        if recurring and normalized_event_key
        else ""
    )

    return {
        "is_recurring_event": recurring,
        "comparison_mode": "same_event_last_year" if recurring else "recent_baseline",
        "event_key": normalized_event_key,
        "comparison_event_key": comparison_event_key,
        "comparison_ready": (not recurring) or bool(normalized_event_key),
        "data_gaps": data_gaps,
    }


def classify_event_phase(article_date: str, event_date: str) -> str:
    """Classify a record as pre-event, event-day, or post-event."""
    if not article_date or not event_date:
        return ""

    try:
        article_dt = datetime.strptime(article_date, "%Y-%m-%d")
        event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    except ValueError:
        return ""

    if article_dt < event_dt:
        return "pre_event"
    if article_dt == event_dt:
        return "event_day"
    return "post_event"


def infer_record_flags(
    *,
    event_phase: str,
    article_type: str,
    source_type: str,
) -> dict[str, Any]:
    """Infer shared structured flags for downstream analysis."""
    is_expectation = event_phase == "pre_event" and article_type in {"法說前預期", "分析師觀點"}
    is_actual = event_phase == "event_day"

    return {
        "source_kind": source_type or "media",
        "is_expectation": is_expectation,
        "is_actual": is_actual,
        "expectation_match": "",
    }


def compact_text(text: str, max_length: int = 200) -> str:
    """Collapse whitespace and trim text for structured summaries."""
    compacted = " ".join(str(text).split())
    if len(compacted) <= max_length:
        return compacted
    return compacted[: max_length - 1].rstrip() + "…"


def dedupe_strings(items: Iterable[str]) -> list[str]:
    """Deduplicate a sequence of strings while preserving order."""
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def dedupe_records(records: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    """Deduplicate records by a stable tuple of key fields while preserving order."""
    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []

    for record in records:
        key = tuple(record.get(field) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)

    return deduped
