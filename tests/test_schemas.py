"""Unit tests for event collection schemas and normalization helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.schemas import (
    build_comparison_strategy,
    build_stock_target,
    classify_event_phase,
    compact_text,
    dedupe_records,
    infer_record_flags,
    normalize_event_key,
    normalize_symbol,
    previous_year_event_key,
)


def test_normalize_symbol_appends_tw_suffix_for_numeric_codes():
    assert normalize_symbol("2330") == "2330.TW"


def test_build_stock_target_fills_code_from_symbol():
    target = build_stock_target(symbol="2330", name="台積電")
    assert target == {
        "symbol": "2330.TW",
        "code": "2330",
        "name": "台積電",
    }


def test_compact_text_collapses_whitespace_and_trims():
    text = "台積電   法說會\n\nAI 需求   強勁"
    assert compact_text(text, max_length=20) == "台積電 法說會 AI 需求 強勁"


def test_dedupe_records_preserves_first_seen_order():
    records = [
        {"headline": "A", "date": "2025-04-10"},
        {"headline": "A", "date": "2025-04-10"},
        {"headline": "B", "date": "2025-04-11"},
    ]
    assert dedupe_records(records, ["headline", "date"]) == [
        {"headline": "A", "date": "2025-04-10"},
        {"headline": "B", "date": "2025-04-11"},
    ]


def test_normalize_event_key_standardizes_quarter_format():
    assert normalize_event_key("法說會", "2025 q4") == "2025Q4"


def test_previous_year_event_key_keeps_same_quarter():
    assert previous_year_event_key("法說會", "2025Q4") == "2024Q4"


def test_build_comparison_strategy_for_recurring_event_requires_event_key():
    strategy = build_comparison_strategy("法說會")
    assert strategy["comparison_mode"] == "same_event_last_year"
    assert strategy["comparison_ready"] is False
    assert strategy["data_gaps"] == ["event_key_missing_for_same_event_comparison"]


def test_build_comparison_strategy_for_one_off_event_uses_recent_baseline():
    strategy = build_comparison_strategy("重大消息")
    assert strategy["comparison_mode"] == "recent_baseline"
    assert strategy["comparison_ready"] is True
    assert strategy["comparison_event_key"] == ""


def test_classify_event_phase_assigns_pre_event_and_event_day_and_post_event():
    assert classify_event_phase("2025-04-15", "2025-04-17") == "pre_event"
    assert classify_event_phase("2025-04-17", "2025-04-17") == "event_day"
    assert classify_event_phase("2025-04-18", "2025-04-17") == "post_event"


def test_infer_record_flags_marks_expectations_and_actuals():
    expectation_flags = infer_record_flags(
        event_phase="pre_event",
        article_type="法說前預期",
        source_type="media",
    )
    actual_flags = infer_record_flags(
        event_phase="event_day",
        article_type="媒體報導",
        source_type="official",
    )

    assert expectation_flags == {
        "source_kind": "media",
        "is_expectation": True,
        "is_actual": False,
        "expectation_match": "",
    }
    assert actual_flags == {
        "source_kind": "official",
        "is_expectation": False,
        "is_actual": True,
        "expectation_match": "",
    }
