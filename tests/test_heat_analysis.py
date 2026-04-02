"""Unit tests for heat analysis strategy selection and ratio calculation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.heat_analysis import analyze_news_heat, scan_event_heat


def test_analyze_news_heat_uses_same_event_last_year_for_earnings_calls():
    result = analyze_news_heat(
        analysis_target="2330 台積電",
        event_type="法說會",
        event_date="2026-01-16",
        event_key="2025Q4",
        current_window_total=18,
        comparison_event_total=9,
    )

    assert result["comparison_mode"] == "same_event_last_year"
    assert result["comparison_event_key"] == "2024Q4"
    assert result["news_heat_ratio"] == 2.0
    assert result["news_heat_label"] == "高"


def test_analyze_news_heat_uses_recent_baseline_for_one_off_event():
    result = analyze_news_heat(
        analysis_target="2330 台積電",
        event_type="重大消息",
        event_date="2026-01-16",
        current_window_total=12,
        baseline_window_total=24,
    )

    assert result["comparison_mode"] == "recent_baseline"
    assert result["comparison_value"] == 6.0
    assert result["news_heat_ratio"] == 2.0
    assert result["news_heat_label"] == "高"


def test_analyze_news_heat_flags_missing_same_event_total():
    result = analyze_news_heat(
        analysis_target="2330 台積電",
        event_type="法說會",
        event_date="2026-01-16",
        event_key="2025Q4",
        current_window_total=18,
    )

    assert result["news_heat_ratio"] is None
    assert "comparison_event_total_missing" in result["data_gaps"]
    assert result["news_heat_label"] == "資料不足"


def test_scan_event_heat_uses_previous_year_event_key_for_recurring_events(monkeypatch):
    calls = []

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        if kwargs["event_key"] == "2025Q4":
            return {"record_count": 8, "records": [{"headline": "current"}]}
        return {"record_count": 4, "records": [{"headline": "previous"}]}

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        event_date="2026-01-16",
        event_key="2025Q4",
    )

    assert len(calls) == 2
    assert calls[1]["event_key"] == "2024Q4"
    assert result["comparison_mode"] == "same_event_last_year"
    assert result["current_record_count"] == 8
    assert result["comparison_record_count"] == 4
    assert result["news_heat_ratio"] == 2.0


def test_scan_event_heat_uses_baseline_window_for_one_off_events(monkeypatch):
    calls = []

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        if kwargs["start_date"] == "2026-01-09":
            return {"record_count": 12, "records": [{"headline": "current"}]}
        return {"record_count": 24, "records": [{"headline": "baseline"}]}

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="重大消息",
        event_date="2026-01-16",
    )

    assert len(calls) == 2
    assert calls[1]["start_date"] == "2025-12-10"
    assert calls[1]["end_date"] == "2026-01-08"
    assert result["comparison_mode"] == "recent_baseline"
    assert result["comparison_record_count"] == 24
    assert result["news_heat_ratio"] == 2.0
