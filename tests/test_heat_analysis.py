"""Unit tests for heat analysis strategy selection and panel construction."""

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
        current_records=[
            {"article_date": "2026-01-15"},
            {"article_date": "2026-01-14"},
        ],
        comparison_records=[
            {"article_date": "2025-01-15"},
            {"article_date": "2025-01-10"},
        ],
        current_record_breakdown={"archive_records": 16, "secondary_source_records": 2, "live_fetched_records": 0, "merged_count": 18},
        comparison_record_breakdown={"archive_records": 6, "secondary_source_records": 3, "live_fetched_records": 0, "merged_count": 9},
        comparison_anchor_date="2025-01-16",
    )

    assert result["comparison_mode"] == "same_event_last_year"
    assert result["heat_version"] == "v2"
    assert result["comparison_event_key"] == "2024Q4"
    assert result["news_heat_ratio"] == 2.0
    assert result["news_heat_label"] == "高"
    assert len(result["panels"]) == 3
    assert result["panels"][0]["panel_id"] == "coverage_panel"
    assert result["panels"][1]["panel_id"] == "recency_panel"
    assert result["panels"][2]["panel_id"] == "source_mix_panel"


def test_analyze_news_heat_uses_recent_baseline_for_one_off_event():
    result = analyze_news_heat(
        analysis_target="2330 台積電",
        event_type="重大消息",
        event_date="2026-01-16",
        current_window_total=12,
        baseline_window_total=24,
        current_records=[
            {"article_date": "2026-01-15"},
            {"article_date": "2026-01-13"},
        ],
        comparison_records=[
            {"article_date": "2025-12-31"},
            {"article_date": "2025-12-20"},
        ],
        current_record_breakdown={"archive_records": 8, "secondary_source_records": 4, "live_fetched_records": 0, "merged_count": 12},
        comparison_record_breakdown={"archive_records": 12, "secondary_source_records": 12, "live_fetched_records": 0, "merged_count": 24},
        comparison_anchor_date="2026-01-09",
    )

    assert result["comparison_mode"] == "recent_baseline"
    assert result["comparison_value"] == 6.0
    assert result["news_heat_ratio"] == 2.0
    assert result["news_heat_label"] == "高"
    assert result["panels"][0]["comparison_value"] == 6.0


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
    assert all(panel["status"] == "insufficient_data" for panel in result["panels"])


def test_scan_event_heat_uses_previous_year_event_key_for_recurring_events(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "tools.heat_analysis.resolve_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "2026-01-16",
        },
    )

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        if kwargs["event_key"] == "2025Q4" and kwargs["start_date"] == "2026-01-09":
            return {
                "record_count": 8,
                "record_breakdown": {"archive_records": 6, "secondary_source_records": 2, "live_fetched_records": 0},
                "records": [
                    {"headline": "current", "article_date": "2026-01-15"},
                    {"headline": "current", "article_date": "2026-01-14"},
                ],
            }
        if kwargs["event_key"] == "2025Q4":
            return {
                "record_count": 5,
                "record_breakdown": {"archive_records": 4, "secondary_source_records": 1, "live_fetched_records": 0},
                "records": [
                    {"headline": "post current", "article_date": "2026-01-17"},
                ],
            }
        if kwargs["start_date"] == "2025-01-17":
            return {
                "record_count": 3,
                "record_breakdown": {"archive_records": 1, "secondary_source_records": 2, "live_fetched_records": 0},
                "records": [
                    {"headline": "post previous", "article_date": "2025-01-18"},
                ],
            }
        return {
            "record_count": 4,
            "record_breakdown": {"archive_records": 2, "secondary_source_records": 2, "live_fetched_records": 0},
            "records": [
                {"headline": "previous", "article_date": "2025-01-15"},
                {"headline": "previous", "article_date": "2025-01-12"},
            ],
        }

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        event_date="2026-01-16",
        event_key="2025Q4",
        max_results=24,
    )

    assert len(calls) == 4
    assert calls[1]["start_date"] == "2026-01-17"
    assert calls[2]["event_key"] == "2024Q4"
    assert calls[3]["event_key"] == "2024Q4"
    assert calls[0]["max_results"] == 200
    assert result["comparison_mode"] == "same_event_last_year"
    assert result["requested_phase"] == "both"
    assert result["current_record_count"] == 8
    assert result["comparison_record_count"] == 4
    assert result["news_heat_ratio"] == 2.0
    assert len(result["panels"]) == 3
    assert result["panel_interpretation"]
    assert result["pre_event_heat_scan"]["comparison_record_count"] == 4
    assert result["post_event_heat_scan"]["comparison_record_count"] == 3
    assert result["post_event_heat_scan"]["panels"][0]["panel_id"] == "coverage_panel"


def test_scan_event_heat_resolves_official_event_date_before_building_windows(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "tools.heat_analysis.resolve_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "2025-04-17",
        },
    )

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        return {
            "record_count": 1,
            "record_breakdown": {"archive_records": 1, "secondary_source_records": 0, "live_fetched_records": 0},
            "records": [{"headline": "current", "article_date": kwargs["end_date"]}],
        }

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        event_date="2025-04-16",
        event_key="2025Q1",
        max_results=24,
    )

    assert calls[0]["start_date"] == "2025-04-10"
    assert calls[0]["event_date"] == "2025-04-17"
    assert result["event_date"] == "2025-04-17"


def test_scan_event_heat_uses_baseline_window_for_one_off_events(monkeypatch):
    calls = []

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        if kwargs["start_date"] == "2026-01-09":
            return {
                "record_count": 12,
                "record_breakdown": {"archive_records": 10, "secondary_source_records": 2, "live_fetched_records": 0},
                "records": [
                {"headline": "current", "article_date": "2026-01-15"},
                {"headline": "current", "article_date": "2026-01-13"},
            ],
        }
        if kwargs["start_date"] == "2026-01-17":
            return {
                "record_count": 6,
                "record_breakdown": {"archive_records": 5, "secondary_source_records": 1, "live_fetched_records": 0},
                "records": [
                    {"headline": "post current", "article_date": "2026-01-18"},
                ],
            }
        if kwargs["start_date"] == "2026-01-24":
            return {
                "record_count": 8,
                "record_breakdown": {"archive_records": 6, "secondary_source_records": 2, "live_fetched_records": 0},
                "records": [
                    {"headline": "post baseline", "article_date": "2026-01-24"},
                ],
            }
        return {
            "record_count": 24,
            "record_breakdown": {"archive_records": 12, "secondary_source_records": 10, "live_fetched_records": 2},
            "records": [
                {"headline": "baseline", "article_date": "2025-12-31"},
                {"headline": "baseline", "article_date": "2025-12-20"},
            ],
        }

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="重大消息",
        event_date="2026-01-16",
        max_results=24,
    )

    assert len(calls) == 4
    assert calls[1]["start_date"] == "2026-01-17"
    assert calls[2]["start_date"] == "2025-12-10"
    assert calls[2]["end_date"] == "2026-01-08"
    assert calls[3]["start_date"] == "2026-01-24"
    assert calls[3]["end_date"] == "2026-01-30"
    assert calls[0]["max_results"] == 200
    assert result["comparison_mode"] == "recent_baseline"
    assert result["comparison_record_count"] == 24
    assert result["news_heat_ratio"] == 2.0
    assert result["panels"][2]["status"] in {"primary_heavier", "secondary_heavier", "similar"}
    assert result["post_event_heat_scan"]["comparison_basis"] == "post_event_baseline"


def test_scan_event_heat_can_limit_to_post_event_phase(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "tools.heat_analysis.resolve_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "2026-01-16",
        },
    )

    def fake_collect_event_records(**kwargs):
        calls.append(kwargs)
        return {
            "record_count": 7,
            "record_breakdown": {"archive_records": 5, "secondary_source_records": 2, "live_fetched_records": 0},
            "records": [
                {"headline": "post current", "article_date": "2026-01-17"},
            ],
        }

    monkeypatch.setattr("tools.heat_analysis.collect_event_records", fake_collect_event_records)

    result = scan_event_heat(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        event_date="2026-01-16",
        event_key="2025Q4",
        max_results=24,
        phase="post_event",
    )

    assert len(calls) == 2
    assert all(call["start_date"] == "2026-01-17" or call["start_date"] == "2025-01-17" for call in calls)
    assert result["requested_phase"] == "post_event"
    assert result["pre_event_heat_scan"] is None
    assert result["post_event_heat_scan"]["current_record_count"] == 7
    assert result["panels"][0]["panel_id"] == "coverage_panel"
