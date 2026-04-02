"""Unit tests for structured expectation-vs-actual analysis."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.expectation_analysis import (
    analyze_expectation_vs_actual,
    extract_metric_observations,
    normalize_event_phase,
    normalize_metric_name,
)


def test_normalize_metric_name_and_event_phase():
    assert normalize_metric_name("毛利率") == "gross_margin"
    assert normalize_metric_name("Gross Margin") == "gross_margin"
    assert normalize_metric_name("營益率") == "operating_margin"
    assert normalize_metric_name("unknown") == ""

    assert normalize_event_phase("pre-event") == "pre_event"
    assert normalize_event_phase("event day") == "event_day"
    assert normalize_event_phase("post_event") == "post_event"


def test_extract_metric_observations_from_text_record():
    record = {
        "event_key": "2025Q4",
        "event_phase": "pre_event",
        "headline": "法人預估台積電法說會",
        "summary": "毛利率 57~59%，營收年增 20%，EPS 3.4~3.6 元，資本支出 300 億。",
        "source_type": "media",
    }

    observations = extract_metric_observations(record, record_index=0)
    by_metric = {item["metric"]: item for item in observations}

    assert by_metric["gross_margin"]["value_low"] == 57.0
    assert by_metric["gross_margin"]["value_high"] == 59.0
    assert by_metric["revenue"]["value_low"] == 20.0
    assert by_metric["revenue"]["unit"] == "%"
    assert by_metric["eps"]["value_low"] == 3.4
    assert by_metric["eps"]["value_high"] == 3.6
    assert by_metric["capex"]["value_low"] == 300.0
    assert by_metric["capex"]["unit"] == "億"


def test_analyze_expectation_vs_actual_compares_same_event_key():
    records = [
        {
            "event_key": "2025Q4",
            "event_phase": "pre_event",
            "headline": "法人預估毛利率 57~59%、營收年增 20%",
            "summary": "EPS 3.4~3.6 元，資本支出 300 億。",
            "source_type": "media",
        },
        {
            "event_key": "2025Q4",
            "event_phase": "event_day",
            "headline": "公司公布毛利率 58%、營收年增 23%",
            "summary": "EPS 3.5 元，資本支出 300 億。",
            "source_type": "official",
        },
        {
            "event_key": "2024Q4",
            "event_phase": "event_day",
            "headline": "不同事件不應納入比較",
            "summary": "毛利率 40%。",
            "source_type": "official",
        },
    ]

    result = analyze_expectation_vs_actual(records, event_key="2025Q4", event_type="法說會")
    metric_map = {item["metric"]: item for item in result["metrics"]}

    assert result["analysis_target"] == {"event_type": "法說會", "event_key": "2025Q4"}
    assert result["records_considered"] == 2
    assert metric_map["gross_margin"]["status"] == "matched"
    assert metric_map["revenue"]["status"] == "beat"
    assert metric_map["eps"]["status"] == "matched"
    assert metric_map["capex"]["status"] == "matched"
    assert metric_map["operating_margin"]["status"] == "unknown"
    assert metric_map["guidance"]["status"] == "unknown"
    assert "operating_margin_comparison_unavailable" in result["data_gaps"]
    assert "guidance_comparison_unavailable" in result["data_gaps"]


def test_analyze_expectation_vs_actual_returns_conservative_partially_matched_for_capex():
    records = [
        {
            "event_key": "2025Q4",
            "event_phase": "pre_event",
            "headline": "法人預估資本支出 300 億",
            "summary": "市場預期維持高檔。",
            "source_type": "media",
        },
        {
            "event_key": "2025Q4",
            "event_phase": "event_day",
            "headline": "公司實際資本支出 350 億",
            "summary": "持續投資先進製程。",
            "source_type": "official",
        },
    ]

    result = analyze_expectation_vs_actual(records, event_key="2025Q4", event_type="法說會")
    metric_map = {item["metric"]: item for item in result["metrics"]}

    assert metric_map["capex"]["status"] == "partially_matched"


def test_analyze_expectation_vs_actual_uses_unknown_when_no_numeric_basis():
    records = [
        {
            "event_key": "2025Q4",
            "event_phase": "pre_event",
            "headline": "市場看好毛利率",
            "summary": "營運維持穩健。",
            "source_type": "media",
        },
        {
            "event_key": "2025Q4",
            "event_phase": "event_day",
            "headline": "公司說毛利率優於預期",
            "summary": "展望仍佳。",
            "source_type": "official",
        },
    ]

    result = analyze_expectation_vs_actual(records, event_key="2025Q4", event_type="法說會")
    metric_map = {item["metric"]: item for item in result["metrics"]}

    assert metric_map["gross_margin"]["status"] == "unknown"
    assert "gross_margin_comparison_unavailable" in result["data_gaps"]


def test_analyze_expectation_vs_actual_merges_hybrid_observations(monkeypatch):
    monkeypatch.setattr(
        "tools.expectation_analysis._extract_hybrid_observations",
        lambda records: [
            {
                "metric": "guidance",
                "event_key": "2025Q4",
                "event_phase": "pre_event",
                "value_low": 20.0,
                "value_high": 25.0,
                "unit": "%",
                "source_text": "全年營收年增 20% 至 25%",
                "source_headline": "法人預期全年營收年增 20% 至 25%",
                "source_record_index": 0,
                "source_kind": "media",
                "score": 80.0,
                "confidence": 0.92,
                "direction": "up",
                "is_expectation": True,
                "is_actual": False,
                "evidence_span": "全年營收年增 20% 至 25%",
                "hybrid_extracted": True,
            }
        ],
    )

    records = [
        {
            "event_key": "2025Q4",
            "event_phase": "pre_event",
            "headline": "法人預期全年營收年增 20% 至 25%",
            "summary": "市場預估台積電全年展望上修。",
            "source_type": "media",
        }
    ]

    result = analyze_expectation_vs_actual(records, event_key="2025Q4", event_type="法說會")
    metric_map = {item["metric"]: item for item in result["metrics"]}

    assert result["hybrid_observations_considered"] == 1
    assert metric_map["guidance"]["expectation"]["hybrid_extracted"] is True
    assert metric_map["guidance"]["expectation"]["confidence"] == 0.92


def test_analyze_expectation_vs_actual_gracefully_handles_hybrid_failures(monkeypatch):
    monkeypatch.setattr(
        "tools.expectation_analysis._extract_hybrid_observations",
        lambda records: (_ for _ in ()).throw(RuntimeError("quota exceeded")),
    )

    records = [
        {
            "event_key": "2025Q4",
            "event_phase": "pre_event",
            "headline": "法人預估毛利率 57~59%",
            "summary": "EPS 3.4~3.6 元。",
            "source_type": "media",
        }
    ]

    result = analyze_expectation_vs_actual(records, event_key="2025Q4", event_type="法說會")

    assert result["hybrid_observations_considered"] == 0
    assert result["hybrid_error"] == "RuntimeError"
