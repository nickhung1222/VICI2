"""Unit tests for event report assembly helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.report import build_event_report_payload, render_event_report_markdown


def _make_event_collection():
    return {
        "query": {
            "event_type": "法說會",
            "event_date": "2025-04-17",
            "event_key": "2025Q1",
            "time_range": {
                "start": "2025-04-01",
                "end": "2025-04-17",
            },
            "stock": {
                "code": "2330",
                "name": "台積電",
                "symbol": "2330.TW",
            },
        },
        "collection_plan": {
            "sources": ["cnyes", "moneydj"],
            "comparison_strategy": {
                "comparison_mode": "same_event_last_year",
                "comparison_event_key": "2024Q1",
            },
        },
        "record_count": 2,
        "records": [
            {"headline": "台積電法說會前市場預期", "article_type": "法說前預期"},
            {"headline": "台積電法說會當日公布結果", "article_type": "媒體報導"},
        ],
    }


def _make_heat_analysis():
    return {
        "analysis_target": "2330 台積電",
        "event_type": "法說會",
        "event_date": "2025-04-17",
        "comparison_mode": "same_event_last_year",
        "event_key": "2025Q1",
        "comparison_event_key": "2024Q1",
        "comparison_ready": True,
        "comparison_basis": "same_event_last_year",
        "current_window_total": 12,
        "comparison_value": 6,
        "news_heat_ratio": 2.0,
        "news_heat_label": "高",
        "data_gaps": [],
    }


def _make_expectation_analysis():
    return {
        "summary": "市場預期營收與毛利率維持高檔。",
        "notes": "僅示意。",
        "data_gaps": [],
        "pre_event_expectations": [
            {
                "metric_name": "營收",
                "content": "法人預估 Q1 營收 1500 億元",
                "source_name": "鉅亨網",
            },
            {
                "metric_name": "毛利率",
                "content": "預估毛利率 55% 至 57%",
                "source_name": "MoneyDJ",
            },
        ],
        "event_day_actuals": [
            {
                "metric_name": "營收",
                "content": "實際 Q1 營收 1520 億元",
                "source_kind": "official",
            },
        ],
        "comparison_rows": [
            {
                "metric_name": "營收",
                "expectation": "1500 億元",
                "actual": "1520 億元",
                "expectation_match": "beat",
            },
            {
                "metric_name": "毛利率",
                "expectation": "55% 至 57%",
                "actual": "56%",
                "expectation_match": "matched",
            },
        ],
    }


def test_build_event_report_payload_assembles_sections():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        expectation_analysis=_make_expectation_analysis(),
        event_study={
            "summary": "事件後市場反應偏正向。",
            "n_events": 1,
            "n_skipped": 0,
            "chart_path": "outputs/charts/demo.png",
            "data_gaps": [],
        },
        generated_at="2026-04-02 09:30:00",
        title="台積電",
    )

    assert payload["report_type"] == "event_report"
    assert payload["metadata"]["stock_code"] == "2330"
    assert payload["sections"]["event_summary"]["record_count"] == 2
    assert payload["sections"]["heat_analysis"]["news_heat_label"] == "高"
    assert "## 一、事件摘要" in payload["markdown"]
    assert "## 五、熱度分析" in payload["markdown"]
    assert "## 六、事件研究（可選）" in payload["markdown"]
    assert "beat" in payload["markdown"]
    assert "56%" in payload["markdown"]


def test_build_event_report_payload_marks_missing_heat_analysis():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=None,
        expectation_analysis=_make_expectation_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    assert "heat_analysis_missing" in payload["data_gaps"]
    assert "尚未提供熱度分析資料。" in payload["markdown"]


def test_build_event_report_payload_marks_missing_expectation_analysis():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        expectation_analysis=None,
        generated_at="2026-04-02 09:30:00",
    )

    assert "expectation_analysis_missing" in payload["data_gaps"]
    assert "尚未提供事件前預期資料。" in payload["markdown"]
    assert "尚未提供預期與實際的比對資料。" in payload["markdown"]


def test_render_event_report_markdown_uses_structured_sections():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        expectation_analysis=_make_expectation_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    markdown = render_event_report_markdown(payload["metadata"], payload["sections"])
    assert markdown.startswith("# 台積電 事件報告")
    assert "## 七、資料缺口與限制" in markdown


def test_build_event_report_payload_accepts_metric_based_expectation_analysis():
    expectation_analysis = {
        "comparison_mode": "expectation_vs_actual",
        "status_counts": {"matched": 1, "unknown": 1},
        "metrics": [
            {
                "metric": "gross_margin",
                "status": "matched",
                "expectation": {"value_low": 57.0, "value_high": 59.0, "unit": "%", "source_kind": "media"},
                "actual": {"value_low": 58.0, "value_high": 58.0, "unit": "%", "source_kind": "official"},
            },
            {
                "metric": "guidance",
                "status": "unknown",
                "expectation": None,
                "actual": None,
            },
        ],
        "data_gaps": ["guidance_comparison_unavailable"],
    }

    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        expectation_analysis=expectation_analysis,
        generated_at="2026-04-02 09:30:00",
    )

    markdown = payload["markdown"]
    assert "gross_margin" in markdown
    assert "57.0 ~ 59.0 %" in markdown
    assert "58.0 %" in markdown
    assert "matched: 1；unknown: 1" in markdown
