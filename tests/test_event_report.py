"""Unit tests for event report assembly helpers."""

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from agent import _build_event_study_payload
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
        "official_artifacts": [
            {
                "artifact_type": "transcript",
                "source_name": "TSMC IR",
                "url": "https://investor.example.com/transcript.pdf",
                "validation_status": "validated",
                "retrieval_status": "ok",
                "excerpt": "We remain confident about AI demand.",
            }
        ],
        "earnings_digest": {
            "financial_snapshot": {
                "gross_margin": {
                    "value_low": 58.0,
                    "value_high": 58.0,
                    "unit": "%",
                    "evidence_span": "gross margin 58%",
                    "source_ref": "https://investor.example.com/transcript.pdf",
                    "source_artifact_type": "transcript",
                    "source_name": "TSMC IR",
                    "validation_status": "validated",
                }
            },
            "management_tone": {
                "label": "bullish",
                "validation_status": "validated",
                "evidence": [
                    {
                        "excerpt": "We remain confident about AI demand.",
                        "source_ref": "https://investor.example.com/transcript.pdf",
                        "source_artifact_type": "transcript",
                    }
                ],
            },
            "qa_topics": [
                {
                    "topic": "capex",
                    "question_summary": "Q: 今年 capex 是否上修？",
                    "answer_summary": "A: 今年資本支出維持高檔。",
                    "source_ref": "https://investor.example.com/transcript.pdf",
                }
            ],
            "official_takeaways": ["公布 2025Q1 財報與展望。"],
            "data_gaps": [],
        },
        "todo_items": [],
        "data_gaps": [],
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
            "event_date": "2025-04-17",
            "reaction_date": "2025-04-18",
            "summary": "事件後市場反應偏正向。",
            "n_events": 1,
            "n_skipped": 0,
            "reaction_shift_trading_days": 1,
            "data_window": {"start": "2024-10-19", "end": "2025-05-02"},
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
    assert "## 二、官方來源清單" in payload["markdown"]
    assert "## 三、法說重點" in payload["markdown"]
    assert "## 四、管理層態度" in payload["markdown"]
    assert "## 五、Q&A 摘要" in payload["markdown"]
    assert "## 九、熱度分析" in payload["markdown"]
    assert "## 十、事件研究（可選）" in payload["markdown"]
    assert "市場反應日（t=0）" in payload["markdown"]
    assert "beat" in payload["markdown"]
    assert "56%" in payload["markdown"]
    assert "gross margin 58%" in payload["markdown"]
    assert "id=" not in payload["markdown"]


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
    assert "## 十一、資料缺口與限制" in markdown


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


def test_build_event_report_payload_renders_todo_items():
    event_collection = _make_event_collection()
    event_collection["todo_items"] = [
        {
            "id": "transcript_missing",
            "priority": "blocking",
            "reason": "No official transcript was found for this earnings call.",
            "next_action": "Fallback to presentation and earnings release for summary fields.",
            "source_context": "earnings_digest.qa_topics",
        }
    ]
    event_collection["data_gaps"] = ["transcript_missing", "qa_not_available"]
    event_collection["earnings_digest"]["qa_topics"] = []
    event_collection["earnings_digest"]["data_gaps"] = ["qa_not_available"]

    payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=_make_heat_analysis(),
        expectation_analysis=_make_expectation_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    markdown = payload["markdown"]
    assert "## 十二、待辦事項" in markdown
    assert "transcript_missing" in markdown
    assert "qa_not_available" in payload["data_gaps"]


def test_build_event_study_payload_extends_price_window_for_post_event_days():
    captured = {}

    def fake_fetch_stock_data(symbol, start_date, end_date):
        captured["symbol"] = symbol
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {
            "stock_returns": [0.01] * 250,
            "market_returns": [0.008] * 250,
            "dates": [f"2024-01-{(i % 28) + 1:02d}" for i in range(250)],
        }

    def fake_run_event_study(**kwargs):
        return {
            "n_events": 1,
            "avg_car": [0.0] * 11,
            "avg_ar": [0.0] * 11,
            "relative_days": list(range(-5, 6)),
            "std_error": [0.0] * 11,
            "t_stats": [0.0] * 11,
            "individual_cars": [[0.0] * 11],
            "skipped_events": [],
            "reaction_dates_used": ["2025-04-18"],
            "aligned_events": [
                {
                    "announcement_date": "2025-04-17",
                    "reaction_date": "2025-04-18",
                    "reaction_shift_trading_days": 1,
                }
            ],
            "reaction_shift_trading_days": kwargs.get("reaction_shift_trading_days", 0),
        }

    with patch("agent.fetch_stock_data", side_effect=fake_fetch_stock_data), patch(
        "agent.run_event_study", side_effect=fake_run_event_study
    ):
        payload = _build_event_study_payload(
            stock="2330.TW",
            event_date="2025-04-17",
            end_date="2025-04-18",
            reaction_shift_trading_days=1,
        )

    assert captured["symbol"] == "2330.TW"
    assert captured["start_date"] == "2024-10-19"
    assert captured["end_date"] == "2025-05-02"
    assert payload["data_window"] == {
        "start": "2024-10-19",
        "end": "2025-05-02",
    }
    assert payload["event_date"] == "2025-04-17"
    assert payload["reaction_date"] == "2025-04-18"
    assert payload["reaction_shift_trading_days"] == 1
