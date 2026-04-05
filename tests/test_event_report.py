"""Unit tests for event report assembly helpers."""

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import _build_event_study_payload, event_report
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
            {
                "headline": "台積電法說會前市場預期",
                "article_type": "法說前預期",
                "event_phase": "pre_event",
                "article_date": "2025-04-15",
                "source_name": "鉅亨網",
                "summary": "市場聚焦 AI 需求與毛利率。",
                "is_post_event_earnings_related": False,
                "post_event_relevance_score": 0,
            },
            {
                "headline": "台積電法說會當日公布結果",
                "article_type": "媒體報導",
                "event_phase": "event_day",
                "article_date": "2025-04-17",
                "source_name": "鉅亨網",
                "summary": "市場開始解讀管理層展望與資本支出。",
                "is_post_event_earnings_related": True,
                "post_event_relevance_score": 6,
            },
        ],
    }


def _make_heat_analysis():
    return {
        "analysis_target": "2330 台積電",
        "heat_version": "v2",
        "event_type": "法說會",
        "event_date": "2025-04-17",
        "requested_phase": "both",
        "comparison_mode": "same_event_last_year",
        "event_key": "2025Q1",
        "comparison_event_key": "2024Q1",
        "comparison_ready": True,
        "available_heat_scans": ["pre_event", "post_event"],
        "comparison_basis": "same_event_last_year",
        "current_window_total": 12,
        "comparison_value": 6,
        "news_heat_ratio": 2.0,
        "news_heat_label": "高",
        "panels": [
            {
                "panel_id": "coverage_panel",
                "label": "Coverage",
                "current_value": 12,
                "comparison_value": 6,
                "delta": {"absolute": 6, "ratio": 2.0},
                "status": "elevated",
                "summary": "事件前覆蓋量明顯高於對照值。",
                "data_gaps": [],
            },
            {
                "panel_id": "recency_panel",
                "label": "Recency",
                "current_value": 5.5,
                "comparison_value": 4.0,
                "delta": 1.5,
                "status": "late_build",
                "summary": "事件前最後幾天才明顯升溫。",
                "data_gaps": [],
            },
            {
                "panel_id": "source_mix_panel",
                "label": "Source Mix",
                "current_value": {"primary_share": 0.8, "secondary_share": 0.2, "merged_count": 12},
                "comparison_value": {"primary_share": 0.5, "secondary_share": 0.5, "merged_count": 6},
                "delta": {"primary_share": 0.3, "secondary_share": -0.3},
                "status": "primary_heavier",
                "summary": "本次來源結構更偏向 primary / archive 路徑。",
                "data_gaps": [],
            },
        ],
        "panel_interpretation": [
            "事件前新聞覆蓋量明顯升高，coverage ratio 約為 2.0。",
            "熱度較集中在事件前最後幾天才拉高。",
            "本次來源結構更偏向 primary / archive 路徑。",
        ],
        "pre_event_heat_scan": {
            "phase": "pre_event",
            "comparison_basis": "same_event_last_year",
            "current_window": {"start": "2025-04-10", "end": "2025-04-16"},
            "current_record_count": 12,
            "comparison_window": {"start": "2024-04-10", "end": "2024-04-16", "event_date": "2024-04-17"},
            "comparison_record_count": 6,
            "current_window_total": 12,
            "comparison_value": 6,
            "news_heat_ratio": 2.0,
            "news_heat_label": "高",
            "panels": [
                {
                    "panel_id": "coverage_panel",
                    "label": "Coverage",
                    "current_value": 12,
                    "comparison_value": 6,
                    "delta": {"absolute": 6, "ratio": 2.0},
                    "status": "elevated",
                    "summary": "事件前覆蓋量明顯高於對照值。",
                    "data_gaps": [],
                },
                {
                    "panel_id": "recency_panel",
                    "label": "Recency",
                    "current_value": 5.5,
                    "comparison_value": 4.0,
                    "delta": 1.5,
                    "status": "late_build",
                    "summary": "事件前最後幾天才明顯升溫。",
                    "data_gaps": [],
                },
                {
                    "panel_id": "source_mix_panel",
                    "label": "Source Mix",
                    "current_value": {"primary_share": 0.8, "secondary_share": 0.2, "merged_count": 12},
                    "comparison_value": {"primary_share": 0.5, "secondary_share": 0.5, "merged_count": 6},
                    "delta": {"primary_share": 0.3, "secondary_share": -0.3},
                    "status": "primary_heavier",
                    "summary": "本次來源結構更偏向 primary / archive 路徑。",
                    "data_gaps": [],
                },
            ],
            "panel_interpretation": [
                "事件前新聞覆蓋量明顯升高，coverage ratio 約為 2.0。",
                "熱度較集中在事件前最後幾天才拉高。",
                "本次來源結構更偏向 primary / archive 路徑。",
            ],
            "data_gaps": [],
        },
        "post_event_heat_scan": {
            "phase": "post_event",
            "comparison_basis": "same_event_last_year",
            "current_window": {"start": "2025-04-18", "end": "2025-04-24"},
            "current_record_count": 9,
            "comparison_window": {"start": "2024-04-18", "end": "2024-04-24", "event_date": "2024-04-17"},
            "comparison_record_count": 3,
            "current_window_total": 9,
            "comparison_value": 3,
            "news_heat_ratio": 3.0,
            "news_heat_label": "極高",
            "panels": [
                {
                    "panel_id": "coverage_panel",
                    "label": "Coverage",
                    "current_value": 9,
                    "comparison_value": 3,
                    "delta": {"absolute": 6, "ratio": 3.0},
                    "status": "surging",
                    "summary": "事件後覆蓋量明顯高於對照值。",
                    "data_gaps": [],
                }
            ],
            "panel_interpretation": [
                "事件後 coverage 高於對照期，ratio 約為 3.0。",
            ],
            "data_gaps": [],
        },
        "data_gaps": [],
    }


def _make_post_event_analysis():
    return {
        "mode": "rule_based_fallback",
        "used_record_count": 3,
        "report": (
            "財務相關資訊\n"
            "法說後新聞主要聚焦毛利率與資本支出。\n\n"
            "管理階層展望\n"
            "媒體多以管理層對 AI 需求與後續展望的說法作為解讀基礎。  \n\n"
            "關注領域重點\n"
            "市場關注點集中在毛利率、capex 與法人反應。\n\n"
            "新聞內容分歧\n"
            "不同文章對毛利率壓力與需求韌性的解讀略有差異。"
        ),
        "data_gaps": ["gemini_post_event_analysis_unavailable"],
    }


def test_build_event_report_payload_assembles_sections():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
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
    assert len(payload["sections"]["heat_analysis"]["panels"]) == 3
    assert "## 一、事件摘要" in payload["markdown"]
    assert "## 二、市場事件前敘事" in payload["markdown"]
    assert "## 三、市場事件後敘事" in payload["markdown"]
    assert "## 四、前後敘事轉折" in payload["markdown"]
    assert "## 五、熱度分析" in payload["markdown"]
    assert "### 事件前 heat scan" in payload["markdown"]
    assert "### 事件後 heat scan" in payload["markdown"]
    assert "| Panel | Current | Comparison | Delta | Status | Summary |" in payload["markdown"]
    assert "事件前新聞覆蓋量明顯升高" in payload["markdown"]
    assert "事件後 coverage 高於對照期" in payload["markdown"]
    assert "整理模式" in payload["markdown"]
    assert "重點整理" in payload["markdown"]
    assert "財務相關資訊" in payload["markdown"]
    assert "## 六、官方來源清單" not in payload["markdown"]
    assert "## 十、事件研究驗證（可選）" not in payload["markdown"]
    assert "市場反應日（t=0）" not in payload["markdown"]
    assert "gross margin 58%" not in payload["markdown"]
    assert "id=" not in payload["markdown"]


def test_build_event_report_payload_marks_missing_heat_analysis():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=None,
        post_event_analysis=None,
        generated_at="2026-04-02 09:30:00",
    )

    assert "heat_analysis_missing" in payload["data_gaps"]
    assert "尚未提供熱度分析資料。" in payload["markdown"]


def test_build_event_report_payload_no_longer_requires_expectation_analysis():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    assert "expectation_analysis_missing" not in payload["data_gaps"]
    assert "## 二、市場事件前敘事" in payload["markdown"]
    assert "## 三、市場事件後敘事" in payload["markdown"]


def test_render_heat_block_falls_back_to_legacy_fields_without_panels():
    legacy_heat = _make_heat_analysis()
    legacy_heat.pop("panels")
    legacy_heat.pop("panel_interpretation")
    legacy_heat.pop("heat_version")
    legacy_heat.pop("pre_event_heat_scan")
    legacy_heat.pop("post_event_heat_scan")
    legacy_heat.pop("available_heat_scans")
    legacy_heat.pop("requested_phase")

    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=legacy_heat,
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    assert "- **熱度比**：2.0" in payload["markdown"]
    assert "- **熱度標籤**：高" in payload["markdown"]


def test_render_event_report_markdown_uses_structured_sections():
    payload = build_event_report_payload(
        event_collection=_make_event_collection(),
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    markdown = render_event_report_markdown(payload["metadata"], payload["sections"])
    assert markdown.startswith("# 台積電 事件報告")
    assert "## 五、熱度分析" in markdown
    assert "## 六、官方來源清單" not in markdown


def test_build_event_report_payload_adds_data_coverage_note_for_older_events():
    event_collection = _make_event_collection()
    event_collection["query"]["event_date"] = "2023-10-19"

    payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    markdown = payload["markdown"]
    assert "2024-10 起" in markdown
    assert "早於 2024-10-01" in markdown


def test_build_event_report_payload_summarizes_narrative_shift():
    event_collection = _make_event_collection()
    event_collection["records"] = [
        {
            "headline": "台積電法說會前市場預期",
            "article_type": "法說前預期",
            "event_phase": "pre_event",
            "article_date": "2025-04-15",
            "source_name": "鉅亨網",
            "summary": "市場聚焦 AI 需求與毛利率。",
            "is_post_event_earnings_related": False,
            "post_event_relevance_score": 0,
        },
        {
            "headline": "台積電法說會後法人解讀",
            "article_type": "法人解讀",
            "event_phase": "post_event",
            "article_date": "2025-04-18",
            "source_name": "鉅亨網",
            "summary": "事件後轉向關注資本支出與後續展望。",
            "is_post_event_earnings_related": True,
            "post_event_relevance_score": 6,
        },
    ]
    event_collection["record_count"] = 2
    payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    markdown = payload["markdown"]
    assert "事件前敘事筆數 1" in markdown
    assert "事件後敘事筆數 1" in markdown
    assert "事件後新增主題" in markdown
    assert "法人解讀" in markdown


def test_build_event_report_payload_supports_pre_event_only_narrative():
    event_collection = _make_event_collection()
    event_collection["records"] = [
        {
            "headline": "台積電法說會前市場預期",
            "article_type": "法說前預期",
            "event_phase": "pre_event",
            "article_date": "2025-04-15",
            "source_name": "鉅亨網",
            "summary": "市場聚焦 AI 需求與毛利率。",
            "is_post_event_earnings_related": False,
            "post_event_relevance_score": 0,
        }
    ]
    event_collection["record_count"] = 1

    payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    assert "尚未提供事件後敘事資料。" in payload["markdown"]
    assert "尚缺事件後解讀" in payload["markdown"]


def test_build_event_report_payload_filters_post_event_noise_for_earnings_calls():
    event_collection = _make_event_collection()
    event_collection["records"] = [
        {
            "headline": "台積電法說會後法人解讀 聚焦毛利率與資本支出",
            "article_type": "法人解讀",
            "event_phase": "post_event",
            "article_date": "2025-04-18",
            "source_name": "鉅亨網",
            "summary": "法說會後市場轉向關注指引與capex。",
            "is_post_event_earnings_related": True,
            "post_event_relevance_score": 7,
        },
        {
            "headline": "台積電慈善基金會攜手熊本大學締結合作協議",
            "article_type": "媒體報導",
            "event_phase": "post_event",
            "article_date": "2025-04-18",
            "source_name": "ETtoday新聞雲",
            "summary": "與法說內容無直接關聯。",
            "is_post_event_earnings_related": False,
            "post_event_relevance_score": -2,
        },
    ]
    event_collection["record_count"] = 2

    payload = build_event_report_payload(
        event_collection=event_collection,
        heat_analysis=_make_heat_analysis(),
        post_event_analysis=_make_post_event_analysis(),
        generated_at="2026-04-02 09:30:00",
    )

    post_section = payload["sections"]["post_event_narratives"]
    assert post_section["selected_count"] == 1
    assert post_section["raw_count"] == 2
    assert "post_event_noise_filtered:1" in post_section["data_gaps"]
    assert "法人解讀" in payload["markdown"]
    assert "慈善基金會" not in payload["markdown"]


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
        generated_at="2026-04-02 09:30:00",
    )

    markdown = payload["markdown"]
    assert "## 十二、待辦事項" not in markdown
    assert "transcript_missing" not in markdown
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

    with patch("pipeline.fetch_stock_data", side_effect=fake_fetch_stock_data), patch(
        "pipeline.run_event_study", side_effect=fake_run_event_study
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


def test_event_report_collects_pre_event_event_day_and_post_event_separately(monkeypatch):
    collect_calls = []

    def fake_collect_event_records(**kwargs):
        collect_calls.append((kwargs["start_date"], kwargs["end_date"]))
        return {
            "query": {
                "event_type": "法說會",
                "event_date": "2025-04-17",
                "event_key": "2025Q1",
                "time_range": {
                    "start": kwargs["start_date"],
                    "end": kwargs["end_date"],
                    "effective_start": kwargs["start_date"],
                    "effective_end": kwargs["end_date"],
                },
                "stock": {"code": "2330", "name": "台積電", "symbol": "2330.TW"},
            },
            "collection_plan": {
                "sources": ["goodinfo"],
                "comparison_strategy": {
                    "comparison_mode": "same_event_last_year",
                    "comparison_event_key": "2024Q1",
                },
            },
            "data_completeness": {
                "official_sources_included": False,
                "heat_analysis_included": False,
                "comparison_strategy": "same_event_last_year",
                "comparison_ready": True,
                "data_gaps": [],
                "notes": "",
            },
            "data_gaps": [],
            "record_count": 0,
            "record_breakdown": {
                "archive_records": 0,
                "secondary_source_records": 0,
                "live_fetched_records": 0,
            },
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
            "records": [],
        }

    monkeypatch.setattr("pipeline.collect_event_records", fake_collect_event_records)
    monkeypatch.setattr("pipeline.scan_event_heat", lambda **kwargs: _make_heat_analysis())
    monkeypatch.setattr("pipeline.build_post_event_analysis", lambda **kwargs: _make_post_event_analysis())
    monkeypatch.setattr("pipeline.save_event_record", lambda record, topic: f"/tmp/{topic}.json")
    monkeypatch.setattr("pipeline.save_report", lambda content, topic: f"/tmp/{topic}.md")

    output = event_report(
        stock="2330.TW",
        stock_name="台積電",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-24",
        event_date="2025-04-17",
        event_key="2025Q1",
    )

    assert collect_calls == [
        ("2025-04-10", "2025-04-16"),
        ("2025-04-17", "2025-04-17"),
        ("2025-04-18", "2025-04-24"),
    ]
    assert output["json_path"].endswith(".json")
