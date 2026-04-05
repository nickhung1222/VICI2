"""Unit tests for the structured event collector."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.event_collector import build_collection_queries, collect_event_records


def test_build_collection_queries_uses_name_code_and_symbol():
    queries = build_collection_queries(
        target={"symbol": "2330.TW", "code": "2330", "name": "台積電"},
        event_type="法說會",
        event_key="2024Q4",
        event_date="2025-01-16",
    )
    assert queries == [
        "台積電 2024Q4 法說會",
        "台積電 法說會",
        "台積電 法說會 2025",
        "2330 2024Q4 法說會",
        "2330 法說會",
        "2330 法說會 2025",
    ]


def test_collect_event_records_returns_structured_payload(monkeypatch):
    fake_articles = [
        {
            "headline": "台積電法說會前市場預期 AI 需求續強",
            "published_at": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "法人預期毛利率維持高檔。",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
        {
            "headline": "台積電法說會前市場預期 AI 需求續強",
            "published_at": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "重複資料應被去除。",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
        {
            "headline": "分析師看好台積電法說會資本支出指引",
            "published_at": "2025-04-14",
            "source": "moneydj",
            "url": "https://example.com/b",
            "snippet": "外資聚焦資本支出。",
            "source_article_id": "",
            "retrieval_method": "goodinfo_http_index",
            "is_primary_source": False,
            "dedupe_key": "https://example.com/b",
        },
    ]

    call_count = {"search": 0}

    def fake_search_news(query, date_from, date_to, max_results, **kwargs):
        call_count["search"] += 1
        return fake_articles

    def fake_fetch_article_content(url, news_id=""):
        return "Q1 營收與毛利率展望優於市場預期，資本支出維持高檔。"

    monkeypatch.setattr("tools.event_collector.search_news", fake_search_news)
    monkeypatch.setattr("tools.event_collector.fetch_article_content", fake_fetch_article_content)
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [],
            "data_gaps": [],
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
        },
    )

    payload = collect_event_records(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-17",
        event_date="2025-04-17",
        event_key="2025Q1",
        max_results=5,
    )

    assert payload["query"]["stock"]["symbol"] == "2330.TW"
    assert payload["query"]["event_key"] == "2025Q1"
    assert payload["collection_plan"]["comparison_strategy"]["comparison_mode"] == "same_event_last_year"
    assert payload["collection_plan"]["comparison_strategy"]["comparison_event_key"] == "2024Q1"
    assert payload["record_count"] == 2
    assert payload["records"][0]["article_date"] == "2025-04-14"
    assert payload["records"][0]["event_key"] == "2025Q1"
    assert payload["records"][0]["event_phase"] == "pre_event"
    assert payload["records"][0]["is_expectation"] is True
    assert payload["records"][0]["is_actual"] is False
    assert payload["records"][0]["article_type"] == "分析師觀點"
    assert payload["records"][1]["article_type"] == "法說前預期"
    assert payload["records"][0]["summary"].startswith("Q1 營收與毛利率展望")
    assert payload["data_completeness"]["official_sources_included"] is False
    assert payload["record_breakdown"]["archive_records"] == 1
    assert payload["record_breakdown"]["secondary_source_records"] == 1
    assert payload["record_breakdown"]["live_fetched_records"] == 0
    assert sum(payload["record_breakdown"].values()) == payload["record_count"]
    assert call_count["search"] == 1
    assert payload["official_artifacts"] == []
    assert payload["todo_items"] == []


def test_collect_event_records_marks_post_event_earnings_related_articles(monkeypatch):
    fake_articles = [
        {
            "headline": "台積電法說後法人解讀 聚焦毛利率與資本支出",
            "published_at": "2025-04-18",
            "source": "cnyes",
            "url": "https://example.com/post-related",
            "snippet": "法說會後法人持續追蹤展望與capex。",
            "source_article_id": "2",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/post-related",
        },
        {
            "headline": "台積電慈善基金會攜手熊本大學締結合作協議",
            "published_at": "2025-04-18",
            "source": "cnyes",
            "url": "https://example.com/post-noise",
            "snippet": "與法說內容無直接關聯。",
            "source_article_id": "3",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/post-noise",
        },
    ]

    monkeypatch.setattr("tools.event_collector.search_news", lambda *args, **kwargs: fake_articles)
    monkeypatch.setattr("tools.event_collector.fetch_article_content", lambda **kwargs: "")
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [],
            "data_gaps": [],
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
        },
    )

    payload = collect_event_records(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        start_date="2025-04-18",
        end_date="2025-04-24",
        event_date="2025-04-17",
        event_key="2025Q1",
        max_results=5,
    )

    related_record = next(record for record in payload["records"] if "法人解讀" in record["headline"])
    noise_record = next(record for record in payload["records"] if "慈善基金會" in record["headline"])

    assert related_record["event_phase"] == "post_event"
    assert related_record["article_type"] in {"法說後解讀", "法人解讀"}
    assert related_record["is_post_event_earnings_related"] is True
    assert related_record["post_event_relevance_score"] >= 3
    assert "mentions_earnings_call" in related_record["post_event_relevance_reasons"]

    assert noise_record["is_post_event_earnings_related"] is False
    assert noise_record["post_event_relevance_score"] < 3
    assert "contains_noise_topic" in noise_record["post_event_relevance_reasons"]


def test_collect_event_records_breakdown_uses_final_deduped_articles(monkeypatch):
    fake_articles = [
        {
            "headline": "台積電法說會前市場預期 AI 需求續強",
            "published_at": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "法人預期毛利率維持高檔。",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
        {
            "headline": "台積電法說會前市場預期 AI 需求續強",
            "published_at": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "重複資料應被去除。",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
    ]

    monkeypatch.setattr("tools.event_collector.search_news", lambda *args, **kwargs: fake_articles)
    monkeypatch.setattr("tools.event_collector.fetch_article_content", lambda **kwargs: "")
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [],
            "data_gaps": [],
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
        },
    )

    payload = collect_event_records(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-17",
        event_date="2025-04-17",
        event_key="2025Q1",
        max_results=5,
    )

    assert payload["record_count"] == 1
    assert payload["record_breakdown"]["archive_records"] == 1
    assert sum(payload["record_breakdown"].values()) == 1


def test_collect_event_records_clamps_pre_event_report_window(monkeypatch):
    captured = {}

    def fake_search_news(query, date_from, date_to, max_results, **kwargs):
        captured["date_from"] = date_from
        captured["date_to"] = date_to
        captured["primary_source"] = kwargs.get("primary_source")
        captured["allow_secondary_sources"] = kwargs.get("allow_secondary_sources")
        return {"articles": [], "data_gaps": [], "source_breakdown": {}}

    monkeypatch.setattr("tools.event_collector.search_news", fake_search_news)
    monkeypatch.setattr("tools.event_collector.fetch_article_content", lambda **kwargs: "")
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [],
            "data_gaps": [],
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
        },
    )

    collect_event_records(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        start_date="2024-01-01",
        end_date="2024-04-18",
        event_date="2024-04-18",
        event_key="2024Q1",
        max_results=5,
        pre_event_report_days=7,
        primary_source="goodinfo",
        allow_secondary_sources=False,
    )

    assert captured["date_from"] == "2024-04-11"
    assert captured["date_to"] == "2024-04-17"
    assert captured["primary_source"] == "goodinfo"
    assert captured["allow_secondary_sources"] is False


def test_collect_event_records_reports_no_news_for_strict_goodinfo_only(monkeypatch):
    monkeypatch.setattr(
        "tools.event_collector.search_news",
        lambda **kwargs: {
            "articles": [],
            "data_gaps": ["goodinfo_http_empty", "goodinfo_browser_empty", "no_news_in_interval"],
            "source_breakdown": {"primary_count": 0, "secondary_count": 0, "merged_count": 0},
        },
    )
    monkeypatch.setattr("tools.event_collector.fetch_article_content", lambda **kwargs: "")
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [],
            "data_gaps": [],
            "official_artifacts": [],
            "earnings_digest": {},
            "todo_items": [],
        },
    )

    payload = collect_event_records(
        symbol="2454.TW",
        stock_name="聯發科",
        event_type="法說會",
        start_date="2023-01-01",
        end_date="2023-02-03",
        event_date="2023-02-03",
        event_key="2022Q4",
        max_results=20,
        pre_event_report_days=7,
        primary_source="goodinfo",
        allow_secondary_sources=False,
    )

    assert payload["record_count"] == 0
    assert "no_news_in_interval" in payload["data_completeness"]["data_gaps"]
    assert "該區間沒有新聞" in payload["data_completeness"]["notes"]
    assert payload["todo_items"][0]["id"] == "no_news_in_interval"


def test_collect_event_records_includes_official_sources(monkeypatch):
    monkeypatch.setattr("tools.event_collector.search_news", lambda **kwargs: [])
    monkeypatch.setattr("tools.event_collector.fetch_article_content", lambda **kwargs: "")
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {
            "records": [
                {
                    "stock_code": "2330",
                    "stock_name": "台積電",
                    "symbol": "2330.TW",
                    "event_type": "法說會",
                    "event_date": "2025-04-17",
                    "event_key": "2025Q1",
                    "event_phase": "event_day",
                    "article_date": "2025-04-17",
                    "article_type": "官方公告",
                    "source_type": "official",
                    "source_name": "公開資訊觀測站",
                    "source_url": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
                    "headline": "2025Q1 法說會官方公告",
                    "summary": "官方法說會資訊",
                    "language": "zh-TW",
                    "matched_query": "",
                    "source_kind": "official",
                    "is_expectation": False,
                    "is_actual": True,
                    "expectation_match": "",
                }
            ],
            "data_gaps": [],
            "official_artifacts": [
                {
                    "artifact_type": "mops_notice",
                    "source_name": "公開資訊觀測站",
                    "url": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
                    "retrieval_status": "ok",
                    "validation_status": "validated",
                    "excerpt": "官方法說會資訊",
                }
            ],
            "earnings_digest": {
                "financial_snapshot": {
                    "gross_margin": {
                        "value_low": 58.0,
                        "value_high": 58.0,
                        "unit": "%",
                        "evidence_span": "gross margin 58%",
                        "source_ref": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
                        "source_artifact_type": "mops_notice",
                        "source_name": "公開資訊觀測站",
                        "validation_status": "validated",
                    }
                }
            },
            "todo_items": [],
        },
    )

    payload = collect_event_records(
        symbol="2330",
        stock_name="台積電",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-18",
        event_date="2025-04-17",
        event_key="2025Q1",
        max_results=5,
    )

    assert payload["record_count"] == 1
    assert payload["records"][0]["source_type"] == "official"
    assert payload["data_completeness"]["official_sources_included"] is True
    assert payload["collection_plan"]["sources"][0] == "mops"
    assert payload["official_artifacts"][0]["artifact_type"] == "mops_notice"
    assert payload["earnings_digest"]["financial_snapshot"]["gross_margin"]["validation_status"] == "validated"
