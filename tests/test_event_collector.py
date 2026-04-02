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
            "title": "台積電法說會前市場預期 AI 需求續強",
            "date": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "法人預期毛利率維持高檔。",
            "news_id": "1",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
        {
            "title": "台積電法說會前市場預期 AI 需求續強",
            "date": "2025-04-15",
            "source": "cnyes",
            "url": "https://example.com/a",
            "snippet": "重複資料應被去除。",
            "news_id": "1",
            "source_article_id": "1",
            "retrieval_method": "cnyes_category",
            "is_primary_source": True,
            "dedupe_key": "https://example.com/a",
        },
        {
            "title": "分析師看好台積電法說會資本支出指引",
            "date": "2025-04-14",
            "source": "moneydj",
            "url": "https://example.com/b",
            "snippet": "外資聚焦資本支出。",
            "news_id": "",
            "source_article_id": "",
            "retrieval_method": "goodinfo_stock_date_index",
            "is_primary_source": False,
            "dedupe_key": "https://example.com/b",
        },
    ]

    def fake_search_news(query, date_from, date_to, max_results, **kwargs):
        return fake_articles

    def fake_fetch_article_content(url, news_id=""):
        return "Q1 營收與毛利率展望優於市場預期，資本支出維持高檔。"

    monkeypatch.setattr("tools.event_collector.search_news", fake_search_news)
    monkeypatch.setattr("tools.event_collector.fetch_article_content", fake_fetch_article_content)
    monkeypatch.setattr(
        "tools.event_collector.collect_official_event_records",
        lambda **kwargs: {"records": [], "data_gaps": []},
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
    assert payload["record_breakdown"]["archive_records"] >= 1


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
