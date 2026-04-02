"""Unit tests for news scraper fallbacks and query variants."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.news_scraper import _build_query_variants, search_news


def test_build_query_variants_adds_broader_fallbacks():
    plans = _build_query_variants("2330 法說會", date_from="2025-04-10", date_to="2025-04-16")
    queries = [plan["query"] for plan in plans]

    assert "2330 法說會" in queries
    assert "2330法說會" in queries
    assert "2330" in queries
    assert "2330 法說會 2025" in queries


def test_search_news_uses_web_fallback_when_primary_sources_fail(monkeypatch):
    monkeypatch.setattr(
        "tools.news_scraper.fetch_news_archive",
        lambda **kwargs: {
            "records": [
                {
                    "headline": "台積電法說會重點整理",
                    "published_at": "2025-04-15",
                    "source": "ctee",
                    "url": "https://example.com/q1",
                    "snippet": "台積電法說會釋出最新展望。",
                    "content": "",
                    "source_article_id": "",
                    "retrieval_method": "google_news_rss",
                    "is_primary_source": False,
                    "dedupe_key": "https://example.com/q1",
                }
            ]
        },
    )

    results = search_news(
        query="台積電 法說會",
        date_from="2025-04-10",
        date_to="2025-04-16",
        max_results=5,
        stock_code="2330",
        stock_name="台積電",
        event_type="法說會",
    )

    assert len(results) == 1
    assert results[0]["source"] == "ctee"
    assert results[0]["title"] == "台積電法說會重點整理"
