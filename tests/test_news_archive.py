"""Unit tests for normalized news archive helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.news_archive import (
    build_news_dedupe_key,
    dedupe_news_articles,
    fetch_news_archive,
    normalize_news_article,
)


def test_build_news_dedupe_key_prefers_canonical_url():
    key = build_news_dedupe_key(
        canonical_url="https://news.cnyes.com/news/id/123",
        source="cnyes",
        source_article_id="123",
        published_at="2025-01-15",
        headline="台積電法說會",
    )
    assert key == "https://news.cnyes.com/news/id/123"


def test_dedupe_news_articles_uses_normalized_keys():
    article = normalize_news_article(
        source="cnyes",
        source_article_id="123",
        published_at="2025-01-15",
        headline="台積電法說會",
        url="https://news.cnyes.com/news/id/123",
        retrieval_method="cnyes_category",
        is_primary_source=True,
    )
    deduped = dedupe_news_articles([article, dict(article)])
    assert len(deduped) == 1


def test_fetch_news_archive_merges_primary_and_secondary(monkeypatch):
    monkeypatch.setattr(
        "tools.news_archive.fetch_cnyes_primary_records",
        lambda **kwargs: [
            normalize_news_article(
                source="cnyes",
                source_article_id="123",
                published_at="2025-01-15",
                headline="台積電法說會",
                url="https://news.cnyes.com/news/id/123",
                retrieval_method="cnyes_category",
                is_primary_source=True,
            )
        ],
    )
    monkeypatch.setattr(
        "tools.news_archive.fetch_goodinfo_discovery_records",
        lambda **kwargs: [
            normalize_news_article(
                source="goodinfo",
                published_at="2025-01-14",
                headline="Goodinfo整理",
                url="https://example.com/goodinfo",
                retrieval_method="goodinfo_stock_date_index",
                is_primary_source=False,
            )
        ],
    )
    monkeypatch.setattr("tools.news_archive.fetch_google_news_rss_records", lambda **kwargs: [])
    monkeypatch.setattr("tools.news_archive.fetch_yfinance_news_records", lambda **kwargs: [])

    payload = fetch_news_archive(
        stock_code="2330",
        stock_name="台積電",
        event_type="法說會",
        date_from="2025-01-09",
        date_to="2025-01-15",
        queries=["台積電 2024Q4 法說會"],
        max_results=5,
    )

    assert payload["source_breakdown"]["primary_count"] == 1
    assert payload["source_breakdown"]["secondary_count"] == 1
    assert payload["source_breakdown"]["merged_count"] == 2
