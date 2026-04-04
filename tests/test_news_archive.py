"""Unit tests for normalized news archive helpers."""

import sys
import subprocess
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.news_archive import (
    build_news_dedupe_key,
    dedupe_news_articles,
    fetch_goodinfo_discovery_records,
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
        "tools.news_archive._fetch_cnyes_symbol_news_as_normalized",
        lambda **kwargs: [
            normalize_news_article(
                source="cnyes",
                source_article_id="123",
                published_at="2025-01-15",
                headline="台積電法說會",
                url="https://news.cnyes.com/news/id/123",
                retrieval_method="cnyes_symbol_news",
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
                retrieval_method="goodinfo_http_index",
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


def test_fetch_goodinfo_discovery_records_parses_data_endpoint(monkeypatch):
    html = """
    <section>
      <table>
        <tr valign="top">
          <th style="padding:8px 4px;text-align:left;" width="1px">
            <nobr><a class="link_blue" target="_blank" href="https://news.cnyes.com/news/id/5823306">Anue鉅亨</a></nobr>
          </th>
          <td style="word-break:break-all;padding:8px 4px;">
            <a class="link_black" target="_blank" href="https://news.cnyes.com/news/id/5823306"><b>輝達執行長黃仁勳CES展開講 市場聚焦三應用</b></a>
            <span style="font-size:9pt;color:gray;font-weight:normal;">(Anue鉅亨&nbsp;2025/01/01 12:40)</span>
            <br/>(<a class="link_blue" target="_blank" href="https://news.cnyes.com/news/id/5823306">詳全文</a>)
          </td>
        </tr>
      </table>
    </section>
    """

    class FakeResponse:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.cookies = type("Cookies", (), {"set": lambda *args, **kwargs: None})()

        def get(self, *args, **kwargs):
            return FakeResponse(html)

    monkeypatch.setattr("tools.news_archive.requests.Session", lambda: FakeSession())

    records = fetch_goodinfo_discovery_records(
        stock_code="2330",
        queries=None,
        date_from="2024-01-01",
        date_to="2025-01-01",
        max_results=1,
    )

    assert len(records) == 1
    assert records[0]["headline"] == "輝達執行長黃仁勳CES展開講 市場聚焦三應用"
    assert records[0]["published_at"] == "2025-01-01"
    assert records[0]["retrieval_method"] == "goodinfo_http_index"
    assert records[0]["stock_code"] == "2330"
    assert records[0]["page"] == 1


def test_fetch_goodinfo_discovery_records_falls_back_to_browser(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.cookies = type("Cookies", (), {"set": lambda *args, **kwargs: None})()

        def get(self, *args, **kwargs):
            raise requests.Timeout("timed out")

    def fake_subprocess_run(command, capture_output, text, timeout, check):
        if "eval" in command:
            return subprocess.CompletedProcess(
                command,
                    0,
                    stdout=(
                        "### Result\n"
                        "[\n"
                        '  {"source_name":"Anue鉅亨","headline":"台積電法說會前夕市場聚焦AI需求",'
                        '"article_url":"https://news.cnyes.com/news/id/1","raw_row_text":"台積電法說會前夕市場聚焦AI需求 (Anue鉅亨 2025/01/15 09:00)"}\n'
                        "]\n"
                        "### Ran Playwright code\n"
                    ),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("tools.news_archive.requests.Session", lambda: FakeSession())
    monkeypatch.setattr("tools.news_archive.subprocess.run", fake_subprocess_run)

    data_gaps = []
    records = fetch_goodinfo_discovery_records(
        stock_code="2330",
        queries=["台積電 法說會"],
        date_from="2025-01-09",
        date_to="2025-01-15",
        max_results=5,
        data_gaps=data_gaps,
    )

    assert len(records) == 1
    assert records[0]["retrieval_method"] == "goodinfo_browser_index"
    assert "goodinfo_http_timeout" in data_gaps


def test_fetch_goodinfo_discovery_records_waits_for_browser_rows(monkeypatch):
    html = """
    <html><body><script>window.location.replace('StockAnnounceList.asp?PAGE=1&REINIT=1');</script></body></html>
    """

    class FakeResponse:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.cookies = type("Cookies", (), {"set": lambda *args, **kwargs: None})()

        def get(self, *args, **kwargs):
            return FakeResponse(html)

    calls = {"eval": 0, "snapshot": 0}

    def fake_subprocess_run(command, capture_output, text, timeout, check):
        if "snapshot" in command:
            calls["snapshot"] += 1
            return subprocess.CompletedProcess(command, 0, stdout="### Snapshot\n", stderr="")
        if "eval" in command:
            calls["eval"] += 1
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=(
                    "### Result\n"
                    "[\n"
                    '  {"source_name":"Anue鉅亨","headline":"台積電法說會前夕市場聚焦AI需求",'
                    '"article_url":"https://news.cnyes.com/news/id/1","raw_row_text":"台積電法說會前夕市場聚焦AI需求 (Anue鉅亨 2025/01/15 09:00) (詳全文)"}\n'
                    "]\n"
                    "### Ran Playwright code\n"
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("tools.news_archive.requests.Session", lambda: FakeSession())
    monkeypatch.setattr("tools.news_archive.subprocess.run", fake_subprocess_run)

    records = fetch_goodinfo_discovery_records(
        stock_code="2330",
        queries=["台積電 法說會"],
        date_from="2025-01-09",
        date_to="2025-01-15",
        max_results=5,
        data_gaps=[],
    )

    assert len(records) == 1
    assert calls["eval"] >= 1
    assert calls["snapshot"] >= 1
