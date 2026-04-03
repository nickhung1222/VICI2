"""Unit tests for official event source adapters."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.event_sources import (
    collect_official_event_artifacts,
    collect_official_event_records,
    fetch_mops_investor_conference,
)


def test_fetch_mops_investor_conference_parses_latest_record(monkeypatch):
    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        text = """
        <html><body><table class='hasBorder'>
        <tr><td><b>召開法人說明會日期：</b></td><td>114/04/17 時間：14 點 0 分 (24小時制)</td></tr>
        <tr><td><b>召開法人說明會地點：</b></td><td>線上法說會</td></tr>
        <tr><td><b>法人說明會擇要訊息：</b></td><td>公布 2025Q1 財務報告與展望。</td></tr>
        <tr><td><a href='https://investor.example.com/q1'>link</a></td></tr>
        </table></body></html>
        """

    monkeypatch.setattr("tools.event_sources.requests.post", lambda *args, **kwargs: FakeResponse())

    record = fetch_mops_investor_conference(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_date="2025-04-17",
        event_key="2025Q1",
    )

    assert record is not None
    assert record["article_date"] == "2025-04-17"
    assert record["source_name"] == "公開資訊觀測站"
    assert record["source_type"] == "official"
    assert record["event_phase"] == "event_day"
    assert record["official_page_url"] == "https://investor.example.com/q1"


def test_collect_official_event_artifacts_discovers_html_and_pdf(monkeypatch):
    class HtmlResponse:
        headers = {"Content-Type": "text/html; charset=utf-8"}
        text = """
        <html>
          <head><title>TSMC Q1 2025</title></head>
          <body>
            <a href="/files/q1_presentation.pdf">Presentation Material</a>
            <a href="/files/q1_transcript.pdf">Earnings Conference Transcript</a>
          </body>
        </html>
        """
        content = text.encode("utf-8")

        def raise_for_status(self):
            return None

    class PdfResponse:
        headers = {"Content-Type": "application/pdf"}
        content = b"%PDF-1.4"
        text = ""

        def raise_for_status(self):
            return None

    def fake_get(url, headers=None, timeout=None):
        if url == "https://investor.example.com/q1":
            return HtmlResponse()
        return PdfResponse()

    monkeypatch.setattr("tools.event_sources.requests.get", fake_get)
    monkeypatch.setattr(
        "tools.event_sources._extract_pdf_text",
        lambda raw_bytes: "2025-04-17 gross margin 58% Q&A Question: capex? Answer: 300 億。",
    )

    artifacts, data_gaps = collect_official_event_artifacts(
        stock_code="2330",
        stock_name="台積電",
        event_date="2025-04-17",
        event_key="2025Q1",
        official_page_url="https://investor.example.com/q1",
        mops_record={
            "headline": "2025Q1 法說會官方公告",
            "summary": "官方摘要",
            "source_url": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
            "article_date": "2025-04-17",
        },
    )

    artifact_types = [artifact["artifact_type"] for artifact in artifacts]
    assert artifact_types[0] == "mops_notice"
    assert "presentation" in artifact_types
    assert "transcript" in artifact_types
    assert "official_artifacts_missing" not in data_gaps
    transcript = next(artifact for artifact in artifacts if artifact["artifact_type"] == "transcript")
    assert transcript["format"] == "pdf"
    assert transcript["retrieval_status"] == "ok"


def test_collect_official_event_records_returns_artifacts_digest_and_todos(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_mops_investor_conference",
        lambda **kwargs: {
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
            "summary": "公布 2025Q1 財務報告與展望。",
            "language": "zh-TW",
            "matched_query": "",
            "official_page_url": "https://investor.example.com/q1",
            "source_kind": "official",
            "is_expectation": False,
            "is_actual": True,
            "expectation_match": "",
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.collect_official_event_artifacts",
        lambda **kwargs: (
            [
                {
                    "stock_code": "2330",
                    "company": "台積電",
                    "event_date": "2025-04-17",
                    "event_key": "2025Q1",
                    "artifact_type": "mops_notice",
                    "source_name": "公開資訊觀測站",
                    "url": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
                    "published_at": "2025-04-17",
                    "fetched_at": "2026-04-03T10:00:00+08:00",
                    "format": "html",
                    "language": "zh-TW",
                    "retrieval_status": "ok",
                    "validation_status": "validated",
                    "excerpt": "公布 2025Q1 財務報告與展望。",
                    "title": "MOPS",
                    "content": "公布 2025Q1 財務報告與展望。",
                },
                {
                    "stock_code": "2330",
                    "company": "台積電",
                    "event_date": "2025-04-17",
                    "event_key": "2025Q1",
                    "artifact_type": "transcript",
                    "source_name": "TSMC IR",
                    "url": "https://investor.example.com/q1_transcript.pdf",
                    "published_at": "2025-04-17",
                    "fetched_at": "2026-04-03T10:00:00+08:00",
                    "format": "pdf",
                    "language": "en",
                    "retrieval_status": "ok",
                    "validation_status": "validated",
                    "excerpt": "gross margin 58% capex 300 億 strong demand Q&A Question: capex? Answer: 300 億。",
                    "title": "Transcript",
                    "content": "gross margin 58% capex 300 億. We remain confident about demand. Q&A Question: capex? Answer: 300 億。",
                },
            ],
            [],
        ),
    )

    payload = collect_official_event_records(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-18",
        event_date="2025-04-17",
        event_key="2025Q1",
    )

    assert payload["records"][0]["source_type"] == "official"
    assert payload["records"][1]["article_type"] == "官方重點"
    assert payload["records"][1]["validation_status"] == "validated"
    assert payload["earnings_digest"]["financial_snapshot"]["gross_margin"]["source_ref"] == "https://investor.example.com/q1_transcript.pdf"
    assert payload["earnings_digest"]["management_tone"]["label"] == "bullish"
    assert payload["earnings_digest"]["qa_topics"]
    assert payload["official_artifacts"][1]["artifact_type"] == "transcript"
    assert payload["todo_items"] == []


def test_collect_official_event_records_filters_out_of_range(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_mops_investor_conference",
        lambda **kwargs: {
            "article_date": "2026-04-17",
            "source_type": "official",
        },
    )

    payload = collect_official_event_records(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_type="法說會",
        start_date="2025-04-01",
        end_date="2025-04-18",
        event_date="2025-04-17",
        event_key="2025Q1",
    )

    assert payload["records"] == []
    assert payload["data_gaps"] == ["mops_record_outside_requested_range"]
