"""Unit tests for official event source adapters."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.event_sources import (
    collect_official_event_artifacts,
    collect_official_event_records,
    fetch_historical_earnings_event_date,
    fetch_mops_investor_conference,
    fetch_yahoo_calendar_event_date,
    resolve_earnings_event_date,
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
    assert record["official_event_key"] == "2025Q1"


def test_resolve_earnings_event_date_overrides_requested_date_when_official_event_key_matches(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_historical_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "",
            "matched_event_key": "2025Q1",
            "status": "unverified",
            "source": "emops_history",
            "reason": "historical_event_not_found",
            "data_gaps": ["historical_event_not_found"],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_yahoo_calendar_event_date",
        lambda **kwargs: {
            "resolved_event_date": "",
            "matched_event_key": "2025Q1",
            "status": "unverified",
            "source": "yahoo_calendar",
            "reason": "yahoo_calendar_quarter_unresolved",
            "data_gaps": ["yahoo_calendar_quarter_unresolved"],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_mops_investor_conference",
        lambda **kwargs: {
            "article_date": "2025-04-17",
            "official_event_key": "2025Q1",
            "official_page_url": "https://investor.example.com/q1",
        },
    )

    result = resolve_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        start_date="2025-04-01",
        end_date="2025-04-30",
        event_date="2025-04-16",
        event_key="2025Q1",
    )

    assert result["resolved_event_date"] == "2025-04-17"
    assert result["status"] == "overridden_by_mops"
    assert result["data_gaps"] == ["event_date_overridden_by_mops"]


def test_resolve_earnings_event_date_keeps_requested_date_when_official_record_is_another_quarter(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_yahoo_calendar_event_date",
        lambda **kwargs: {
            "resolved_event_date": "",
            "matched_event_key": "2024Q4",
            "status": "unverified",
            "source": "yahoo_calendar",
            "reason": "yahoo_calendar_quarter_unresolved",
            "data_gaps": ["yahoo_calendar_quarter_unresolved"],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_mops_investor_conference",
        lambda **kwargs: {
            "article_date": "2026-04-16",
            "official_event_key": "2026Q1",
            "official_page_url": "https://investor.example.com/q1",
        },
    )

    result = resolve_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        start_date="2025-01-01",
        end_date="2025-04-30",
        event_date="2025-01-16",
        event_key="2024Q4",
    )

    assert result["resolved_event_date"] == "2025-01-16"
    assert result["status"] == "unverified"
    assert "mops_event_key_mismatch" in result["data_gaps"]


def test_fetch_historical_earnings_event_date_resolves_from_emops_history(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources._fetch_emops_history_entries",
        lambda **kwargs: [
            {
                "announcement_date": "2025-03-28",
                "announcement_time": "17:27:21",
                "subject": "TSMC will hold the First Quarter 2025 Earnings Conference on April 17, 2025",
                "detail_url": "https://emops.twse.com.tw/server-java/t05st01_e?step=1&co_id=2330&spoke_date=20250328&spoke_time=172721&seq_no=1",
            }
        ],
    )
    monkeypatch.setattr(
        "tools.event_sources._fetch_emops_history_detail",
        lambda url: {
            "event_date": "2025-04-17",
            "subject": "TSMC will hold the First Quarter 2025 Earnings Conference on April 17, 2025",
            "statement": "1.Date of institutional investor conference:2025/04/17",
            "detail_url": url,
        },
    )

    result = fetch_historical_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_key="2025Q1",
    )

    assert result["resolved_event_date"] == "2025-04-17"
    assert result["status"] == "resolved_from_emops_history"
    assert result["source"] == "emops_history"


def test_fetch_historical_earnings_event_date_handles_q4_cross_year(monkeypatch):
    calls = []

    def fake_entries(stock_code, year):
        calls.append(year)
        if year == 2026:
            return [
                {
                    "announcement_date": "2026-01-03",
                    "announcement_time": "17:27:21",
                    "subject": "TSMC will hold the Fourth Quarter 2025 Earnings Conference on January 15, 2026",
                    "detail_url": "https://emops.twse.com.tw/server-java/t05st01_e?step=1&co_id=2330&spoke_date=20260103&spoke_time=172721&seq_no=1",
                }
            ]
        return []

    monkeypatch.setattr("tools.event_sources._fetch_emops_history_entries", fake_entries)
    monkeypatch.setattr(
        "tools.event_sources._fetch_emops_history_detail",
        lambda url: {
            "event_date": "2026-01-15",
            "subject": "TSMC will hold the Fourth Quarter 2025 Earnings Conference on January 15, 2026",
            "statement": "Date of institutional investor conference:2026/01/15",
            "detail_url": url,
        },
    )

    result = fetch_historical_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_key="2025Q4",
    )

    assert calls == [2025, 2026]
    assert result["resolved_event_date"] == "2026-01-15"
    assert result["matched_event_key"] == "2025Q4"


def test_resolve_earnings_event_date_prefers_historical_event_key_lookup(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_historical_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "2025-04-17",
            "matched_event_key": "2025Q1",
            "status": "resolved_from_emops_history",
            "source": "emops_history",
            "reason": "historical_event_key_match",
            "data_gaps": [],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_mops_investor_conference",
        lambda **kwargs: {
            "article_date": "2026-04-16",
            "official_event_key": "2026Q1",
        },
    )

    result = resolve_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_date="2025-04-16",
        event_key="2025Q1",
    )

    assert result["resolved_event_date"] == "2025-04-17"
    assert result["source"] == "emops_history"
    assert result["official_record"] is None


def test_fetch_yahoo_calendar_event_date_resolves_when_quarter_text_matches(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources._fetch_yahoo_calendar_events",
        lambda **kwargs: [
            {
                "symbol": "2330.TW",
                "symbol_name": "台積電",
                "event_type": "earningsCall",
                "event_type_name": "法說會",
                "event_date": "2025-10-16",
                "detail_date": "2025-10-16T14:00:00+08:00",
                "information": "本公司114年第三季法人說明會",
                "place": "線上法說會",
                "corp_review_name": "國內",
                "source_url": "https://tw.stock.yahoo.com/quote/2330.TW/calendar",
            }
        ],
    )

    result = fetch_yahoo_calendar_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_key="2025Q3",
    )

    assert result["resolved_event_date"] == "2025-10-16"
    assert result["status"] == "resolved_from_yahoo_calendar"
    assert result["source"] == "yahoo_calendar"


def test_fetch_yahoo_calendar_event_date_keeps_unverified_when_quarter_text_missing(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources._fetch_yahoo_calendar_events",
        lambda **kwargs: [
            {
                "symbol": "2412.TW",
                "symbol_name": "中華電",
                "event_type": "earningsCall",
                "event_type_name": "法說會",
                "event_date": "2025-01-23",
                "detail_date": "2025-01-23T15:00:00+08:00",
                "information": "本公司召開線上法說會",
                "place": "電話會議",
                "corp_review_name": "國內",
                "source_url": "https://tw.stock.yahoo.com/quote/2412.TW/calendar",
            }
        ],
    )

    result = fetch_yahoo_calendar_event_date(
        stock_code="2412",
        stock_name="中華電",
        symbol="2412.TW",
        event_key="2024Q4",
    )

    assert result["resolved_event_date"] == ""
    assert result["status"] == "unverified"
    assert result["data_gaps"] == ["yahoo_calendar_quarter_unresolved"]


def test_resolve_earnings_event_date_falls_back_to_yahoo_calendar_when_emops_history_misses(monkeypatch):
    monkeypatch.setattr(
        "tools.event_sources.fetch_historical_earnings_event_date",
        lambda **kwargs: {
            "resolved_event_date": "",
            "matched_event_key": "2025Q3",
            "status": "unverified",
            "source": "emops_history",
            "reason": "historical_event_not_found",
            "data_gaps": ["historical_event_not_found"],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_yahoo_calendar_event_date",
        lambda **kwargs: {
            "resolved_event_date": "2025-10-16",
            "matched_event_key": "2025Q3",
            "status": "resolved_from_yahoo_calendar",
            "source": "yahoo_calendar",
            "reason": "historical_event_key_match_from_yahoo_calendar",
            "data_gaps": [],
        },
    )

    result = resolve_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_date="2025-10-15",
        event_key="2025Q3",
    )

    assert result["resolved_event_date"] == "2025-10-16"
    assert result["status"] == "overridden_by_yahoo_calendar"
    assert result["source"] == "yahoo_calendar"
    assert result["official_record"] is None


def test_resolve_earnings_event_date_skips_historical_scope_outside_top10_and_cutoff(monkeypatch):
    historical_calls = []
    yahoo_calls = []

    monkeypatch.setattr(
        "tools.event_sources.fetch_historical_earnings_event_date",
        lambda **kwargs: historical_calls.append(kwargs) or {
            "resolved_event_date": "2024-11-14",
            "matched_event_key": "2024Q3",
            "status": "resolved_from_emops_history",
            "source": "emops_history",
            "reason": "historical_event_key_match",
            "data_gaps": [],
        },
    )
    monkeypatch.setattr(
        "tools.event_sources.fetch_yahoo_calendar_event_date",
        lambda **kwargs: yahoo_calls.append(kwargs) or {
            "resolved_event_date": "2024-11-14",
            "matched_event_key": "2024Q3",
            "status": "resolved_from_yahoo_calendar",
            "source": "yahoo_calendar",
            "reason": "historical_event_key_match_from_yahoo_calendar",
            "data_gaps": [],
        },
    )
    monkeypatch.setattr("tools.event_sources.fetch_mops_investor_conference", lambda **kwargs: None)

    outside_top10 = resolve_earnings_event_date(
        stock_code="2881",
        stock_name="富邦金",
        symbol="2881.TW",
        event_date="2024-11-14",
        event_key="2024Q3",
    )
    before_cutoff = resolve_earnings_event_date(
        stock_code="2330",
        stock_name="台積電",
        symbol="2330.TW",
        event_date="2024-07-18",
        event_key="2024Q2",
    )

    assert historical_calls == []
    assert yahoo_calls == []
    assert outside_top10["resolved_event_date"] == "2024-11-14"
    assert outside_top10["status"] == "unverified"
    assert before_cutoff["resolved_event_date"] == "2024-07-18"
    assert before_cutoff["status"] == "unverified"


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
