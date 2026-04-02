"""Unit tests for official event source adapters."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.event_sources import collect_official_event_records, fetch_mops_investor_conference


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
