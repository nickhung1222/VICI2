"""Unit tests for chat mode parsing and defaults."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chat_cli import apply_request_defaults, parse_chat_request


def test_parse_chat_request_detects_event_report_from_quarter_phrase():
    request = parse_chat_request("幫我分析台積電 2025Q1 法說會")

    assert request.mode == "event_report"
    assert request.stock == "2330.TW"
    assert request.stock_name == "台積電"
    assert request.event_key == "2025Q1"
    assert request.event_date == ""


def test_parse_chat_request_detects_heat_scan_and_phase():
    request = parse_chat_request("幫我做 2454.TW 2025-04-30 法說會事件後熱度分析")

    assert request.mode == "heat_scan"
    assert request.stock == "2454.TW"
    assert request.stock_name == "聯發科"
    assert request.event_date == "2025-04-30"
    assert request.phase == "post_event"


def test_parse_chat_request_extracts_event_study_dates():
    request = parse_chat_request("做台積電 2025-01-16, 2025-04-17 的 event study")

    assert request.mode == "event_study"
    assert request.stock == "2330.TW"
    assert request.event_dates == ["2025-01-16", "2025-04-17"]


def test_apply_request_defaults_uses_event_date_window_for_event_report():
    request = parse_chat_request("幫我分析 2330.TW 2025-04-17 法說會")

    apply_request_defaults(request)

    assert request.start_date == "2025-04-01"
    assert request.end_date == "2025-04-24"
