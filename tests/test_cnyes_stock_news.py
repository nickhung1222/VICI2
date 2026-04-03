"""Unit tests for the standalone Cnyes stock news fetcher."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.cnyes_stock_news import _build_article_url, fetch_cnyes_stock_news


def _ts(value: str) -> int:
    return int(__import__("datetime").datetime.fromisoformat(value).timestamp())


def _symbol_item(
    news_id: int,
    published_at: str,
    title: str,
    *,
    market: list[dict] | None = None,
    other_product: list[str] | None = None,
    summary: str = "",
    keyword_tags: list[str] | None = None,
) -> dict:
    return {
        "newsId": news_id,
        "title": title,
        "publishAt": _ts(published_at),
        "summary": summary,
        "keywordForTag": keyword_tags or [],
        "market": market or [],
        "otherProduct": other_product or [],
    }


class FakeResponse:
    def __init__(self, *, json_data=None, text="", status_code=200):
        self._json_data = json_data
        self.text = text
        self.status_code = status_code

    def json(self):
        if self._json_data is None:
            raise ValueError("No JSON payload")
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, handler):
        self._handler = handler
        self.calls: list[tuple[str, dict | None]] = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        return self._handler(url, params=params, headers=headers, timeout=timeout)

    def close(self):
        return None


def test_build_article_url_uses_news_cnyes_domain():
    assert _build_article_url("6406861") == "https://news.cnyes.com/news/id/6406861"


def test_fetch_cnyes_stock_news_stops_symbol_news_after_covering_start_date(monkeypatch):
    page_1 = {
        "items": {
            "total": 50,
            "last_page": 3,
            "data": [
                _symbol_item(101, "2026-04-03T15:40:02+08:00", "台積電法說會前瞻"),
                _symbol_item(102, "2026-04-02T17:42:55+08:00", "外資轉賣超386億元 自台積電提款"),
            ],
        }
    }
    page_2 = {
        "items": {
            "total": 50,
            "last_page": 3,
            "data": [
                _symbol_item(103, "2026-04-01T17:26:47+08:00", "台積電公告取得固定收益證券"),
                _symbol_item(104, "2026-03-31T17:11:22+08:00", "區間外舊聞"),
            ],
        }
    }
    fake_session = FakeSession(
        lambda url, params=None, **kwargs: (
            FakeResponse(json_data=page_1)
            if params and params.get("page") == 1
            else FakeResponse(json_data=page_2)
        )
    )
    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: fake_session)

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-01",
        date_to="2026-04-03",
        stock_name="台積電",
    )

    symbol_calls = [call for call in fake_session.calls if "symbolNews" in call[0]]
    assert len(symbol_calls) == 2
    assert payload["coverage"]["symbol_news_pages_fetched"] == 2
    assert [record["news_id"] for record in payload["records"]] == ["101", "102", "103"]


def test_fetch_cnyes_stock_news_infers_stock_name_from_company_page(monkeypatch):
    def handler(url, params=None, **kwargs):
        if "twstock/2330" in url:
            return FakeResponse(text="<html><head><title>台積電 2330 - 總覽 | 鉅亨網 - 台股</title></head></html>")
        if "symbolNews" in url:
            return FakeResponse(json_data={"items": {"total": 0, "last_page": 0, "data": []}})
        if "ess.api.cnyes.com" in url:
            return FakeResponse(json_data={"data": {"items": []}})
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: FakeSession(handler))

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-01",
        date_to="2026-04-03",
    )

    assert payload["stock_name"] == "台積電"
    assert payload["records"] == []
    assert "no_news_in_interval" in payload["data_gaps"]


def test_fetch_cnyes_stock_news_match_modes_filter_direct_tagged_and_broad(monkeypatch):
    payload_page = {
        "items": {
            "total": 3,
            "last_page": 1,
            "data": [
                _symbol_item(201, "2026-04-03T10:00:00+08:00", "台積電法說會重點"),
                _symbol_item(
                    202,
                    "2026-04-03T09:00:00+08:00",
                    "半導體市場觀察",
                    market=[{"code": "2330", "symbol": "TWS:2330:STOCK"}],
                ),
                _symbol_item(203, "2026-04-03T08:00:00+08:00", "完全不相關的文章"),
            ],
        }
    }
    monkeypatch.setattr(
        "tools.cnyes_stock_news.requests.Session",
        lambda: FakeSession(lambda url, params=None, **kwargs: FakeResponse(json_data=payload_page)),
    )

    strict_payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-03",
        date_to="2026-04-03",
        stock_name="台積電",
        match_mode="strict",
    )
    balanced_payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-03",
        date_to="2026-04-03",
        stock_name="台積電",
        match_mode="balanced",
    )
    broad_payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-03",
        date_to="2026-04-03",
        stock_name="台積電",
        match_mode="broad",
    )

    assert [record["news_id"] for record in strict_payload["records"]] == ["201"]
    assert [record["news_id"] for record in balanced_payload["records"]] == ["201", "202"]
    assert [record["news_id"] for record in broad_payload["records"]] == ["201", "202", "203"]


def test_fetch_cnyes_stock_news_merges_symbol_news_and_keyword_fallback_by_news_id(monkeypatch):
    symbol_page = {
        "items": {
            "total": 1,
            "last_page": 1,
            "data": [
                _symbol_item(
                    301,
                    "2025-05-12T10:00:00+08:00",
                    "半導體市場觀察",
                    market=[{"code": "2330", "symbol": "TWS:2330:STOCK"}],
                )
            ],
        }
    }
    keyword_page = {
        "data": {
            "items": [
                _symbol_item(301, "2025-05-12T10:00:00+08:00", "台積電供應鏈觀察", summary="台積電擴產"),
                _symbol_item(302, "2025-05-11T09:00:00+08:00", "台積電法說會摘要", summary="台積電展望"),
            ]
        }
    }

    def handler(url, params=None, **kwargs):
        if "symbolNews" in url:
            return FakeResponse(json_data=symbol_page)
        if "ess.api.cnyes.com" in url:
            return FakeResponse(json_data=keyword_page)
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: FakeSession(handler))

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2025-05-10",
        date_to="2025-05-15",
        stock_name="台積電",
        match_mode="balanced",
    )

    by_id = {record["news_id"]: record for record in payload["records"]}
    assert list(by_id) == ["301", "302"]
    assert by_id["301"]["source"] == "cnyes_symbol_news"
    assert by_id["301"]["relevance"] == "direct"
    assert "title_alias" in by_id["301"]["matched_by"]


def test_fetch_cnyes_stock_news_uses_article_verification_for_boundary_keyword_candidates(monkeypatch):
    symbol_page = {"items": {"total": 0, "last_page": 0, "data": []}}
    keyword_page = {
        "data": {
            "items": [
                _symbol_item(401, "2025-05-12T10:00:00+08:00", "供應鏈景氣更新", summary="需求變化"),
                _symbol_item(402, "2025-05-11T09:00:00+08:00", "完全不相關", summary="沒有股票別名"),
            ]
        }
    }

    def handler(url, params=None, **kwargs):
        if "symbolNews" in url:
            return FakeResponse(json_data=symbol_page)
        if "ess.api.cnyes.com" in url:
            return FakeResponse(json_data=keyword_page)
        if url.endswith("/401"):
            html = """
            <html><head>
            <link rel="canonical" href="https://news.cnyes.com/news/id/401"/>
            <meta name="description" content="台積電供應鏈需求持續增溫"/>
            </head><body>台積電供應鏈需求持續增溫</body></html>
            """
            return FakeResponse(text=html)
        if url.endswith("/402"):
            html = """
            <html><head>
            <link rel="canonical" href="https://news.cnyes.com/news/id/402"/>
            <meta name="description" content="這篇文章與目標股票無關"/>
            </head><body>完全無關內容</body></html>
            """
            return FakeResponse(text=html)
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: FakeSession(handler))

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2025-05-10",
        date_to="2025-05-15",
        stock_name="台積電",
        match_mode="balanced",
    )

    assert [record["news_id"] for record in payload["records"]] == ["401"]
    assert payload["records"][0]["source"] == "cnyes_keyword_fallback"
    assert payload["records"][0]["matched_by"] == ["article_page_alias"]


def test_fetch_cnyes_stock_news_returns_data_gaps_on_request_failures(monkeypatch):
    def handler(url, params=None, **kwargs):
        if "symbolNews" in url or "ess.api.cnyes.com" in url:
            raise requests.RequestException("boom")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: FakeSession(handler))

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2026-04-01",
        date_to="2026-04-03",
        stock_name="台積電",
    )

    assert payload["records"] == []
    assert "symbol_news_request_failed" in payload["data_gaps"]
    assert "keyword_fallback_request_failed" in payload["data_gaps"]
    assert "no_news_in_interval" not in payload["data_gaps"]


def test_fetch_cnyes_stock_news_switches_from_slow_name_query_to_code_query(monkeypatch):
    symbol_page = {"items": {"total": 0, "last_page": 0, "data": []}}
    slow_name_page = {
        "data": {
            "items": [
                _symbol_item(501, "2026-03-20T10:00:00+08:00", "產業觀察", summary="沒有別名"),
                _symbol_item(502, "2026-03-19T10:00:00+08:00", "市場焦點", summary="沒有別名"),
            ]
            * 10
        }
    }
    code_page = {
        "data": {
            "items": [
                _symbol_item(503, "2025-05-12T10:00:00+08:00", "台積電法說會摘要", summary="台積電展望")
            ]
        }
    }
    fake_session = FakeSession(
        lambda url, params=None, **kwargs: (
            FakeResponse(json_data=symbol_page)
            if "symbolNews" in url
            else FakeResponse(json_data=slow_name_page)
            if params and params.get("q") == "台積電"
            else FakeResponse(json_data=code_page)
        )
    )
    monkeypatch.setattr("tools.cnyes_stock_news.requests.Session", lambda: fake_session)

    payload = fetch_cnyes_stock_news(
        stock="2330",
        date_from="2025-05-10",
        date_to="2025-05-15",
        stock_name="台積電",
        match_mode="balanced",
    )

    name_query_calls = [
        call for call in fake_session.calls if "ess.api.cnyes.com" in call[0] and call[1] and call[1].get("q") == "台積電"
    ]
    code_query_calls = [
        call for call in fake_session.calls if "ess.api.cnyes.com" in call[0] and call[1] and call[1].get("q") == "2330"
    ]

    assert len(name_query_calls) == 10
    assert len(code_query_calls) == 1
    assert [record["news_id"] for record in payload["records"]] == ["503"]
