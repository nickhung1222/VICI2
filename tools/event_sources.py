"""Official event source adapters."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

from tools.schemas import classify_event_phase, infer_record_flags

_MOPS_OV_EVENT_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb07_1"
_MOPS_OV_PAGE_URL = "https://mopsov.twse.com.tw/mops/web/t100sb07_1"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
}


def collect_official_event_records(
    *,
    stock_code: str,
    stock_name: str,
    symbol: str,
    event_type: str,
    start_date: str,
    end_date: str,
    event_date: str = "",
    event_key: str = "",
) -> dict[str, Any]:
    """Collect official-source records for supported event types."""
    if event_type != "法說會" or not stock_code:
        return {"records": [], "data_gaps": []}

    record = fetch_mops_investor_conference(
        stock_code=stock_code,
        stock_name=stock_name,
        symbol=symbol,
        event_date=event_date,
        event_key=event_key,
    )
    if not record:
        return {"records": [], "data_gaps": ["mops_official_record_unavailable"]}

    article_date = record.get("article_date", "")
    if start_date and article_date and article_date < start_date:
        return {"records": [], "data_gaps": ["mops_record_outside_requested_range"]}
    if end_date and article_date and article_date > end_date:
        return {"records": [], "data_gaps": ["mops_record_outside_requested_range"]}

    return {"records": [record], "data_gaps": []}


def fetch_mops_investor_conference(
    *,
    stock_code: str,
    stock_name: str,
    symbol: str,
    event_date: str = "",
    event_key: str = "",
) -> dict[str, Any] | None:
    """Fetch the latest investor-conference record from MOPS OV."""
    response = requests.post(
        _MOPS_OV_EVENT_URL,
        data={
            "step": "1",
            "firstin": "true",
            "off": "1",
            "queryName": "co_id",
            "inpuType": "co_id",
            "TYPEK": "all",
            "co_id": stock_code,
        },
        headers=_HEADERS,
        timeout=5,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    table = soup.select_one("table.hasBorder")
    if table is None:
        return None

    raw_date = _extract_label_value(table, "召開法人說明會日期")
    parsed_date = _parse_mops_date(raw_date)
    if not parsed_date:
        return None

    location = _extract_label_value(table, "召開法人說明會地點")
    summary = _extract_label_value(table, "法人說明會擇要訊息")
    website_url = ""
    first_link = table.select_one("a[href]")
    if first_link is not None:
        website_url = first_link.get("href", "").strip()

    event_phase = classify_event_phase(article_date=parsed_date, event_date=event_date or parsed_date)
    flags = infer_record_flags(
        event_phase=event_phase,
        article_type="官方公告",
        source_type="official",
    )

    summary_parts = [part for part in (location, summary) if part]
    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "symbol": symbol,
        "event_type": "法說會",
        "event_date": event_date or parsed_date,
        "event_key": event_key,
        "event_phase": event_phase,
        "article_date": parsed_date,
        "article_type": "官方公告",
        "source_type": "official",
        "source_name": "公開資訊觀測站",
        "source_url": _MOPS_OV_PAGE_URL,
        "headline": f"{event_key or parsed_date} 法說會官方公告",
        "summary": "；".join(summary_parts) if summary_parts else "法人說明會官方公告",
        "language": "zh-TW",
        "matched_query": "",
        "official_page_url": website_url,
        **flags,
    }


def _extract_label_value(table: BeautifulSoup, label: str) -> str:
    """Extract a value cell by its bold label text."""
    bold = table.find("b", string=lambda text: isinstance(text, str) and label in text)
    if bold is None:
        return ""
    row = bold.find_parent("tr")
    if row is None:
        return ""
    cells = row.find_all("td")
    if not cells:
        return ""
    return cells[-1].get_text(" ", strip=True)


def _parse_mops_date(raw: str) -> str:
    """Convert ROC date strings like 115/04/16 to YYYY-MM-DD."""
    match = re.search(r"(\d{2,3})/(\d{2})/(\d{2})", raw)
    if not match:
        return ""
    year, month, day = match.groups()
    western_year = int(year) + 1911 if int(year) < 1911 else int(year)
    return datetime(western_year, int(month), int(day)).strftime("%Y-%m-%d")
