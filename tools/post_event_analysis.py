"""Post-event narrative analysis for earnings-call related coverage."""

from __future__ import annotations

import os
from collections import Counter
from typing import Any


def build_post_event_analysis(
    *,
    event_collection: dict[str, Any],
    source_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a simple post-event narrative report from related media records."""
    records = _select_related_post_event_records(event_collection, source_records=source_records)
    if not records:
        return {
            "mode": "not_available",
            "used_record_count": 0,
            "report": "",
            "records": [],
            "data_gaps": ["post_event_related_records_missing"],
        }

    if os.environ.get("GEMINI_API_KEY"):
        report = _generate_ai_report(event_collection=event_collection, records=records)
        if report:
            return {
                "mode": "gemini_simple_report",
                "used_record_count": len(records),
                "report": report,
                "records": records,
                "data_gaps": [],
            }

    return {
        "mode": "rule_based_fallback",
        "used_record_count": len(records),
        "report": _build_rule_based_report(records),
        "records": records,
        "data_gaps": ["gemini_post_event_analysis_unavailable"],
    }


def _select_related_post_event_records(
    event_collection: dict[str, Any],
    *,
    source_records: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    records = source_records if source_records is not None else (
        event_collection.get("records", []) if isinstance(event_collection, dict) else []
    )
    selected: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if str(record.get("event_phase", "")).strip() not in {"event_day", "post_event"}:
            continue
        if not record.get("is_post_event_earnings_related", False):
            continue
        selected.append(record)
    return selected


def _generate_ai_report(
    *,
    event_collection: dict[str, Any],
    records: list[dict[str, Any]],
) -> str:
    try:
        import google.genai as genai
    except Exception:
        return ""

    stock = ((event_collection or {}).get("query") or {}).get("stock") or {}
    event_type = ((event_collection or {}).get("query") or {}).get("event_type") or ""
    event_date = ((event_collection or {}).get("query") or {}).get("event_date") or ""
    stock_label = stock.get("name") or stock.get("code") or stock.get("symbol") or "該公司"

    prompt = _build_ai_prompt(
        stock_label=str(stock_label),
        event_type=str(event_type),
        event_date=str(event_date),
        records=records,
    )

    try:
        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        model_id = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
        )
    except Exception:
        return ""

    text = getattr(response, "text", "") or ""
    return text.strip()


def _build_ai_prompt(
    *,
    stock_label: str,
    event_type: str,
    event_date: str,
    records: list[dict[str, Any]],
) -> str:
    lines = [
        f"你是一位台股研究助理。請根據以下 {stock_label} {event_type} 事件後相關新聞，",
        "用繁體中文整理一份簡單、可讀、不要過度格式化的短報告。",
        "請只根據提供內容，不要補外部資訊，不要虛構數字或結論。",
        "請用以下四個小節，各寫 2 到 4 句：",
        "1. 財務相關資訊",
        "2. 管理階層展望",
        "3. 關注領域重點",
        "4. 新聞內容分歧",
        "如果某一節資訊不足，直接寫資訊不足，不要硬推論。",
        f"事件日期：{event_date}",
        "",
        "新聞材料：",
    ]
    for idx, record in enumerate(records, start=1):
        lines.append(
            (
                f"[{idx}] 日期={record.get('article_date', '')} | 來源={record.get('source_name', '')} | "
                f"類型={record.get('article_type', '')} | 標題={record.get('headline', '')} | "
                f"摘要={record.get('summary', '')}"
            )
        )
    return "\n".join(lines)


def _build_rule_based_report(records: list[dict[str, Any]]) -> str:
    article_types = Counter(
        str(record.get("article_type", "")).strip()
        for record in records
        if str(record.get("article_type", "")).strip()
    )
    headlines = [str(record.get("headline", "")).strip() for record in records if str(record.get("headline", "")).strip()]
    joined = " ".join(headlines)

    financial_topics = []
    for keyword in ("毛利率", "營收", "EPS", "資本支出", "capex", "財測", "指引"):
        if keyword.lower() in joined.lower() and keyword not in financial_topics:
            financial_topics.append(keyword)

    outlook_topics = []
    for keyword in ("展望", "AI", "需求", "先進製程", "海外擴產"):
        if keyword in joined and keyword not in outlook_topics:
            outlook_topics.append(keyword)

    focus_topics = []
    for keyword in ("毛利率", "資本支出", "法人", "外資", "AI", "需求", "台股"):
        if keyword in joined and keyword not in focus_topics:
            focus_topics.append(keyword)

    disagreement = "目前相關文章數有限，初步看法以毛利率、資本支出與法人反應為主，不同文章的著眼點仍有差異。"
    if article_types:
        disagreement = (
            "文章解讀角度不完全一致，"
            f"目前可見的類型包含 {', '.join(list(article_types.keys())[:3])}。"
        )

    sections = [
        "財務相關資訊",
        (
            "目前相關新聞主要圍繞 "
            + (", ".join(financial_topics) if financial_topics else "財務數字與指引")
            + " 進行解讀。"
        ),
        "",
        "管理階層展望",
        (
            "媒體轉述的重點較集中在 "
            + (", ".join(outlook_topics) if outlook_topics else "後續展望與需求方向")
            + "。"
        ),
        "",
        "關注領域重點",
        (
            "事件後的市場關注點以 "
            + (", ".join(focus_topics[:4]) if focus_topics else "法說後解讀與法人反應")
            + " 為主。"
        ),
        "",
        "新聞內容分歧",
        disagreement,
    ]
    return "\n".join(sections).strip()
