"""Save reports and assemble event-report payloads."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


def save_report(content: str, topic: str) -> str:
    """Save a Markdown report to outputs/reports/.

    Args:
        content: Full Markdown content
        topic: Topic name used in filename

    Returns:
        Absolute path to the saved file
    """
    output_dir = Path(__file__).parent.parent / "outputs" / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_topic = _sanitize_filename(topic)
    filename = f"{safe_topic}_{timestamp}.md"
    filepath = output_dir / filename

    filepath.write_text(content, encoding="utf-8")
    return str(filepath)


def save_event_record(record: dict, topic: str) -> str:
    """Save structured event study JSON to outputs/events/.

    Args:
        record: Dict containing event study results, sentiment data, etc.
        topic: Topic name used in filename

    Returns:
        Absolute path to the saved file
    """
    output_dir = Path(__file__).parent.parent / "outputs" / "events"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_topic = _sanitize_filename(topic)
    filename = f"{safe_topic}_{timestamp}.json"
    filepath = output_dir / filename

    filepath.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(filepath)


def build_event_report_payload(
    event_collection: dict[str, Any],
    heat_analysis: Optional[dict[str, Any]] = None,
    post_event_analysis: Optional[dict[str, Any]] = None,
    event_study: Optional[dict[str, Any]] = None,
    generated_at: Optional[str] = None,
    title: Optional[str] = None,
) -> dict[str, Any]:
    """Assemble a JSON-friendly event report payload from structured inputs."""
    event_collection = event_collection or {}
    heat_analysis = heat_analysis or {}
    post_event_analysis = post_event_analysis or {}
    event_study = event_study or {}

    metadata = _build_report_metadata(
        event_collection=event_collection,
        generated_at=generated_at,
        title=title,
    )
    sections = {
        "event_summary": _build_event_summary_section(event_collection, heat_analysis),
        "pre_event_narratives": _build_narrative_section(event_collection, {"pre_event"}),
        "post_event_narratives": _build_narrative_section(
            event_collection,
            {"event_day", "post_event"},
            post_event_analysis=post_event_analysis,
        ),
        "narrative_shift": _build_narrative_shift_section(event_collection),
        "heat_analysis": _build_heat_section(heat_analysis),
        "official_sources": _build_official_sources_section(event_collection),
        "earnings_highlights": _build_earnings_highlights_section(event_collection),
        "management_tone": _build_management_tone_section(event_collection),
        "qa_summary": _build_qa_summary_section(event_collection),
        "event_study": _build_event_study_section(event_study),
        "data_gaps": _collect_data_gaps(event_collection, heat_analysis, event_study),
        "todo_items": _build_todo_section(event_collection),
    }
    markdown = render_event_report_markdown(metadata=metadata, sections=sections)

    return {
        "report_type": "event_report",
        "metadata": metadata,
        "event_collection": event_collection,
        "heat_analysis": heat_analysis,
        "post_event_analysis": post_event_analysis,
        "event_study": event_study,
        "sections": sections,
        "data_gaps": sections["data_gaps"],
        "markdown": markdown,
    }


def render_event_report_markdown(
    metadata: dict[str, Any],
    sections: dict[str, Any],
) -> str:
    """Render a Markdown report from a structured payload."""
    lines: list[str] = []
    title = metadata.get("title", "事件")
    lines.append(f"# {title} 事件報告")
    lines.append("")
    lines.append(f"**股票代碼**：{metadata.get('stock_code', '-')}")
    lines.append(f"**股票名稱**：{metadata.get('stock_name', '-')}")
    lines.append(f"**事件類型**：{metadata.get('event_type', '-')}")
    lines.append(f"**事件日期**：{metadata.get('event_date', '-')}")
    lines.append(f"**事件鍵**：{metadata.get('event_key', '-')}")
    lines.append(f"**分析日期**：{metadata.get('generated_at', '-')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    lines.extend(_render_event_summary_section(sections.get("event_summary", {})))
    lines.extend(_render_narrative_block("二、市場事件前敘事", sections.get("pre_event_narratives", {}), "尚未提供事件前敘事資料。"))
    lines.extend(_render_narrative_block("三、市場事件後敘事", sections.get("post_event_narratives", {}), "尚未提供事件後敘事資料。"))
    lines.extend(_render_narrative_shift_block(sections.get("narrative_shift", {})))
    lines.extend(_render_heat_block(sections.get("heat_analysis", {}), title="五、熱度分析"))

    lines.append("")
    lines.append("*報告由 VICI2 台灣新聞事件研究 Agent 自動生成*")
    lines.append(f"*生成時間：{metadata.get('generated_at', '-')}*")
    return "\n".join(lines).strip() + "\n"


def _sanitize_filename(name: str) -> str:
    """Convert a topic string to a safe filename."""
    # Replace Chinese and special chars with underscore
    safe = ""
    for ch in name:
        if ch.isalnum() or ch in "-_":
            safe += ch
        elif ch in " /\\:*?\"<>|":
            safe += "_"
        else:
            safe += "_"
    return safe[:60].strip("_") or "report"


def _build_report_metadata(
    event_collection: dict[str, Any],
    generated_at: Optional[str],
    title: Optional[str],
) -> dict[str, Any]:
    query = event_collection.get("query", {}) if isinstance(event_collection, dict) else {}
    stock = query.get("stock", {}) if isinstance(query, dict) else {}
    generated_value = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    event_type = query.get("event_type", "") if isinstance(query, dict) else ""
    event_date = query.get("event_date", "") if isinstance(query, dict) else ""
    event_key = query.get("event_key", "") if isinstance(query, dict) else ""
    stock_code = stock.get("code", "") if isinstance(stock, dict) else ""
    stock_name = stock.get("name", "") if isinstance(stock, dict) else ""

    return {
        "title": title or stock_name or stock_code or event_type or "事件",
        "stock_code": stock_code or "-",
        "stock_name": stock_name or "-",
        "event_type": event_type or "-",
        "event_date": event_date or "-",
        "event_key": event_key or "-",
        "generated_at": generated_value,
    }


def _build_event_summary_section(event_collection: dict[str, Any], heat_analysis: dict[str, Any]) -> dict[str, Any]:
    query = event_collection.get("query", {}) if isinstance(event_collection, dict) else {}
    stock = query.get("stock", {}) if isinstance(query, dict) else {}
    collection_plan = event_collection.get("collection_plan", {}) if isinstance(event_collection, dict) else {}
    transcript_artifact = _select_primary_transcript_artifact(event_collection)

    return {
        "stock_code": stock.get("code", ""),
        "stock_name": stock.get("name", ""),
        "symbol": stock.get("symbol", ""),
        "event_type": query.get("event_type", ""),
        "event_date": query.get("event_date", ""),
        "event_key": query.get("event_key", ""),
        "data_coverage_note": _build_data_coverage_note(query.get("event_date", "")),
        "time_range": query.get("time_range", {}),
        "record_count": event_collection.get("record_count", 0),
        "record_breakdown": event_collection.get("record_breakdown", {}),
        "comparison_strategy": collection_plan.get("comparison_strategy", {}),
        "sources": collection_plan.get("sources", []),
        "source_policy": collection_plan.get("source_policy", ""),
        "primary_source": collection_plan.get("primary_source", ""),
        "heat_mode": heat_analysis.get("comparison_mode", ""),
        "official_artifact_count": len(event_collection.get("official_artifacts", [])),
        "transcript_available": bool(transcript_artifact),
        "transcript_source_name": transcript_artifact.get("source_name", "") if transcript_artifact else "",
        "transcript_url": transcript_artifact.get("url", "") if transcript_artifact else "",
        "transcript_retrieval_status": transcript_artifact.get("retrieval_status", "") if transcript_artifact else "",
        "transcript_excerpt": transcript_artifact.get("excerpt", "") if transcript_artifact else "",
        "todo_count": len(event_collection.get("todo_items", [])),
        "pre_event_record_count": _count_records_by_phase(event_collection, {"pre_event"}),
        "post_event_record_count": _count_records_by_phase(event_collection, {"event_day", "post_event"}),
    }


def _build_data_coverage_note(event_date: Any) -> str:
    base_note = "目前新聞資料來源實際可用區間主要自 2024-10 起；更早日期的法說會可能僅產生空報告或資料不足結果。"
    try:
        if str(event_date).strip():
            parsed = datetime.strptime(str(event_date).strip(), "%Y-%m-%d")
            if parsed < datetime(2024, 10, 1):
                return f"{base_note} 本次事件日期早於 2024-10-01，請特別留意資料覆蓋限制。"
    except ValueError:
        pass
    return base_note


def _build_official_sources_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    artifacts = event_collection.get("official_artifacts", []) if isinstance(event_collection, dict) else []
    return {
        "rows": [artifact for artifact in artifacts if isinstance(artifact, dict)],
    }


def _select_primary_transcript_artifact(event_collection: dict[str, Any]) -> dict[str, Any]:
    artifacts = event_collection.get("official_artifacts", []) if isinstance(event_collection, dict) else []
    transcript_candidates = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict) and artifact.get("artifact_type") == "transcript"
    ]
    if not transcript_candidates:
        return {}

    transcript_candidates.sort(
        key=lambda artifact: (
            0 if artifact.get("retrieval_status") == "ok" else 1,
            0 if artifact.get("excerpt") else 1,
            str(artifact.get("source_name", "")),
        )
    )
    return transcript_candidates[0]


def _build_earnings_highlights_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    digest = event_collection.get("earnings_digest", {}) if isinstance(event_collection, dict) else {}
    return {
        "financial_snapshot": dict(digest.get("financial_snapshot", {})) if isinstance(digest, dict) else {},
        "official_takeaways": list(digest.get("official_takeaways", [])) if isinstance(digest, dict) else [],
        "data_gaps": list(digest.get("data_gaps", [])) if isinstance(digest, dict) else [],
    }


def _build_management_tone_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    digest = event_collection.get("earnings_digest", {}) if isinstance(event_collection, dict) else {}
    return dict(digest.get("management_tone", {})) if isinstance(digest, dict) else {}


def _build_qa_summary_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    digest = event_collection.get("earnings_digest", {}) if isinstance(event_collection, dict) else {}
    data_gaps = []
    if isinstance(digest, dict):
        digest_gaps = digest.get("data_gaps", [])
        if isinstance(digest_gaps, list):
            data_gaps = [gap for gap in digest_gaps if gap == "qa_not_available"]
    return {
        "rows": list(digest.get("qa_topics", [])) if isinstance(digest, dict) else [],
        "data_gaps": data_gaps,
    }


def _build_narrative_section(
    event_collection: dict[str, Any],
    phases: set[str],
    post_event_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(event_collection, dict):
        return {"rows": [], "summary": "", "theme_labels": [], "data_gaps": []}

    query = event_collection.get("query", {}) if isinstance(event_collection, dict) else {}
    event_type = str(query.get("event_type", "")).strip()
    should_filter_post_event = event_type == "法說會" and bool(phases & {"event_day", "post_event"})
    override_records = []
    if should_filter_post_event and isinstance(post_event_analysis, dict):
        override_records = post_event_analysis.get("records", []) or []
    candidate_records: list[dict[str, Any]] = []
    raw_count = 0
    filtered_out_count = 0
    rows: list[dict[str, Any]] = []
    theme_labels: list[str] = []
    source_records = override_records if override_records else event_collection.get("records", [])
    for record in source_records:
        if not isinstance(record, dict):
            continue
        if str(record.get("event_phase", "")).strip() not in phases:
            continue
        raw_count += 1
        if should_filter_post_event and not record.get("is_post_event_earnings_related", False):
            filtered_out_count += 1
            continue
        candidate_records.append(record)

    for record in candidate_records:
        rows.append(
            {
                "date": record.get("article_date") or record.get("published_at", ""),
                "headline": record.get("headline", ""),
                "article_type": record.get("article_type", ""),
                "source_name": record.get("source_name") or record.get("source", ""),
                "summary": record.get("summary", ""),
                "post_event_relevance_score": record.get("post_event_relevance_score"),
            }
        )
        article_type = str(record.get("article_type", "")).strip()
        if article_type and article_type not in theme_labels:
            theme_labels.append(article_type)

    rows.sort(key=lambda item: (str(item.get("date", "")), str(item.get("headline", ""))))
    data_gaps: list[str] = []
    if should_filter_post_event and filtered_out_count > 0:
        data_gaps.append(f"post_event_noise_filtered:{filtered_out_count}")
    return {
        "rows": rows,
        "summary": _summarize_narratives(rows),
        "theme_labels": theme_labels[:5],
        "data_gaps": data_gaps,
        "raw_count": raw_count,
        "selected_count": len(rows),
        "analysis_report": (
            str((post_event_analysis or {}).get("report", "")).strip()
            if should_filter_post_event
            else ""
        ),
        "analysis_mode": (
            str((post_event_analysis or {}).get("mode", "")).strip()
            if should_filter_post_event
            else ""
        ),
        "analysis_used_record_count": (
            (post_event_analysis or {}).get("used_record_count")
            if should_filter_post_event
            else None
        ),
    }


def _build_narrative_shift_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    pre_section = _build_narrative_section(event_collection, {"pre_event"})
    post_section = _build_narrative_section(event_collection, {"event_day", "post_event"})
    pre_types = pre_section.get("theme_labels", [])
    post_types = post_section.get("theme_labels", [])

    summary_parts = [
        f"事件前敘事筆數 {len(pre_section.get('rows', []))}",
        f"事件後敘事筆數 {len(post_section.get('rows', []))}",
    ]
    if pre_types:
        summary_parts.append(f"事件前主題偏向 {', '.join(pre_types[:3])}")
    if post_types:
        summary_parts.append(f"事件後主題偏向 {', '.join(post_types[:3])}")
    if not post_section.get("rows"):
        summary_parts.append("目前仍以事件前敘事為主，尚缺事件後解讀。")

    appeared_after = [label for label in post_types if label not in pre_types]
    faded_after = [label for label in pre_types if label not in post_types]
    return {
        "summary": "；".join(summary_parts),
        "appeared_after_event": appeared_after[:5],
        "faded_after_event": faded_after[:5],
    }


def _build_heat_section(heat_analysis: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(heat_analysis, dict) or not heat_analysis:
        return {}

    section = dict(heat_analysis)
    section.setdefault("data_gaps", [])
    return section


def _build_event_study_section(event_study: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event_study, dict) or not event_study:
        return {}

    section = dict(event_study)
    section.setdefault("data_gaps", [])
    return section


def _build_todo_section(event_collection: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(event_collection, dict):
        return []
    todo_items = event_collection.get("todo_items", [])
    if not isinstance(todo_items, list):
        return []
    return [item for item in todo_items if isinstance(item, dict)]


def _collect_data_gaps(
    event_collection: dict[str, Any],
    heat_analysis: dict[str, Any],
    event_study: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    for source in (event_collection, heat_analysis, event_study):
        if isinstance(source, dict):
            source_gaps = source.get("data_gaps", [])
            if isinstance(source_gaps, list):
                gaps.extend(str(item) for item in source_gaps if item)

    if not heat_analysis:
        gaps.append("heat_analysis_missing")

    deduped: list[str] = []
    seen: set[str] = set()
    for gap in gaps:
        if gap in seen:
            continue
        seen.add(gap)
        deduped.append(gap)
    return deduped


def _render_event_summary_section(section: dict[str, Any]) -> list[str]:
    lines = ["## 一、事件摘要", ""]
    if not section:
        lines.append("尚未提供事件摘要資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    lines.append(f"- **股票代碼**：{_format_value(section.get('stock_code'))}")
    lines.append(f"- **股票名稱**：{_format_value(section.get('stock_name'))}")
    lines.append(f"- **代號**：{_format_value(section.get('symbol'))}")
    lines.append(f"- **事件類型**：{_format_value(section.get('event_type'))}")
    lines.append(f"- **事件日期**：{_format_value(section.get('event_date'))}")
    lines.append(f"- **事件鍵**：{_format_value(section.get('event_key'))}")
    lines.append(f"- **記錄數**：{_format_value(section.get('record_count'))}")
    lines.append(f"- **事件前敘事筆數**：{_format_value(section.get('pre_event_record_count'))}")
    lines.append(f"- **事件後敘事筆數**：{_format_value(section.get('post_event_record_count'))}")
    lines.append(f"- **官方來源數**：{_format_value(section.get('official_artifact_count'))}")
    lines.append(f"- **逐字稿**：{'有' if section.get('transcript_available') else '無'}")
    if section.get("transcript_available"):
        lines.append(f"- **逐字稿來源**：{_format_value(section.get('transcript_source_name'))}")
        lines.append(f"- **逐字稿狀態**：{_format_value(section.get('transcript_retrieval_status'))}")
        lines.append(f"- **逐字稿連結**：{_format_value(section.get('transcript_url'))}")
        if section.get("transcript_excerpt"):
            lines.append(f"- **逐字稿摘錄**：{_format_value(section.get('transcript_excerpt'))}")
    lines.append(f"- **待辦數**：{_format_value(section.get('todo_count'))}")
    lines.append(f"- **比較模式**：{_format_value(section.get('comparison_strategy', {}).get('comparison_mode'))}")
    lines.append(f"- **資料來源**：{_format_value(section.get('sources'))}")
    lines.append(f"- **資料覆蓋說明**：{_format_value(section.get('data_coverage_note'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_official_sources_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 六、官方來源清單", ""]
    rows = section.get("rows", []) if isinstance(section, dict) else []
    if rows:
        lines.append(_render_metric_table(rows, ["類型", "來源", "URL", "狀態", "摘錄"]))
        lines.append("")
    else:
        lines.append("尚未提供官方來源資料。")
        lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_earnings_highlights_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 七、法說重點", ""]
    financial_snapshot = section.get("financial_snapshot", {}) if isinstance(section, dict) else {}
    official_takeaways = section.get("official_takeaways", []) if isinstance(section, dict) else []

    if financial_snapshot:
        rows = [
            {
                "metric_name": metric,
                "content": _format_verified_metric(metric_payload),
                "source_name": metric_payload.get("source_name", ""),
            }
            for metric, metric_payload in financial_snapshot.items()
            if isinstance(metric_payload, dict)
        ]
        lines.append(_render_metric_table(rows, ["指標", "內容", "來源"]))
        lines.append("")
    else:
        lines.append("尚未提供已驗證的官方財務重點。")
        lines.append("")

    if official_takeaways:
        lines.append("- **官方摘要重點**：")
        for takeaway in official_takeaways:
            lines.append(f"  - {_format_value(takeaway)}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_management_tone_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 八、管理層態度", ""]
    if not section:
        lines.append("尚未提供管理層態度資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    lines.append(f"- **標籤**：{_format_value(section.get('label'))}")
    lines.append(f"- **驗證狀態**：{_format_value(section.get('validation_status'))}")
    evidence = section.get("evidence", [])
    if evidence:
        lines.append("- **證據句**：")
        for item in evidence:
            lines.append(
                f"  - {_format_value(item.get('excerpt'))} "
                f"({_format_value(item.get('source_artifact_type'))}, {_format_value(item.get('source_ref'))})"
            )
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_qa_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 九、Q&A 摘要", ""]
    rows = section.get("rows", []) if isinstance(section, dict) else []
    if rows:
        lines.append(_render_metric_table(rows, ["主題", "問題", "回答", "來源"]))
        lines.append("")
    else:
        lines.append("尚未提供已驗證的 Q&A 摘要。")
        lines.append("")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_narrative_block(title: str, section: dict[str, Any], empty_message: str) -> list[str]:
    lines = [f"## {title}", ""]
    rows = section.get("rows", []) if isinstance(section, dict) else []
    summary = section.get("summary", "") if isinstance(section, dict) else ""
    theme_labels = section.get("theme_labels", []) if isinstance(section, dict) else []
    analysis_report = section.get("analysis_report", "") if isinstance(section, dict) else ""
    analysis_mode = section.get("analysis_mode", "") if isinstance(section, dict) else ""
    analysis_used_record_count = section.get("analysis_used_record_count") if isinstance(section, dict) else None

    if rows:
        lines.append(_render_metric_table(rows, ["日期", "標題", "類型", "來源", "摘要"]))
        lines.append("")
    else:
        lines.append(empty_message)
        lines.append("")

    if summary:
        lines.append(f"- **摘要**：{summary}")
    if theme_labels:
        lines.append(f"- **主要敘事類型**：{_format_value(theme_labels)}")
    if analysis_report:
        if analysis_mode:
            lines.append(f"- **整理模式**：{_format_value(analysis_mode)}")
        if analysis_used_record_count is not None:
            lines.append(f"- **整理使用文章數**：{_format_value(analysis_used_record_count)}")
        lines.append("- **重點整理**：")
        for paragraph in str(analysis_report).splitlines():
            paragraph = paragraph.strip()
            if paragraph:
                lines.append(f"  - {paragraph}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_narrative_shift_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 四、前後敘事轉折", ""]
    if not section:
        lines.append("尚未提供前後敘事轉折資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    lines.append(f"- **摘要**：{_format_value(section.get('summary'))}")
    lines.append(f"- **事件後新增主題**：{_format_value(section.get('appeared_after_event'))}")
    lines.append(f"- **事件後淡出主題**：{_format_value(section.get('faded_after_event'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_heat_block(section: dict[str, Any], title: str = "熱度分析") -> list[str]:
    lines = [f"## {title}", ""]
    if not section:
        lines.append("尚未提供熱度分析資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    lines.append(f"- **比較模式**：{_format_value(section.get('comparison_mode'))}")
    lines.append(f"- **事件鍵**：{_format_value(section.get('event_key'))}")
    lines.append(f"- **對照事件鍵**：{_format_value(section.get('comparison_event_key'))}")
    lines.append(f"- **Heat 版本**：{_format_value(section.get('heat_version'))}")
    lines.append(f"- **請求 phase**：{_format_value(section.get('requested_phase'))}")
    available_scans = section.get("available_heat_scans", [])
    if available_scans:
        lines.append(f"- **已輸出 heat scan**：{_format_value(available_scans)}")
    if section.get("comparison_basis"):
        lines.append(f"- **比較基準**：{_format_value(section.get('comparison_basis'))}")
    pre_event_scan = section.get("pre_event_heat_scan")
    post_event_scan = section.get("post_event_heat_scan")
    if pre_event_scan or post_event_scan:
        if pre_event_scan:
            lines.extend(_render_phase_heat_scan_block("事件前 heat scan", pre_event_scan))
        if post_event_scan:
            lines.extend(_render_phase_heat_scan_block("事件後 heat scan", post_event_scan))
    else:
        panels = section.get("panels", [])
        if isinstance(panels, list) and panels:
            lines.append("")
            lines.append(_render_metric_table(panels, ["Panel", "Current", "Comparison", "Delta", "Status", "Summary"]))
            lines.append("")
            interpretations = section.get("panel_interpretation", [])
            if interpretations:
                lines.append("- **解讀**：")
                for item in interpretations:
                    lines.append(f"  - {_format_value(item)}")
        else:
            lines.append(f"- **目前窗口總量**：{_format_value(section.get('current_window_total'))}")
            lines.append(f"- **對照值**：{_format_value(section.get('comparison_value'))}")
            lines.append(f"- **熱度比**：{_format_value(section.get('news_heat_ratio'))}")
            lines.append(f"- **熱度標籤**：{_format_value(section.get('news_heat_label'))}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_phase_heat_scan_block(title: str, section: dict[str, Any]) -> list[str]:
    lines = [f"### {title}"]
    lines.append(f"- **窗口**：{_format_value(section.get('current_window'))}")
    lines.append(f"- **目前筆數**：{_format_value(section.get('current_record_count'))}")
    lines.append(f"- **對照窗口**：{_format_value(section.get('comparison_window'))}")
    lines.append(f"- **對照筆數**：{_format_value(section.get('comparison_record_count'))}")
    lines.append(f"- **比較基準**：{_format_value(section.get('comparison_basis'))}")
    lines.append(f"- **熱度比**：{_format_value(section.get('news_heat_ratio'))}")
    lines.append(f"- **熱度標籤**：{_format_value(section.get('news_heat_label'))}")
    panels = section.get("panels", [])
    if isinstance(panels, list) and panels:
        lines.append("")
        lines.append(_render_metric_table(panels, ["Panel", "Current", "Comparison", "Delta", "Status", "Summary"]))
        lines.append("")
    interpretations = section.get("panel_interpretation", [])
    if interpretations:
        lines.append("- **解讀**：")
        for item in interpretations:
            lines.append(f"  - {_format_value(item)}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    return lines


def _render_event_study_block(section: dict[str, Any], title: str = "事件研究（可選）") -> list[str]:
    lines = [f"## {title}", ""]
    if not section:
        lines.append("尚未提供事件研究資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    if section.get("event_date"):
        lines.append(f"- **原始事件日**：{_format_value(section.get('event_date'))}")
    if section.get("reaction_date"):
        lines.append(f"- **市場反應日（t=0）**：{_format_value(section.get('reaction_date'))}")
    if section.get("summary"):
        lines.append(f"- **摘要**：{_format_value(section.get('summary'))}")
    if section.get("n_events") is not None:
        lines.append(f"- **有效事件數**：{_format_value(section.get('n_events'))}")
    if section.get("n_skipped") is not None:
        lines.append(f"- **跳過事件數**：{_format_value(section.get('n_skipped'))}")
    if section.get("reaction_shift_trading_days") is not None:
        lines.append(
            f"- **t=0 位移交易日數**：{_format_value(section.get('reaction_shift_trading_days'))}"
        )
    if section.get("data_window"):
        lines.append(f"- **股價資料窗**：{_format_value(section.get('data_window'))}")
    if section.get("chart_path"):
        lines.append(f"- **圖表**：{_format_value(section.get('chart_path'))}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    if len(lines) == 2:
        lines.append("尚未提供事件研究資料。")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_data_gaps_block(data_gaps: list[str], title: str = "資料缺口與限制") -> list[str]:
    lines = [f"## {title}", ""]
    if data_gaps:
        for gap in data_gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("- 無")
    lines.append("")
    return lines


def _render_todo_block(todo_items: list[dict[str, Any]], title: str = "待辦事項") -> list[str]:
    lines = [f"## {title}", ""]
    if todo_items:
        for item in todo_items:
            lines.append(
                "- "
                + " | ".join(
                    [
                        f"id={_format_value(item.get('id'))}",
                        f"priority={_format_value(item.get('priority'))}",
                        f"reason={_format_value(item.get('reason'))}",
                        f"next_action={_format_value(item.get('next_action'))}",
                        f"source_context={_format_value(item.get('source_context'))}",
                    ]
                )
            )
    else:
        lines.append("- 無")
    lines.append("")
    return lines


def _render_metric_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = []

    for row in rows:
        if columns == ["指標", "內容", "來源"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["metric_name", "metric", "name", "label"])),
                        _format_value(_pick(row, ["content", "value", "summary", "expected_value", "actual_value", "forecast", "actual"])),
                        _format_value(_pick(row, ["source", "source_name", "source_kind"])),
                    ]
                )
                + " |"
            )
        elif columns == ["指標", "預期", "實際", "結果"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["metric_name", "metric", "name", "label"])),
                        _format_value(_pick(row, ["expectation", "expected_value", "forecast", "range", "pre_event"])),
                        _format_value(_pick(row, ["actual", "actual_value", "event_day", "value"])),
                        _format_value(_pick(row, ["expectation_match", "match", "status", "result"])),
                    ]
                )
                + " |"
            )
        elif columns == ["類型", "來源", "URL", "狀態", "摘錄"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["artifact_type", "type"])),
                        _format_value(_pick(row, ["source_name", "source"])),
                        _format_value(_pick(row, ["url", "source_ref"])),
                        _format_value(_pick(row, ["validation_status", "retrieval_status", "status"])),
                        _format_value(_pick(row, ["excerpt", "summary", "content"])),
                    ]
                )
                + " |"
            )
        elif columns == ["主題", "問題", "回答", "來源"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["topic", "metric_name", "name", "label"])),
                        _format_value(_pick(row, ["question_summary", "question", "summary"])),
                        _format_value(_pick(row, ["answer_summary", "answer", "content"])),
                        _format_value(_pick(row, ["source_ref", "source_name", "source"])),
                    ]
                )
                + " |"
            )
        elif columns == ["日期", "標題", "類型", "來源", "摘要"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["date", "article_date", "published_at"])),
                        _format_value(_pick(row, ["headline", "title"])),
                        _format_value(_pick(row, ["article_type", "type", "label"])),
                        _format_value(_pick(row, ["source_name", "source"])),
                        _format_value(_pick(row, ["summary", "content", "snippet"])),
                    ]
                )
                + " |"
            )
        elif columns == ["Panel", "Current", "Comparison", "Delta", "Status", "Summary"]:
            body.append(
                "| "
                + " | ".join(
                    [
                        _format_value(_pick(row, ["label", "panel_id"])),
                        _format_value(row.get("current_value")),
                        _format_value(row.get("comparison_value")),
                        _format_value(row.get("delta")),
                        _format_value(row.get("status")),
                        _format_value(row.get("summary")),
                    ]
                )
                + " |"
            )
        else:
            body.append("| " + " | ".join(_format_value(row.get(col.lower(), "")) for col in columns) + " |")

    if not body:
        body.append("| " + " | ".join(["-"] * len(columns)) + " |")
    return "\n".join([header, separator, *body])


def _pick(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, "", []):
            return value
    return ""


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, dict):
        if not value:
            return "-"
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, list):
        if not value:
            return "-"
        return ", ".join(_format_value(item) for item in value)
    text = str(value).strip()
    return text if text else "-"


def _format_observation(observation: Any) -> str:
    if not isinstance(observation, dict) or not observation:
        return "-"

    low = observation.get("value_low")
    high = observation.get("value_high")
    unit = _format_value(observation.get("unit"))

    if low in (None, "") and high in (None, ""):
        return "-"
    if low == high or high in (None, ""):
        base = f"{_format_value(low)} {unit}".strip()
    else:
        base = f"{_format_value(low)} ~ {_format_value(high)} {unit}".strip()

    evidence = _format_value(observation.get("evidence_span") or observation.get("source_text"))
    confidence = observation.get("confidence")
    source_kind = _format_value(observation.get("source_kind"))
    source_headline = _format_value(observation.get("source_headline"))
    hybrid = "hybrid_extracted" if observation.get("hybrid_extracted") else ""
    suffix_parts = [part for part in [hybrid, source_kind if source_kind != "-" else "", source_headline if source_headline != "-" else ""] if part]
    if evidence != "-":
        suffix_parts.append(f"evidence={evidence}")
    if confidence not in (None, ""):
        suffix_parts.append(f"confidence={confidence}")
    if suffix_parts:
        return f"{base} ({'; '.join(suffix_parts)})"
    return base


def _format_verified_metric(observation: dict[str, Any]) -> str:
    base = _format_observation(observation)
    if base == "-":
        return base
    evidence = _format_value(observation.get("evidence_span"))
    source_ref = _format_value(observation.get("source_ref"))
    artifact_type = _format_value(observation.get("source_artifact_type"))
    validation_status = _format_value(observation.get("validation_status"))
    suffix = f"evidence={evidence}; source={source_ref}; artifact={artifact_type}; validation={validation_status}"
    return f"{base} ({suffix})"


def _summarize_metric_statuses(status_counts: Any) -> str:
    if not isinstance(status_counts, dict) or not status_counts:
        return ""
    ordered = ["matched", "beat", "below", "partially_matched", "unknown"]
    parts = [f"{status}: {status_counts[status]}" for status in ordered if status in status_counts]
    return "；".join(parts)


def _count_records_by_phase(event_collection: dict[str, Any], phases: set[str]) -> int:
    count = 0
    for record in event_collection.get("records", []):
        if not isinstance(record, dict):
            continue
        if str(record.get("event_phase", "")).strip() in phases:
            count += 1
    return count


def _summarize_narratives(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    source_names: list[str] = []
    article_types: list[str] = []
    for row in rows:
        source_name = str(row.get("source_name", "")).strip()
        article_type = str(row.get("article_type", "")).strip()
        if source_name and source_name not in source_names:
            source_names.append(source_name)
        if article_type and article_type not in article_types:
            article_types.append(article_type)

    summary_parts = [f"共整理 {len(rows)} 筆敘事"]
    if article_types:
        summary_parts.append(f"主要類型包含 {', '.join(article_types[:3])}")
    if source_names:
        summary_parts.append(f"來源涵蓋 {', '.join(source_names[:3])}")
    return "；".join(summary_parts)
