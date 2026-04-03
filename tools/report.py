"""Save reports and assemble event-report payloads."""

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
    expectation_analysis: Optional[dict[str, Any]] = None,
    event_study: Optional[dict[str, Any]] = None,
    generated_at: Optional[str] = None,
    title: Optional[str] = None,
) -> dict[str, Any]:
    """Assemble a JSON-friendly event report payload from structured inputs."""
    event_collection = event_collection or {}
    heat_analysis = heat_analysis or {}
    expectation_analysis = expectation_analysis or {}
    event_study = event_study or {}

    metadata = _build_report_metadata(
        event_collection=event_collection,
        generated_at=generated_at,
        title=title,
    )
    sections = {
        "event_summary": _build_event_summary_section(event_collection, heat_analysis),
        "official_sources": _build_official_sources_section(event_collection),
        "earnings_highlights": _build_earnings_highlights_section(event_collection),
        "management_tone": _build_management_tone_section(event_collection),
        "qa_summary": _build_qa_summary_section(event_collection),
        "pre_event_expectations": _build_expectation_section(expectation_analysis, "pre_event_expectations"),
        "event_day_actuals": _build_expectation_section(expectation_analysis, "event_day_actuals"),
        "expectation_vs_actual": _build_comparison_section(expectation_analysis),
        "heat_analysis": _build_heat_section(heat_analysis),
        "event_study": _build_event_study_section(event_study),
        "data_gaps": _collect_data_gaps(event_collection, heat_analysis, expectation_analysis, event_study),
        "todo_items": _build_todo_section(event_collection),
    }
    markdown = render_event_report_markdown(metadata=metadata, sections=sections)

    return {
        "report_type": "event_report",
        "metadata": metadata,
        "event_collection": event_collection,
        "heat_analysis": heat_analysis,
        "expectation_analysis": expectation_analysis,
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
    lines.extend(_render_official_sources_block(sections.get("official_sources", {})))
    lines.extend(_render_earnings_highlights_block(sections.get("earnings_highlights", {})))
    lines.extend(_render_management_tone_block(sections.get("management_tone", {})))
    lines.extend(_render_qa_block(sections.get("qa_summary", {})))
    lines.extend(_render_expectation_block("六、事件前預期", sections.get("pre_event_expectations", {}), empty_message="尚未提供事件前預期資料。"))
    lines.extend(_render_expectation_block("七、事件當天實際", sections.get("event_day_actuals", {}), empty_message="尚未提供事件當天實際資料。"))
    lines.extend(_render_comparison_block(sections.get("expectation_vs_actual", {})))
    lines.extend(_render_heat_block(sections.get("heat_analysis", {})))
    lines.extend(_render_event_study_block(sections.get("event_study", {})))
    lines.extend(_render_data_gaps_block(sections.get("data_gaps", [])))
    lines.extend(_render_todo_block(sections.get("todo_items", [])))

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

    return {
        "stock_code": stock.get("code", ""),
        "stock_name": stock.get("name", ""),
        "symbol": stock.get("symbol", ""),
        "event_type": query.get("event_type", ""),
        "event_date": query.get("event_date", ""),
        "event_key": query.get("event_key", ""),
        "time_range": query.get("time_range", {}),
        "record_count": event_collection.get("record_count", 0),
        "record_breakdown": event_collection.get("record_breakdown", {}),
        "comparison_strategy": collection_plan.get("comparison_strategy", {}),
        "sources": collection_plan.get("sources", []),
        "source_policy": collection_plan.get("source_policy", ""),
        "primary_source": collection_plan.get("primary_source", ""),
        "heat_mode": heat_analysis.get("comparison_mode", ""),
        "official_artifact_count": len(event_collection.get("official_artifacts", [])),
        "todo_count": len(event_collection.get("todo_items", [])),
    }


def _build_official_sources_section(event_collection: dict[str, Any]) -> dict[str, Any]:
    artifacts = event_collection.get("official_artifacts", []) if isinstance(event_collection, dict) else []
    return {
        "rows": [artifact for artifact in artifacts if isinstance(artifact, dict)],
    }


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


def _build_expectation_section(expectation_analysis: dict[str, Any], section_key: str) -> dict[str, Any]:
    if not isinstance(expectation_analysis, dict):
        return {"rows": [], "summary": "", "data_gaps": []}

    rows = _extract_rows(expectation_analysis, [section_key, "rows", "items", "metrics"])
    if not rows and isinstance(expectation_analysis.get("metrics"), list):
        rows = _build_expectation_rows_from_metrics(expectation_analysis["metrics"], section_key)
    return {
        "rows": rows,
        "summary": expectation_analysis.get("summary", ""),
        "notes": expectation_analysis.get("notes", ""),
        "data_gaps": list(expectation_analysis.get("data_gaps", [])),
    }


def _build_comparison_section(expectation_analysis: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(expectation_analysis, dict):
        return {"rows": [], "summary": "", "data_gaps": []}

    rows = _extract_rows(expectation_analysis, ["comparison_rows", "comparisons", "metric_comparisons"])
    if not rows and isinstance(expectation_analysis.get("metrics"), list):
        rows = _build_comparison_rows_from_metrics(expectation_analysis["metrics"])
    return {
        "rows": rows,
        "summary": expectation_analysis.get(
            "comparison_summary",
            expectation_analysis.get("summary", _summarize_metric_statuses(expectation_analysis.get("status_counts", {}))),
        ),
        "data_gaps": list(expectation_analysis.get("data_gaps", [])),
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
    expectation_analysis: dict[str, Any],
    event_study: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    for source in (event_collection, heat_analysis, expectation_analysis, event_study):
        if isinstance(source, dict):
            source_gaps = source.get("data_gaps", [])
            if isinstance(source_gaps, list):
                gaps.extend(str(item) for item in source_gaps if item)

    if not heat_analysis:
        gaps.append("heat_analysis_missing")
    if not expectation_analysis:
        gaps.append("expectation_analysis_missing")

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
    lines.append(f"- **官方來源數**：{_format_value(section.get('official_artifact_count'))}")
    lines.append(f"- **待辦數**：{_format_value(section.get('todo_count'))}")
    lines.append(f"- **比較模式**：{_format_value(section.get('comparison_strategy', {}).get('comparison_mode'))}")
    lines.append(f"- **資料來源**：{_format_value(section.get('sources'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_official_sources_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 二、官方來源清單", ""]
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
    lines = ["## 三、法說重點", ""]
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
    lines = ["## 四、管理層態度", ""]
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
    lines = ["## 五、Q&A 摘要", ""]
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


def _render_expectation_block(title: str, section: dict[str, Any], empty_message: str) -> list[str]:
    lines = [f"## {title}", ""]
    rows = section.get("rows", []) if isinstance(section, dict) else []
    summary = section.get("summary", "") if isinstance(section, dict) else ""
    notes = section.get("notes", "") if isinstance(section, dict) else ""

    if rows:
        lines.append(_render_metric_table(rows, ["指標", "內容", "來源"]))
        lines.append("")
    else:
        lines.append(empty_message)
        lines.append("")

    if summary:
        lines.append(f"- **摘要**：{summary}")
    if notes:
        lines.append(f"- **備註**：{notes}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_comparison_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 八、預期 vs 實際", ""]
    rows = section.get("rows", []) if isinstance(section, dict) else []
    summary = section.get("summary", "") if isinstance(section, dict) else ""

    if rows:
        lines.append(_render_metric_table(rows, ["指標", "預期", "實際", "結果"]))
        lines.append("")
    else:
        lines.append("尚未提供預期與實際的比對資料。")
        lines.append("")

    if summary:
        lines.append(f"- **比對摘要**：{summary}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_heat_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 九、熱度分析", ""]
    if not section:
        lines.append("尚未提供熱度分析資料。")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines

    lines.append(f"- **比較模式**：{_format_value(section.get('comparison_mode'))}")
    lines.append(f"- **事件鍵**：{_format_value(section.get('event_key'))}")
    lines.append(f"- **對照事件鍵**：{_format_value(section.get('comparison_event_key'))}")
    lines.append(f"- **目前窗口總量**：{_format_value(section.get('current_window_total'))}")
    lines.append(f"- **對照值**：{_format_value(section.get('comparison_value'))}")
    lines.append(f"- **熱度比**：{_format_value(section.get('news_heat_ratio'))}")
    lines.append(f"- **熱度標籤**：{_format_value(section.get('news_heat_label'))}")
    if section.get("comparison_basis"):
        lines.append(f"- **比較基準**：{_format_value(section.get('comparison_basis'))}")
    if section.get("data_gaps"):
        lines.append(f"- **資料缺口**：{_format_value(section.get('data_gaps'))}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _render_event_study_block(section: dict[str, Any]) -> list[str]:
    lines = ["## 十、事件研究（可選）", ""]
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


def _render_data_gaps_block(data_gaps: list[str]) -> list[str]:
    lines = ["## 十一、資料缺口與限制", ""]
    if data_gaps:
        for gap in data_gaps:
            lines.append(f"- {gap}")
    else:
        lines.append("- 無")
    lines.append("")
    return lines


def _render_todo_block(todo_items: list[dict[str, Any]]) -> list[str]:
    lines = ["## 十二、待辦事項", ""]
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


def _extract_rows(source: dict[str, Any], keys: list[str]) -> list[dict[str, Any]]:
    for key in keys:
        value = source.get(key)
        if isinstance(value, list) and value:
            return [row for row in value if isinstance(row, dict)]
    return []


def _build_expectation_rows_from_metrics(metrics: list[dict[str, Any]], section_key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    observation_key = "expectation" if section_key == "pre_event_expectations" else "actual"

    for metric_row in metrics:
        if not isinstance(metric_row, dict):
            continue
        observation = metric_row.get(observation_key)
        if not isinstance(observation, dict) or not observation:
            continue
        rows.append(
            {
                "metric_name": metric_row.get("metric", ""),
                "content": _format_observation(observation),
                "source_kind": observation.get("source_kind", ""),
            }
        )

    return rows


def _build_comparison_rows_from_metrics(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric_row in metrics:
        if not isinstance(metric_row, dict):
            continue
        rows.append(
            {
                "metric_name": metric_row.get("metric", ""),
                "expectation": _format_observation(metric_row.get("expectation")),
                "actual": _format_observation(metric_row.get("actual")),
                "expectation_match": metric_row.get("status", ""),
            }
        )
    return rows


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
