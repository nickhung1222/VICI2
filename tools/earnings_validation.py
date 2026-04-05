"""Regression helpers for earnings-call ingestion validation."""

from __future__ import annotations

from typing import Any

DEFAULT_GOLD_SAMPLES: list[dict[str, Any]] = [
    {"stock_code": "2330", "stock_name": "台積電", "sector": "semiconductor", "transcript_expected": True},
    {"stock_code": "2454", "stock_name": "聯發科", "sector": "ic_design", "transcript_expected": True},
    {"stock_code": "2303", "stock_name": "聯電", "sector": "semiconductor", "transcript_expected": True},
    {"stock_code": "2317", "stock_name": "鴻海", "sector": "electronics_manufacturing", "transcript_expected": True},
    {"stock_code": "3711", "stock_name": "日月光投控", "sector": "semiconductor_packaging", "transcript_expected": False},
    {"stock_code": "2382", "stock_name": "廣達", "sector": "electronics_manufacturing", "transcript_expected": False},
    {"stock_code": "2308", "stock_name": "台達電", "sector": "industrial_electronics", "transcript_expected": True},
    {"stock_code": "2412", "stock_name": "中華電", "sector": "telecom", "transcript_expected": False},
    {"stock_code": "2379", "stock_name": "瑞昱", "sector": "ic_design", "transcript_expected": False},
    {"stock_code": "3231", "stock_name": "緯創", "sector": "electronics_manufacturing", "transcript_expected": False},
]


def summarize_regression_packages(packages: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize regression outputs from multiple event packages."""
    artifact_success = 0
    official_source_saved = 0
    validated_metrics = 0
    blocking_todos = 0

    for payload in packages:
        artifacts = payload.get("official_artifacts", []) if isinstance(payload, dict) else []
        if any(isinstance(item, dict) and item.get("retrieval_status") == "ok" for item in artifacts):
            artifact_success += 1
        if artifacts:
            official_source_saved += 1

        digest = payload.get("earnings_digest", {}) if isinstance(payload, dict) else {}
        snapshot = digest.get("financial_snapshot", {}) if isinstance(digest, dict) else {}
        validated_metrics += sum(
            1 for value in snapshot.values() if isinstance(value, dict) and value.get("validation_status") == "validated"
        )

        todos = payload.get("todo_items", []) if isinstance(payload, dict) else []
        blocking_todos += sum(1 for item in todos if isinstance(item, dict) and item.get("priority") == "blocking")

    return {
        "sample_count": len(packages),
        "artifact_success_count": artifact_success,
        "official_source_saved_count": official_source_saved,
        "validated_metric_count": validated_metrics,
        "blocking_todo_count": blocking_todos,
    }


__all__ = ["DEFAULT_GOLD_SAMPLES", "summarize_regression_packages"]
