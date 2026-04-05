"""Tests for earnings-call regression helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.earnings_validation import DEFAULT_GOLD_SAMPLES, summarize_regression_packages


def test_default_gold_samples_cover_multiple_sectors_and_transcript_modes():
    assert len(DEFAULT_GOLD_SAMPLES) == 10
    sectors = {sample["sector"] for sample in DEFAULT_GOLD_SAMPLES}
    assert {"semiconductor", "ic_design", "electronics_manufacturing", "telecom"} <= sectors
    assert any(sample["transcript_expected"] for sample in DEFAULT_GOLD_SAMPLES)
    assert any(not sample["transcript_expected"] for sample in DEFAULT_GOLD_SAMPLES)


def test_summarize_regression_packages_counts_sources_metrics_and_blocking_todos():
    summary = summarize_regression_packages(
        [
            {
                "official_artifacts": [{"retrieval_status": "ok"}],
                "earnings_digest": {
                    "financial_snapshot": {
                        "gross_margin": {"validation_status": "validated"},
                        "capex": {"validation_status": "validated"},
                    }
                },
                "todo_items": [{"priority": "blocking"}],
            },
            {
                "official_artifacts": [],
                "earnings_digest": {"financial_snapshot": {}},
                "todo_items": [{"priority": "non_blocking"}],
            },
        ]
    )

    assert summary == {
        "sample_count": 2,
        "artifact_success_count": 1,
        "official_source_saved_count": 1,
        "validated_metric_count": 2,
        "blocking_todo_count": 1,
    }
