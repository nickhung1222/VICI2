"""Unit tests for post-event narrative analysis."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.post_event_analysis import build_post_event_analysis


def test_build_post_event_analysis_uses_related_records_and_fallback_report(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    payload = build_post_event_analysis(
        event_collection={
            "query": {
                "event_type": "法說會",
                "event_date": "2025-04-17",
                "stock": {"code": "2330", "name": "台積電", "symbol": "2330.TW"},
            },
            "records": [
                {
                    "headline": "台積電法說後法人解讀 聚焦毛利率與資本支出",
                    "summary": "法說後市場轉向關注毛利率與 capex。",
                    "article_type": "法人解讀",
                    "event_phase": "post_event",
                    "article_date": "2025-04-18",
                    "source_name": "鉅亨網",
                    "is_post_event_earnings_related": True,
                },
                {
                    "headline": "台積電慈善基金會攜手熊本大學締結合作協議",
                    "summary": "與法說內容無直接關聯。",
                    "article_type": "媒體報導",
                    "event_phase": "post_event",
                    "article_date": "2025-04-18",
                    "source_name": "ETtoday新聞雲",
                    "is_post_event_earnings_related": False,
                },
            ],
        }
    )

    assert payload["mode"] == "rule_based_fallback"
    assert payload["used_record_count"] == 1
    assert "財務相關資訊" in payload["report"]
    assert "管理階層展望" in payload["report"]
    assert "新聞內容分歧" in payload["report"]
