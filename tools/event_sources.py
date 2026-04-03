"""Official event source adapters and earnings-call artifact extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from tools.expectation_analysis import SUPPORTED_METRICS, extract_metric_observations
from tools.schemas import classify_event_phase, infer_record_flags

_MOPS_OV_EVENT_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb07_1"
_MOPS_OV_PAGE_URL = "https://mopsov.twse.com.tw/mops/web/t100sb07_1"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
}
_ARTIFACT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "transcript": ("transcript", "逐字稿", "conference transcript"),
    "presentation": ("presentation", "簡報", "slide", "deck"),
    "earnings_release": ("earnings release", "financial results", "results", "財務報告", "財報"),
    "management_report": ("management report", "營運報告", "業務報告"),
    "webcast_replay": ("webcast", "replay", "conference call", "影音", "直播"),
}
_POSITIVE_TONE_KEYWORDS = (
    "strong",
    "confident",
    "confidence",
    "optimistic",
    "well positioned",
    "record",
    "robust",
    "樂觀",
    "看好",
    "強勁",
    "有信心",
    "穩健",
)
_CAUTIOUS_TONE_KEYWORDS = (
    "uncertain",
    "uncertainty",
    "soft",
    "challenging",
    "headwind",
    "conservative",
    "保守",
    "謹慎",
    "逆風",
    "不確定",
    "疲弱",
)
_QA_SECTION_PATTERNS = (
    "questions and answers",
    "question-and-answer",
    "q&a",
    "q & a",
    "問答",
)
_TODO_TEMPLATES = {
    "official_page_url_missing": {
        "priority": "blocking",
        "reason": "MOPS record does not expose an official IR page URL for this event.",
        "next_action": "Review the issuer IR site manually or extend the company-IR resolver.",
        "source_context": "official_artifacts",
    },
    "official_artifacts_missing": {
        "priority": "blocking",
        "reason": "No official presentation, release, transcript, or replay was found.",
        "next_action": "Fallback to MOPS summary and event-day media only, then inspect IR site manually.",
        "source_context": "official_artifacts",
    },
    "transcript_missing": {
        "priority": "blocking",
        "reason": "No official transcript was found for this earnings call.",
        "next_action": "Fallback to presentation and earnings release for summary fields.",
        "source_context": "earnings_digest.qa_topics",
    },
    "qa_not_available": {
        "priority": "non_blocking",
        "reason": "Q&A topics were not extracted because no verified transcript content was available.",
        "next_action": "Retry after a transcript is published or add webcast transcription support.",
        "source_context": "earnings_digest.qa_topics",
    },
    "pdf_text_extraction_failed": {
        "priority": "blocking",
        "reason": "PDF content could not be converted into usable text.",
        "next_action": "Retry with a fallback extractor or inspect the original PDF manually.",
        "source_context": "official_artifacts",
    },
    "metric_without_evidence": {
        "priority": "blocking",
        "reason": "A metric candidate was discarded because it lacked evidence or source references.",
        "next_action": "Keep the field empty and inspect the artifact text manually.",
        "source_context": "earnings_digest.financial_snapshot",
    },
    "artifact_company_mismatch": {
        "priority": "blocking",
        "reason": "An artifact appears to belong to a different company or unrelated domain.",
        "next_action": "Review the source URL and tighten domain or title validation rules.",
        "source_context": "official_artifacts",
    },
    "artifact_event_date_mismatch": {
        "priority": "blocking",
        "reason": "An artifact date conflicts with the requested event date.",
        "next_action": "Verify whether the artifact is from another quarter or update the event key/date.",
        "source_context": "official_artifacts",
    },
    "official_metrics_unavailable": {
        "priority": "blocking",
        "reason": "No official financial metrics were extracted with evidence.",
        "next_action": "Inspect official artifacts manually and improve extraction rules for this issuer.",
        "source_context": "earnings_digest.financial_snapshot",
    },
}


@dataclass(frozen=True)
class _ArtifactValidation:
    status: str
    gaps: tuple[str, ...]


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
    empty_payload = {
        "records": [],
        "data_gaps": [],
        "official_artifacts": [],
        "earnings_digest": _empty_earnings_digest(),
        "todo_items": [],
    }
    if event_type != "法說會" or not stock_code:
        return empty_payload

    record = fetch_mops_investor_conference(
        stock_code=stock_code,
        stock_name=stock_name,
        symbol=symbol,
        event_date=event_date,
        event_key=event_key,
    )
    if not record:
        gaps = ["mops_official_record_unavailable"]
        empty_payload["data_gaps"] = gaps
        empty_payload["todo_items"] = build_todo_items(gaps)
        return empty_payload

    article_date = record.get("article_date", "")
    if start_date and article_date and article_date < start_date:
        gaps = ["mops_record_outside_requested_range"]
        empty_payload["data_gaps"] = gaps
        empty_payload["todo_items"] = build_todo_items(gaps)
        return empty_payload
    if end_date and article_date and article_date > end_date:
        gaps = ["mops_record_outside_requested_range"]
        empty_payload["data_gaps"] = gaps
        empty_payload["todo_items"] = build_todo_items(gaps)
        return empty_payload

    official_artifacts, artifact_gaps = collect_official_event_artifacts(
        stock_code=stock_code,
        stock_name=stock_name,
        event_date=event_date or article_date,
        event_key=event_key,
        official_page_url=record.get("official_page_url", ""),
        mops_record=record,
    )
    digest_payload = build_earnings_digest(
        stock_code=stock_code,
        stock_name=stock_name,
        event_date=event_date or article_date,
        event_key=event_key,
        artifacts=official_artifacts,
        fallback_summary=record.get("summary", ""),
    )

    records = [record]
    synthesized_record = build_synthesized_official_record(
        base_record=record,
        earnings_digest=digest_payload["earnings_digest"],
        official_artifacts=official_artifacts,
    )
    if synthesized_record:
        records.append(synthesized_record)

    data_gaps = _dedupe_preserve_order(
        artifact_gaps
        + list(digest_payload.get("data_gaps", []))
    )
    todo_items = build_todo_items(data_gaps)
    return {
        "records": records,
        "data_gaps": data_gaps,
        "official_artifacts": [serialize_artifact(artifact) for artifact in official_artifacts],
        "earnings_digest": dict(digest_payload["earnings_digest"]),
        "todo_items": todo_items,
    }


def fetch_mops_investor_conference(
    *,
    stock_code: str,
    stock_name: str,
    symbol: str,
    event_date: str = "",
    event_key: str = "",
) -> dict[str, Any] | None:
    """Fetch the latest investor-conference record from MOPS OV."""
    try:
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
    except requests.RequestException:
        return None
    try:
        response.raise_for_status()
    except requests.RequestException:
        return None

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


def collect_official_event_artifacts(
    *,
    stock_code: str,
    stock_name: str,
    event_date: str,
    event_key: str,
    official_page_url: str,
    mops_record: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Collect official artifacts for an earnings call."""
    artifacts = [
        build_mops_artifact(
            stock_code=stock_code,
            stock_name=stock_name,
            event_date=event_date,
            event_key=event_key,
            mops_record=mops_record,
        )
    ]
    data_gaps: list[str] = []

    if not official_page_url:
        data_gaps.append("official_page_url_missing")
        return artifacts, data_gaps

    try:
        response = requests.get(official_page_url, headers=_HEADERS, timeout=8)
        response.raise_for_status()
    except requests.RequestException:
        data_gaps.append("official_artifacts_missing")
        return artifacts, data_gaps

    content_type = response.headers.get("Content-Type", "").lower()
    discovered_urls: list[tuple[str, str]] = []
    if "pdf" in content_type or official_page_url.lower().endswith(".pdf"):
        discovered_urls.append((official_page_url, _classify_artifact_type(official_page_url, official_page_url)))
    else:
        html = response.text
        soup = BeautifulSoup(html, "lxml")
        discovered_urls.extend(_discover_artifact_links(soup, official_page_url))

    seen_urls = {artifact.get("url", "") for artifact in artifacts}
    external_artifact_count = 0
    for artifact_url, artifact_type in discovered_urls:
        if not artifact_url or artifact_url in seen_urls:
            continue
        seen_urls.add(artifact_url)
        artifact = fetch_official_artifact(
            stock_code=stock_code,
            stock_name=stock_name,
            event_date=event_date,
            event_key=event_key,
            artifact_url=artifact_url,
            artifact_type=artifact_type,
            source_name=_infer_source_name(artifact_url),
            official_page_url=official_page_url,
        )
        for gap in artifact.pop("_validation_gaps", []):
            if gap not in data_gaps:
                data_gaps.append(gap)
        artifacts.append(artifact)
        external_artifact_count += 1

    if external_artifact_count == 0:
        data_gaps.append("official_artifacts_missing")
    if not any(artifact.get("artifact_type") == "transcript" and artifact.get("retrieval_status") == "ok" for artifact in artifacts):
        data_gaps.append("transcript_missing")

    return artifacts, _dedupe_preserve_order(data_gaps)


def build_earnings_digest(
    *,
    stock_code: str,
    stock_name: str,
    event_date: str,
    event_key: str,
    artifacts: list[dict[str, Any]],
    fallback_summary: str,
) -> dict[str, Any]:
    """Build a verified digest from official artifacts."""
    data_gaps: list[str] = []
    digest = _empty_earnings_digest()
    official_text_artifacts = [
        artifact
        for artifact in artifacts
        if artifact.get("artifact_type") != "mops_notice" and artifact.get("retrieval_status") == "ok"
    ]

    financial_snapshot = extract_financial_snapshot(
        artifacts=official_text_artifacts,
        event_key=event_key,
    )
    if financial_snapshot["data_gaps"]:
        data_gaps.extend(financial_snapshot["data_gaps"])
    digest["financial_snapshot"] = financial_snapshot["metrics"]

    tone = extract_management_tone(official_text_artifacts)
    if tone.get("data_gaps"):
        data_gaps.extend(tone["data_gaps"])
    digest["management_tone"] = tone["tone"]

    qa_topics = extract_qa_topics(official_text_artifacts)
    if qa_topics["data_gaps"]:
        data_gaps.extend(qa_topics["data_gaps"])
    digest["qa_topics"] = qa_topics["qa_topics"]

    takeaways = _build_official_takeaways(artifacts, fallback_summary=fallback_summary)
    digest["official_takeaways"] = takeaways

    if not digest["financial_snapshot"]:
        data_gaps.append("official_metrics_unavailable")

    digest["analysis_target"] = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "event_date": event_date,
        "event_key": event_key,
    }
    digest["data_gaps"] = _dedupe_preserve_order(data_gaps)
    return {
        "earnings_digest": digest,
        "data_gaps": digest["data_gaps"],
    }


def extract_financial_snapshot(*, artifacts: list[dict[str, Any]], event_key: str) -> dict[str, Any]:
    """Extract verified metrics from official artifacts."""
    by_metric: dict[str, dict[str, Any]] = {}
    data_gaps: list[str] = []
    artifact_priority = {
        "transcript": 40,
        "earnings_release": 35,
        "management_report": 30,
        "presentation": 25,
        "webcast_replay": 20,
        "mops_notice": 10,
    }

    for index, artifact in enumerate(artifacts):
        content = str(artifact.get("content", "")).strip()
        if not content:
            continue
        observations = extract_metric_observations(
            {
                "event_key": event_key,
                "event_phase": "event_day",
                "headline": artifact.get("title", artifact.get("artifact_type", "")),
                "summary": content,
                "source_type": "official",
            },
            record_index=index,
        )
        for observation in observations:
            metric = observation.get("metric", "")
            if metric not in SUPPORTED_METRICS:
                continue
            evidence = str(observation.get("source_text", "")).strip()
            if not evidence or not artifact.get("url"):
                if "metric_without_evidence" not in data_gaps:
                    data_gaps.append("metric_without_evidence")
                continue
            candidate = {
                "value_low": observation.get("value_low"),
                "value_high": observation.get("value_high"),
                "unit": observation.get("unit", ""),
                "evidence_span": compact_excerpt(evidence, max_length=220),
                "source_ref": artifact.get("url", ""),
                "source_artifact_type": artifact.get("artifact_type", ""),
                "source_name": artifact.get("source_name", ""),
                "validation_status": "validated",
                "_rank": float(observation.get("score", 0.0)) + artifact_priority.get(artifact.get("artifact_type", ""), 0),
            }
            current = by_metric.get(metric)
            if current is None or candidate["_rank"] > current["_rank"]:
                by_metric[metric] = candidate

    for metric in list(by_metric):
        by_metric[metric].pop("_rank", None)

    return {
        "metrics": by_metric,
        "data_gaps": _dedupe_preserve_order(data_gaps),
    }


def extract_management_tone(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    """Extract conservative management tone labels with evidence."""
    scored_sentences: list[tuple[int, str, dict[str, Any]]] = []
    positive_hits = 0
    cautious_hits = 0

    for artifact in artifacts:
        if artifact.get("artifact_type") not in {"transcript", "earnings_release", "management_report"}:
            continue
        for sentence in _split_sentences(str(artifact.get("content", ""))):
            lowered = sentence.lower()
            pos = sum(keyword in lowered for keyword in _POSITIVE_TONE_KEYWORDS)
            neg = sum(keyword in lowered for keyword in _CAUTIOUS_TONE_KEYWORDS)
            if not pos and not neg:
                continue
            positive_hits += pos
            cautious_hits += neg
            scored_sentences.append((max(pos, neg), sentence, artifact))

    if not scored_sentences:
        return {"tone": {}, "data_gaps": []}

    if positive_hits and cautious_hits:
        label = "mixed"
    elif positive_hits:
        label = "bullish"
    elif cautious_hits:
        label = "cautious"
    else:
        label = "neutral"

    evidence_rows = []
    for _, sentence, artifact in sorted(scored_sentences, key=lambda item: item[0], reverse=True)[:3]:
        evidence_rows.append(
            {
                "excerpt": compact_excerpt(sentence, max_length=220),
                "source_ref": artifact.get("url", ""),
                "source_artifact_type": artifact.get("artifact_type", ""),
                "source_name": artifact.get("source_name", ""),
            }
        )

    return {
        "tone": {
            "label": label,
            "evidence": evidence_rows,
            "validation_status": "validated" if evidence_rows else "unverified",
        },
        "data_gaps": [],
    }


def extract_qa_topics(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    """Extract conservative Q&A topics from transcript-like artifacts."""
    transcript_artifacts = [
        artifact
        for artifact in artifacts
        if artifact.get("artifact_type") == "transcript" and artifact.get("retrieval_status") == "ok"
    ]
    if not transcript_artifacts:
        return {"qa_topics": [], "data_gaps": ["qa_not_available"]}

    for artifact in transcript_artifacts:
        topics = _extract_qa_topics_from_text(str(artifact.get("content", "")), artifact)
        if topics:
            return {"qa_topics": topics[:5], "data_gaps": []}

    return {"qa_topics": [], "data_gaps": ["qa_not_available"]}


def build_synthesized_official_record(
    *,
    base_record: dict[str, Any],
    earnings_digest: dict[str, Any],
    official_artifacts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Build a synthesized official record for downstream expectation analysis."""
    metrics = dict(earnings_digest.get("financial_snapshot", {}))
    tone = dict(earnings_digest.get("management_tone", {}))
    qa_topics = list(earnings_digest.get("qa_topics", []))

    if not metrics and not tone and not qa_topics:
        return None

    record = {
        **base_record,
        "headline": f"{base_record.get('event_key') or base_record.get('event_date')} 法說會官方重點",
        "summary": "；".join(earnings_digest.get("official_takeaways", [])[:3]) or base_record.get("summary", ""),
        "article_type": "官方重點",
        "actual_metrics": metrics,
        "tone": tone,
        "qa_topics": qa_topics,
        "artifact_refs": [artifact.get("url", "") for artifact in official_artifacts if artifact.get("url")],
        "validation_status": "validated" if metrics else "partial",
        "is_actual": True,
        "is_expectation": False,
    }
    return record


def build_mops_artifact(
    *,
    stock_code: str,
    stock_name: str,
    event_date: str,
    event_key: str,
    mops_record: dict[str, Any],
) -> dict[str, Any]:
    """Build the baseline MOPS artifact."""
    excerpt = compact_excerpt(mops_record.get("summary", ""), max_length=220)
    return {
        "stock_code": stock_code,
        "company": stock_name,
        "event_date": event_date,
        "event_key": event_key,
        "artifact_type": "mops_notice",
        "source_name": "公開資訊觀測站",
        "url": mops_record.get("source_url", _MOPS_OV_PAGE_URL),
        "published_at": mops_record.get("article_date", ""),
        "fetched_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "format": "html",
        "language": "zh-TW",
        "retrieval_status": "ok",
        "validation_status": "validated",
        "excerpt": excerpt,
        "title": mops_record.get("headline", ""),
        "content": mops_record.get("summary", ""),
    }


def fetch_official_artifact(
    *,
    stock_code: str,
    stock_name: str,
    event_date: str,
    event_key: str,
    artifact_url: str,
    artifact_type: str,
    source_name: str,
    official_page_url: str,
) -> dict[str, Any]:
    """Fetch a single official artifact and extract basic metadata."""
    fetched_at = datetime.now().astimezone().isoformat(timespec="seconds")
    artifact = {
        "stock_code": stock_code,
        "company": stock_name,
        "event_date": event_date,
        "event_key": event_key,
        "artifact_type": artifact_type,
        "source_name": source_name,
        "url": artifact_url,
        "published_at": event_date,
        "fetched_at": fetched_at,
        "format": "pdf" if artifact_url.lower().endswith(".pdf") else "html",
        "language": "",
        "retrieval_status": "unavailable",
        "validation_status": "unverified",
        "excerpt": "",
        "title": "",
        "content": "",
        "_validation_gaps": [],
    }

    try:
        response = requests.get(artifact_url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException:
        artifact["retrieval_status"] = "request_failed"
        return artifact

    content_type = response.headers.get("Content-Type", "").lower()
    artifact["format"] = "pdf" if "pdf" in content_type or artifact_url.lower().endswith(".pdf") else "html"
    content, title = _extract_artifact_text(response.content, response.text if artifact["format"] == "html" else "")
    if artifact["format"] == "pdf" and not content:
        artifact["_validation_gaps"].append("pdf_text_extraction_failed")
    artifact["title"] = title
    artifact["content"] = content
    artifact["excerpt"] = compact_excerpt(content, max_length=220)
    artifact["language"] = _detect_language(f"{title} {content}")
    artifact["retrieval_status"] = "ok" if content or artifact["format"] == "webcast_replay" else "ok"
    validation = validate_artifact(
        artifact=artifact,
        stock_code=stock_code,
        stock_name=stock_name,
        event_date=event_date,
        official_page_url=official_page_url,
    )
    artifact["validation_status"] = validation.status
    artifact["_validation_gaps"] = list(validation.gaps)
    return artifact


def validate_artifact(
    *,
    artifact: dict[str, Any],
    stock_code: str,
    stock_name: str,
    event_date: str,
    official_page_url: str,
) -> _ArtifactValidation:
    """Validate source-level company/domain/date consistency."""
    gaps: list[str] = []
    artifact_host = urlparse(str(artifact.get("url", ""))).netloc.lower()
    official_host = urlparse(official_page_url).netloc.lower()
    title_and_excerpt = f"{artifact.get('title', '')} {artifact.get('excerpt', '')}"

    if official_host and artifact_host and official_host != artifact_host and stock_name not in title_and_excerpt and stock_code not in title_and_excerpt:
        gaps.append("artifact_company_mismatch")

    parsed_dates = _find_iso_dates(title_and_excerpt)
    if event_date and parsed_dates and event_date not in parsed_dates:
        gaps.append("artifact_event_date_mismatch")

    return _ArtifactValidation(status="validated" if not gaps else "mismatch", gaps=tuple(gaps))


def build_todo_items(data_gaps: list[str]) -> list[dict[str, Any]]:
    """Convert data gaps into explicit todo items."""
    todos: list[dict[str, Any]] = []
    seen: set[str] = set()
    for gap in data_gaps:
        template = _TODO_TEMPLATES.get(gap)
        if not template or gap in seen:
            continue
        seen.add(gap)
        todos.append({"id": gap, **template})
    return todos


def serialize_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    """Drop internal-only fields before returning artifacts to callers."""
    return {
        key: value
        for key, value in artifact.items()
        if key not in {"content", "_validation_gaps"}
    }


def compact_excerpt(text: str, max_length: int = 220) -> str:
    """Compact text for source excerpt fields."""
    compacted = " ".join(str(text).split())
    if len(compacted) <= max_length:
        return compacted
    return compacted[: max_length - 1].rstrip() + "…"


def _empty_earnings_digest() -> dict[str, Any]:
    return {
        "analysis_target": {},
        "financial_snapshot": {},
        "management_tone": {},
        "qa_topics": [],
        "official_takeaways": [],
        "data_gaps": [],
    }


def _discover_artifact_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    host = urlparse(base_url).netloc.lower()
    discovered: list[tuple[str, str]] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        artifact_url = urljoin(base_url, href)
        parsed = urlparse(artifact_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if host and parsed.netloc.lower() != host:
            continue
        text = f"{anchor.get_text(' ', strip=True)} {href}"
        artifact_type = _classify_artifact_type(text, href)
        if not artifact_type:
            continue
        if artifact_url in seen:
            continue
        seen.add(artifact_url)
        discovered.append((artifact_url, artifact_type))
    return discovered


def _classify_artifact_type(text: str, href: str) -> str:
    lowered = f"{text} {href}".lower()
    for artifact_type, keywords in _ARTIFACT_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return artifact_type
    if href.lower().endswith(".pdf"):
        return "presentation"
    return ""


def _extract_artifact_text(raw_bytes: bytes, raw_html: str) -> tuple[str, str]:
    if raw_html:
        soup = BeautifulSoup(raw_html, "lxml")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        text = soup.get_text(" ", strip=True)
        return text, title

    text = _extract_pdf_text(raw_bytes)
    return text, ""


def _extract_pdf_text(raw_bytes: bytes) -> str:
    try:
        from pypdf import PdfReader

        reader = PdfReader(BytesIO(raw_bytes))
        return " ".join(page.extract_text() or "" for page in reader.pages).strip()
    except Exception:
        try:
            import pdfplumber

            with pdfplumber.open(BytesIO(raw_bytes)) as pdf:
                return " ".join(page.extract_text() or "" for page in pdf.pages).strip()
        except Exception:
            return ""


def _build_official_takeaways(artifacts: list[dict[str, Any]], *, fallback_summary: str) -> list[str]:
    takeaways: list[str] = []
    if fallback_summary:
        takeaways.append(compact_excerpt(fallback_summary, max_length=160))
    for artifact in artifacts:
        excerpt = compact_excerpt(artifact.get("excerpt", ""), max_length=160)
        if excerpt and excerpt not in takeaways:
            takeaways.append(excerpt)
        if len(takeaways) >= 5:
            break
    return takeaways


def _extract_qa_topics_from_text(text: str, artifact: dict[str, Any]) -> list[dict[str, Any]]:
    lowered = text.lower()
    start_index = -1
    for marker in _QA_SECTION_PATTERNS:
        start_index = lowered.find(marker)
        if start_index != -1:
            break
    if start_index == -1:
        return []

    qa_text = text[start_index:]
    paragraphs = [part.strip() for part in re.split(r"\n{2,}|(?<=[.?!。！？])\s+(?=Q[:：]|A[:：]|問[:：]|答[:：])", qa_text) if part.strip()]
    topics: list[dict[str, Any]] = []
    for paragraph in paragraphs:
        if len(topics) >= 5:
            break
        question_match = re.search(
            r"(Q(?:uestion)?[:：].*?|問[:：].*?)(A(?:nswer)?[:：]|答[:：])",
            paragraph,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not question_match:
            continue
        answer_marker = question_match.group(2)
        split_index = paragraph.find(answer_marker)
        question_text = paragraph[:split_index].strip()
        answer_text = paragraph[split_index:].strip()
        if not question_text or not answer_text:
            continue
        topics.append(
            {
                "topic": _infer_qa_topic(question_text, answer_text),
                "question_summary": compact_excerpt(question_text, max_length=140),
                "answer_summary": compact_excerpt(answer_text, max_length=180),
                "evidence": compact_excerpt(paragraph, max_length=220),
                "source_ref": artifact.get("url", ""),
                "source_artifact_type": artifact.get("artifact_type", ""),
                "source_name": artifact.get("source_name", ""),
            }
        )
    return topics


def _infer_qa_topic(question_text: str, answer_text: str) -> str:
    lowered = f"{question_text} {answer_text}".lower()
    topic_map = {
        "capex": ("capex", "資本支出"),
        "guidance": ("guidance", "展望", "財測"),
        "gross_margin": ("gross margin", "毛利率"),
        "revenue": ("revenue", "營收"),
        "eps": ("eps", "每股盈餘"),
        "ai_demand": ("ai", "需求", "demand"),
    }
    for label, keywords in topic_map.items():
        if any(keyword in lowered for keyword in keywords):
            return label
    return "general"


def _split_sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[。！？.!?])\s+", str(text)) if part.strip()]


def _detect_language(text: str) -> str:
    if re.search(r"[\u4e00-\u9fff]", text):
        return "zh-TW"
    if re.search(r"[A-Za-z]", text):
        return "en"
    return ""


def _infer_source_name(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if "tsmc" in host:
        return "TSMC IR"
    if "mediatek" in host:
        return "MediaTek IR"
    if "twse" in host or "mops" in host:
        return "公開資訊觀測站"
    return parsed.netloc or "Official IR"


def _find_iso_dates(text: str) -> list[str]:
    matches = []
    for match in re.finditer(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text):
        year, month, day = match.groups()
        try:
            matches.append(datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return matches


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


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
