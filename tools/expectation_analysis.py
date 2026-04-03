"""Structured expectation-vs-actual analysis for recurring events.

v1 focuses on Taiwanese earnings calls (``法說會``). The analyzer consumes
structured event/news records, groups them by ``event_key`` and
``event_phase``, and conservatively compares pre-event expectations with
event-day actuals.
"""

from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

SUPPORTED_METRICS = ("revenue", "gross_margin", "operating_margin", "eps", "capex", "guidance")

_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "revenue": ("營收", "營業收入", "revenue", "sales"),
    "gross_margin": ("毛利率", "gross margin", "gross_margin"),
    "operating_margin": ("營業利益率", "營益率", "operating margin", "operating_margin"),
    "eps": ("eps", "每股盈餘"),
    "capex": ("capex", "資本支出", "資本開支"),
    "guidance": ("財測", "指引", "展望", "guidance", "預期", "預估"),
}

_METRIC_POLARITY: dict[str, str] = {
    "revenue": "higher",
    "gross_margin": "higher",
    "operating_margin": "higher",
    "eps": "higher",
    "guidance": "higher",
    "capex": "neutral",
}

_PERCENT_UNITS = {"%", "％", "pct", "percent", "percentage", "成"}
_AMOUNT_UNITS = {"兆", "億", "億元", "萬", "萬元", "元", "usd", "usd$", "ntd", "bn", "b", "m", "million"}
_TEXT_FIELDS = ("headline", "summary", "content", "body", "snippet")
_EVENT_PHASES = {"pre_event", "event_day"}

_RANGE_RE = re.compile(
    r"(?P<low>\d+(?:\.\d+)?)\s*(?:~|～|-|—|–|至|到)\s*(?P<high>\d+(?:\.\d+)?)\s*(?P<unit>%|％|兆|億|億元|萬|萬元|元|usd\$?|ntd|bn|b|m|million|成)?",
    re.IGNORECASE,
)
_SINGLE_RE = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|％|兆|億|億元|萬|萬元|元|usd\$?|ntd|bn|b|m|million|成)?",
    re.IGNORECASE,
)
_CHINESE_RATIO_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*成")


@dataclass(frozen=True)
class MetricObservation:
    """Normalized metric evidence extracted from a record."""

    metric: str
    event_key: str
    event_phase: str
    value_low: float | None
    value_high: float | None
    unit: str
    source_text: str
    source_headline: str
    source_record_index: int
    source_kind: str
    score: float

    @property
    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "event_key": self.event_key,
            "event_phase": self.event_phase,
            "value_low": self.value_low,
            "value_high": self.value_high,
            "unit": self.unit,
            "source_text": self.source_text,
            "source_headline": self.source_headline,
            "source_record_index": self.source_record_index,
            "source_kind": self.source_kind,
            "score": self.score,
        }


def normalize_metric_name(text: str) -> str:
    """Normalize a metric label or alias into a canonical metric name.

    Returns an empty string when the input does not map to a supported metric.
    """
    cleaned = " ".join(text.strip().lower().split())
    for metric, aliases in _METRIC_ALIASES.items():
        if cleaned == metric or cleaned in aliases:
            return metric
    return ""


def extract_metric_observations(record: dict[str, Any], record_index: int = 0) -> list[dict[str, Any]]:
    """Extract canonical metric observations from a structured record.

    Structured fields take precedence. If they are absent, the function falls
    back to conservative keyword-and-number matching over the record text.
    """
    event_key = str(record.get("event_key", "")).strip()
    event_phase = normalize_event_phase(str(record.get("event_phase", "")).strip())
    source_kind = str(record.get("source_type") or record.get("source_kind") or "").strip()

    observations: list[MetricObservation] = []

    structured_metrics = _extract_structured_metrics(record)
    for metric, payload in structured_metrics.items():
        if metric not in SUPPORTED_METRICS:
            continue
        observation = _observation_from_payload(
            metric=metric,
            payload=payload,
            event_key=event_key,
            event_phase=event_phase,
            source_kind=source_kind,
            record_index=record_index,
            source_text=_record_text(record),
            source_headline=str(record.get("headline", "")).strip(),
            score=100.0,
        )
        if observation:
            observations.append(observation)

    text = _record_text(record)
    if text:
        for metric in SUPPORTED_METRICS:
            if metric in structured_metrics:
                continue
            if event_phase and event_phase not in _EVENT_PHASES:
                continue
            observations.extend(
                _extract_text_observations(
                    metric=metric,
                    text=text,
                    event_key=event_key,
                    event_phase=event_phase,
                    source_kind=source_kind,
                    record_index=record_index,
                    headline=str(record.get("headline", "")).strip(),
                )
            )

    return [ob.as_dict for ob in observations]


def analyze_expectation_vs_actual(
    records: list[dict[str, Any]],
    event_key: str,
    event_type: str = "法說會",
) -> dict[str, Any]:
    """Compare event-day actuals against pre-event expectations.

    The function is intentionally conservative:
    - only compares records for the same ``event_key``
    - only uses ``pre_event`` and ``event_day`` phases
    - returns ``unknown`` when a metric cannot be compared numerically
    """
    normalized_event_key = str(event_key).strip()
    if not normalized_event_key:
        raise ValueError("event_key is required for expectation analysis")

    filtered_records = _filter_records_for_event(records, normalized_event_key)
    deterministic_observations = [
        observation
        for index, record in enumerate(filtered_records)
        for observation in extract_metric_observations(record, record_index=index)
    ]
    hybrid_error = ""
    try:
        hybrid_observations = _extract_hybrid_observations(filtered_records)
    except Exception as exc:
        hybrid_observations = []
        hybrid_error = type(exc).__name__
    observations = deterministic_observations + hybrid_observations

    grouped = defaultdict(lambda: {"pre_event": [], "event_day": []})
    for observation in observations:
        metric = observation["metric"]
        phase = observation["event_phase"]
        if metric in SUPPORTED_METRICS and phase in _EVENT_PHASES:
            grouped[metric][phase].append(observation)

    metric_results: list[dict[str, Any]] = []
    pre_event_rows: list[dict[str, Any]] = []
    event_day_rows: list[dict[str, Any]] = []
    data_gaps: list[str] = []

    for metric in SUPPORTED_METRICS:
        expectation = _select_best_observation(grouped[metric]["pre_event"])
        actual = _select_best_observation(grouped[metric]["event_day"])
        result = _compare_metric(metric, expectation, actual)
        metric_results.append(result)
        if expectation:
            pre_event_rows.append(_format_observation_row(metric, expectation, "pre_event_expectations"))
        if actual:
            event_day_rows.append(_format_observation_row(metric, actual, "event_day_actuals"))
        if result["status"] == "unknown":
            data_gaps.append(f"{metric}_comparison_unavailable")

    status_counts = Counter(result["status"] for result in metric_results)
    if not filtered_records:
        data_gaps.append("no_records_for_event_key")
    if not pre_event_rows:
        data_gaps.append("pre_event_expectations_missing")
    if not event_day_rows:
        data_gaps.append("event_day_actuals_missing")

    return {
        "analysis_target": {
            "event_type": event_type,
            "event_key": normalized_event_key,
        },
        "comparison_mode": "expectation_vs_actual",
        "records_considered": len(filtered_records),
        "observations_considered": len(observations),
        "deterministic_observations_considered": len(deterministic_observations),
        "hybrid_observations_considered": len(hybrid_observations),
        "pre_event_expectations": pre_event_rows,
        "event_day_actuals": event_day_rows,
        "metrics": metric_results,
        "comparison_rows": [_format_comparison_row(result) for result in metric_results],
        "status_counts": dict(status_counts),
        "summary": _summarize_metric_statuses(status_counts),
        "comparison_summary": _summarize_metric_statuses(status_counts),
        "data_gaps": _dedupe_preserve_order(data_gaps),
        "hybrid_enabled": bool(os.environ.get("GEMINI_API_KEY")),
        "hybrid_error": hybrid_error,
    }


def normalize_event_phase(value: str) -> str:
    """Normalize event phases into the canonical internal form."""
    cleaned = " ".join(value.strip().lower().split())
    alias_map = {
        "pre-event": "pre_event",
        "pre event": "pre_event",
        "pre_event": "pre_event",
        "event-day": "event_day",
        "event day": "event_day",
        "event_day": "event_day",
        "day-of": "event_day",
        "day of": "event_day",
        "post-event": "post_event",
        "post event": "post_event",
        "post_event": "post_event",
    }
    return alias_map.get(cleaned, cleaned)


def _filter_records_for_event(records: list[dict[str, Any]], event_key: str) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for record in records:
        record_key = str(record.get("event_key", "")).strip()
        if record_key and record_key != event_key:
            continue
        phase = normalize_event_phase(str(record.get("event_phase", "")).strip())
        if phase not in _EVENT_PHASES:
            continue
        filtered.append(record)
    return filtered


def _record_text(record: dict[str, Any]) -> str:
    parts = [str(record.get(field, "")).strip() for field in _TEXT_FIELDS]
    return " ".join(part for part in parts if part)


def _extract_structured_metrics(record: dict[str, Any]) -> dict[str, Any]:
    structured: dict[str, Any] = {}

    metrics_payload = record.get("metrics")
    if isinstance(metrics_payload, dict):
        for key, payload in metrics_payload.items():
            metric = normalize_metric_name(str(key))
            if metric:
                structured[metric] = payload

    for field_name in ("expected_metrics", "actual_metrics"):
        payload = record.get(field_name)
        if isinstance(payload, dict):
            for key, value in payload.items():
                metric = normalize_metric_name(str(key))
                if metric and metric not in structured:
                    structured[metric] = value

    metric_name = normalize_metric_name(str(record.get("metric_name", "")))
    if metric_name and ("metric_value" in record or "metric_low" in record or "metric_high" in record):
        structured[metric_name] = {
            "value": record.get("metric_value"),
            "value_low": record.get("metric_low"),
            "value_high": record.get("metric_high"),
            "unit": record.get("metric_unit"),
        }

    return structured


def _observation_from_payload(
    metric: str,
    payload: Any,
    event_key: str,
    event_phase: str,
    source_kind: str,
    record_index: int,
    source_text: str,
    source_headline: str,
    score: float,
) -> MetricObservation | None:
    if isinstance(payload, dict):
        value_low = _coerce_number(payload.get("value_low", payload.get("low", payload.get("min"))))
        value_high = _coerce_number(payload.get("value_high", payload.get("high", payload.get("max"))))
        value = _coerce_number(payload.get("value"))
        unit = str(payload.get("unit") or payload.get("value_unit") or "").strip().lower()
    else:
        value_low = value_high = None
        value = _coerce_number(payload)
        unit = ""

    if value_low is None and value_high is None and value is None:
        return None

    if value is not None:
        value_low = value_low if value_low is not None else value
        value_high = value_high if value_high is not None else value
    elif value_low is not None and value_high is None:
        value_high = value_low
    elif value_high is not None and value_low is None:
        value_low = value_high

    normalized_unit = _normalize_unit(unit)
    return MetricObservation(
        metric=metric,
        event_key=event_key,
        event_phase=event_phase,
        value_low=value_low,
        value_high=value_high,
        unit=normalized_unit,
        source_text=source_text,
        source_headline=source_headline,
        source_record_index=record_index,
        source_kind=source_kind,
        score=score,
    )


def _extract_text_observations(
    metric: str,
    text: str,
    event_key: str,
    event_phase: str,
    source_kind: str,
    record_index: int,
    headline: str,
) -> list[MetricObservation]:
    aliases = _METRIC_ALIASES[metric]
    observations: list[MetricObservation] = []
    lowered = text.lower()
    for alias in aliases:
        alias_lower = alias.lower()
        start = 0
        while True:
            idx = lowered.find(alias_lower, start)
            if idx == -1:
                break
            tail_window = text[idx : min(len(text), idx + 140)]
            payload = _parse_metric_value_from_window(metric, tail_window)
            source_window = tail_window
            if not payload:
                head_window = text[max(0, idx - 30) : idx + len(alias_lower)]
                payload = _parse_metric_value_from_window(metric, head_window)
                source_window = head_window
            if payload:
                observations.append(
                    MetricObservation(
                        metric=metric,
                        event_key=event_key,
                        event_phase=event_phase,
                        value_low=payload["value_low"],
                        value_high=payload["value_high"],
                        unit=payload["unit"],
                        source_text=source_window.strip(),
                        source_headline=headline,
                        source_record_index=record_index,
                        source_kind=source_kind,
                        score=payload["score"],
                    )
                )
            start = idx + len(alias_lower)
    return observations


def _parse_metric_value_from_window(metric: str, window: str) -> dict[str, Any] | None:
    context = window.lower()

    candidates: list[tuple[int, int, dict[str, Any]]] = []

    for match in _RANGE_RE.finditer(context):
        unit = _normalize_unit(match.group("unit") or "")
        low = _coerce_number(match.group("low"))
        high = _coerce_number(match.group("high"))
        if low is None or high is None:
            continue
        low, high = sorted((low, high))
        candidates.append(
            (
                match.start(),
                0,
                {
                    "value_low": low,
                    "value_high": high,
                    "unit": unit,
                    "score": 60.0 if unit else 55.0,
                },
            )
        )

    for match in _CHINESE_RATIO_RE.finditer(context):
        value = _coerce_number(match.group("value"))
        if value is None:
            continue
        candidates.append(
            (
                match.start(),
                1,
                {
                    "value_low": value * 10.0,
                    "value_high": value * 10.0,
                    "unit": "%",
                    "score": 50.0,
                },
            )
        )

    for match in _SINGLE_RE.finditer(context):
        unit = _normalize_unit(match.group("unit") or "")
        value = _coerce_number(match.group("value"))
        if value is None:
            continue
        score = 45.0 if unit else 35.0
        if metric == "guidance":
            score += 5.0
        if unit == "%" and metric in {"revenue", "guidance"}:
            score += 5.0
        if unit in {"億", "億元", "兆", "萬", "萬元", "元", "usd", "ntd", "bn", "b", "m"} and metric in {"revenue", "capex", "guidance"}:
            score += 5.0
        candidates.append(
            (
                match.start(),
                2,
                {
                    "value_low": value,
                    "value_high": value,
                    "unit": unit,
                    "score": score,
                },
            )
        )

    if not candidates:
        return None

    _, _, best = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
    return best


def _select_best_observation(observations: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not observations:
        return None
    return sorted(
        observations,
        key=lambda item: (
            float(item.get("score", 0.0)),
            float(item.get("confidence", 0.0)),
            _observation_span(item),
            -int(item.get("source_record_index", 0)),
        ),
        reverse=True,
    )[0]


def _compare_metric(
    metric: str,
    expectation: dict[str, Any] | None,
    actual: dict[str, Any] | None,
) -> dict[str, Any]:
    result = {
        "metric": metric,
        "status": "unknown",
        "comparison_direction": _METRIC_POLARITY[metric],
        "expectation": expectation,
        "actual": actual,
    }

    if not expectation or not actual:
        return result

    if not _units_compatible(expectation.get("unit", ""), actual.get("unit", "")):
        return result

    exp_low, exp_high = _observation_bounds(expectation)
    act_low, act_high = _observation_bounds(actual)
    if exp_low is None or exp_high is None or act_low is None or act_high is None:
        return result

    if _intervals_overlap(exp_low, exp_high, act_low, act_high):
        result["status"] = "matched"
        return result

    polarity = _METRIC_POLARITY[metric]
    if polarity == "higher":
        if act_low > exp_high:
            result["status"] = "beat"
        elif act_high < exp_low:
            result["status"] = "below"
        else:
            result["status"] = "partially_matched"
        return result

    # Neutral metrics such as capex are intentionally conservative.
    result["status"] = "partially_matched"
    return result


def _format_observation(observation: dict[str, Any] | None) -> str:
    if not observation:
        return "-"

    low = _coerce_number(observation.get("value_low"))
    high = _coerce_number(observation.get("value_high"))
    unit = str(observation.get("unit", "")).strip()
    if low is None and high is None:
        return "-"
    if low is None:
        low = high
    if high is None:
        high = low
    if low is None or high is None:
        return "-"
    if low == high:
        return f"{low:g}{f' {unit}' if unit else ''}"
    return f"{low:g} ~ {high:g}{f' {unit}' if unit else ''}"


def _format_observation_row(section_metric: str, observation: dict[str, Any], section_key: str) -> dict[str, Any]:
    return {
        "metric_name": section_metric,
        "content": _format_observation(observation),
        "source_name": str(observation.get("source_headline", "")).strip() or str(observation.get("source_kind", "")).strip(),
        "source_kind": observation.get("source_kind", ""),
        "event_phase": observation.get("event_phase", ""),
        "event_key": observation.get("event_key", ""),
        "confidence": observation.get("confidence", 0.0),
        "source_record_index": observation.get("source_record_index", 0),
        "section": section_key,
    }


def _format_comparison_row(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "metric_name": result.get("metric", ""),
        "expectation": _format_observation(result.get("expectation")),
        "actual": _format_observation(result.get("actual")),
        "expectation_match": result.get("status", ""),
        "comparison_direction": result.get("comparison_direction", ""),
    }


def _observation_bounds(observation: dict[str, Any]) -> tuple[float | None, float | None]:
    low = _coerce_number(observation.get("value_low"))
    high = _coerce_number(observation.get("value_high"))
    if low is None and high is None:
        return None, None
    if low is None:
        low = high
    if high is None:
        high = low
    if low is None or high is None:
        return None, None
    return (low, high) if low <= high else (high, low)


def _observation_span(observation: dict[str, Any]) -> float:
    low, high = _observation_bounds(observation)
    if low is None or high is None:
        return float("inf")
    return abs(high - low)


def _intervals_overlap(low_a: float, high_a: float, low_b: float, high_b: float) -> bool:
    return low_a <= high_b and low_b <= high_a


def _units_compatible(unit_a: str, unit_b: str) -> bool:
    category_a = _unit_category(unit_a)
    category_b = _unit_category(unit_b)
    return category_a == category_b or not category_a or not category_b


def _unit_category(unit: str) -> str:
    normalized = _normalize_unit(unit)
    if not normalized:
        return ""
    if normalized in _PERCENT_UNITS:
        return "percent"
    if normalized in _AMOUNT_UNITS or normalized in {"usd", "usd$", "ntd"}:
        return "amount"
    return normalized


def _normalize_unit(unit: str) -> str:
    cleaned = str(unit).strip().lower()
    replacements = {
        "％": "%",
        "percent": "%",
        "pct": "%",
        "percentage": "%",
        "成": "%",
        "usd$": "usd",
        "usd": "usd",
        "ntd": "ntd",
        "bn": "bn",
        "b": "b",
        "m": "m",
        "million": "m",
    }
    return replacements.get(cleaned, cleaned)


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _summarize_metric_statuses(status_counts: Counter) -> str:
    if not status_counts:
        return "尚無可比較的指標。"
    order = ("beat", "matched", "partially_matched", "below", "unknown")
    parts = [f"{status}: {status_counts.get(status, 0)}" for status in order if status_counts.get(status, 0)]
    return "；".join(parts) if parts else "尚無可比較的指標。"


def _extract_hybrid_observations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Use LLM normalization on candidate records that likely contain metrics."""
    if not os.environ.get("GEMINI_API_KEY"):
        return []

    candidate_records = [record for record in records if _record_likely_contains_metrics(record)]
    if not candidate_records:
        return []

    try:
        import google.genai as genai
        from google.genai import types
    except Exception:
        return []

    prompt = _build_hybrid_prompt(candidate_records)
    schema = {
        "type": "object",
        "properties": {
            "observations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "metric_name": {"type": "string"},
                        "event_phase": {"type": "string"},
                        "event_key": {"type": "string"},
                        "value": {"type": "number"},
                        "value_low": {"type": "number"},
                        "value_high": {"type": "number"},
                        "unit": {"type": "string"},
                        "direction": {"type": "string"},
                        "is_expectation": {"type": "boolean"},
                        "is_actual": {"type": "boolean"},
                        "confidence": {"type": "number"},
                        "evidence_span": {"type": "string"},
                        "source_record_index": {"type": "integer"},
                    },
                    "required": ["metric_name", "event_phase", "event_key", "source_record_index"],
                },
            }
        },
        "required": ["observations"],
    }

    timeout_seconds = float(os.environ.get("EXPECTATION_HYBRID_TIMEOUT_SECONDS", "8"))

    def _run_hybrid() -> str:
        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        model_id = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=schema,
                temperature=0.0,
            ),
        )
        return getattr(response, "text", "") or ""

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            text = executor.submit(_run_hybrid).result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        raise TimeoutError("hybrid expectation extraction timed out") from exc
    except Exception:
        return []

    if not text:
        return []
    try:
        payload = __import__("json").loads(text)
    except Exception:
        return []

    observations: list[dict[str, Any]] = []
    for item in payload.get("observations", []):
        metric = normalize_metric_name(str(item.get("metric_name", "")))
        if not metric:
            continue
        evidence_span = str(item.get("evidence_span", "")).strip()
        if not evidence_span:
            continue
        source_index = int(item.get("source_record_index", 0))
        source_record = candidate_records[source_index] if 0 <= source_index < len(candidate_records) else {}
        event_phase = normalize_event_phase(str(item.get("event_phase", "")))
        if event_phase not in _EVENT_PHASES:
            continue
        observations.append(
            {
                "metric": metric,
                "event_key": str(item.get("event_key", "")).strip(),
                "event_phase": event_phase,
                "value_low": _coerce_number(item.get("value_low", item.get("value"))),
                "value_high": _coerce_number(item.get("value_high", item.get("value"))),
                "unit": _normalize_unit(str(item.get("unit", ""))),
                "source_text": evidence_span,
                "source_headline": str(source_record.get("headline", "")).strip(),
                "source_record_index": source_index,
                "source_kind": str(source_record.get("source_type") or source_record.get("source_kind") or "").strip(),
                "score": 80.0,
                "confidence": float(item.get("confidence", 0.0) or 0.0),
                "direction": str(item.get("direction", "")).strip(),
                "is_expectation": bool(item.get("is_expectation", False)),
                "is_actual": bool(item.get("is_actual", False)),
                "evidence_span": evidence_span,
                "hybrid_extracted": True,
            }
        )
    return observations


def _record_likely_contains_metrics(record: dict[str, Any]) -> bool:
    text = _record_text(record)
    if not text:
        return False
    if any(alias.lower() in text.lower() for aliases in _METRIC_ALIASES.values() for alias in aliases):
        return True
    return bool(re.search(r"\d", text))


def _build_hybrid_prompt(records: list[dict[str, Any]]) -> str:
    lines = [
        "Extract structured earnings-call expectation/actual observations.",
        "Return only high-confidence observations with exact evidence spans from the input records.",
        "Supported metrics: revenue, gross_margin, operating_margin, eps, capex, guidance.",
    ]
    for index, record in enumerate(records):
        lines.append(
            f"[Record {index}] event_key={record.get('event_key','')} phase={record.get('event_phase','')} "
            f"headline={record.get('headline','')} summary={record.get('summary','')}"
        )
    return "\n".join(lines)


__all__ = [
    "SUPPORTED_METRICS",
    "analyze_expectation_vs_actual",
    "extract_metric_observations",
    "normalize_event_phase",
    "normalize_metric_name",
]
