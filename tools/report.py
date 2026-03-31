"""Save event study reports (Markdown + JSON) to outputs/."""

import json
from datetime import datetime
from pathlib import Path


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
