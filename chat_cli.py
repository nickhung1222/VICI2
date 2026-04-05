"""Interactive chat-style CLI for VICI2 workflows."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from tools.schemas import normalize_event_key, normalize_symbol

_DATE_PATTERN = re.compile(r"\b(\d{4})[-/](\d{2})[-/](\d{2})\b")
_DATE_RANGE_PATTERN = re.compile(
    r"(\d{4}[-/]\d{2}[-/]\d{2})\s*(?:到|至|~|～|-)\s*(\d{4}[-/]\d{2}[-/]\d{2})"
)
_QUARTER_PATTERN = re.compile(r"\b(\d{4})\s*[-_/ ]?\s*[Qq]([1-4])\b")
_STOCK_PATTERN = re.compile(r"\b(\d{4})(?:\.TW)?\b", re.IGNORECASE)

_HELP_COMMANDS = {"help", "/help", "?", "可以做什麼", "你可以做什麼", "能做什麼", "範例", "examples"}
_EXIT_COMMANDS = {"exit", "quit", "/exit", "離開", "結束"}
_YES_COMMANDS = {"y", "yes", "ok", "run", "好", "是", "確認"}

_STOCK_ALIASES = {
    "2330": "台積電",
    "2454": "聯發科",
    "2303": "聯電",
    "2317": "鴻海",
    "3711": "日月光投控",
    "2382": "廣達",
    "2308": "台達電",
    "2412": "中華電",
    "2379": "瑞昱",
    "3231": "緯創",
}
_NAME_TO_STOCK = {name: f"{code}.TW" for code, name in _STOCK_ALIASES.items()}
_NAME_TO_STOCK.update(
    {
        "tsmc": "2330.TW",
        "mediatek": "2454.TW",
        "umc": "2303.TW",
        "foxconn": "2317.TW",
    }
)


@dataclass
class ChatRequest:
    mode: str = ""
    stock: str = ""
    stock_name: str = ""
    event_type: str = "法說會"
    start_date: str = ""
    end_date: str = ""
    event_date: str = ""
    event_key: str = ""
    comparison_event_date: str = ""
    phase: str = "both"
    include_event_study: bool = False
    event_dates: list[str] = field(default_factory=list)
    topic: str = ""
    max_results: int | None = None
    notes: list[str] = field(default_factory=list)


def run_chat_mode() -> int:
    """Start the interactive chat loop."""
    print(render_welcome())

    while True:
        try:
            user_input = input("\nchat> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n已離開 chat mode。")
            return 0

        if not user_input:
            continue
        if user_input.lower() in _EXIT_COMMANDS or user_input in _EXIT_COMMANDS:
            print("已離開 chat mode。")
            return 0
        if user_input.lower() in _HELP_COMMANDS or user_input in _HELP_COMMANDS:
            print(render_help())
            continue

        request = parse_chat_request(user_input)
        if not request.mode:
            print("目前只能幫你啟動 event_report、heat_scan、event_collect、event_study。輸入 `help` 看範例。")
            continue

        hydrate_request_interactively(request)
        apply_request_defaults(request)

        print("\n我準備執行這個任務：")
        print(render_request_summary(request))
        confirm = input("\n要開始執行嗎？ [Y/n]: ").strip()
        if confirm and confirm.casefold() not in _YES_COMMANDS and confirm not in _YES_COMMANDS:
            print("已取消本次執行。")
            continue

        execute_request(request)


def render_welcome() -> str:
    return (
        "VICI Chat Mode\n"
        "\n"
        "我目前可以幫你做：\n"
        "1. 產生法說會事件報告\n"
        "2. 做事件前後熱度分析\n"
        "3. 蒐集指定事件期間的新聞\n"
        "4. 做 event study 驗證分析\n"
        "\n"
        "你可以這樣說：\n"
        "- 幫我分析台積電 2025Q1 法說會\n"
        "- 幫我做聯發科法說會前後熱度分析\n"
        "- 蒐集鴻海 2025-03-01 到 2025-03-20 的法說會新聞\n"
        "- 做台積電 2025-01-16, 2025-04-17 的 event study\n"
        "\n"
        "目前限制：\n"
        "- 主要是法說會 workflow，不是通用股票助理\n"
        "- 新聞資料實際可用區間主要自 2024-10 起，更早日期常見結果是空報告或資料不足\n"
        "- 法說會 event_key 歷史季度比對目前正式支援範圍以台灣市值前 10 大公司、2024Q3 之後為主\n"
        "- 若股票名稱不在目前內建常見名單，我會要求你補股票代碼或 Yahoo symbol\n"
        "- chat mode 會先盡量解析需求，但真正能不能跑出結果仍取決於底層資料來源是否有資料\n"
        "\n"
        "輸入 `help` 可再看一次範例，輸入 `exit` 離開。"
    )


def render_help() -> str:
    return (
        "\n支援的任務：\n"
        "- `event_report`：法說會主報告\n"
        "- `heat_scan`：事件前後熱度分析\n"
        "- `event_collect`：收集事件相關新聞\n"
        "- `event_study`：AR / CAR 驗證分析\n"
        "\n"
        "建議問法：\n"
        "- 幫我分析台積電 2025Q1 法說會\n"
        "- 幫我做 2454.TW 2025-04-30 法說會熱度分析\n"
        "- 蒐集 2317 2025-03-01 到 2025-03-20 的法說會新聞\n"
        "- 幫我做 2330.TW 2025-01-16,2025-04-17 的 event study\n"
        "\n"
        "限制說明：\n"
        "- 資料覆蓋主要從 2024-10 開始，較早事件可能只有空結果或資料不足\n"
        "- 法說會 historical resolver 目前偏向台灣市值前 10 大公司，且以 2024Q3 之後為主\n"
        "- 不在內建 alias 的股票名稱，請直接給我 `2330` 或 `2330.TW` 這類代碼\n"
        "- 若你給的是模糊要求，我會追問事件日、日期區間或季度 key\n"
        "- chat mode 不是保證成功的資料層；若來源抓不到資料，最終輸出仍會反映資料不足\n"
        "\n"
        "輸入格式越完整越好：股票、季度、事件日、日期區間。缺少必要資訊時我會追問。"
    )


def parse_chat_request(text: str) -> ChatRequest:
    request = ChatRequest()
    stripped = text.strip()
    lowered = stripped.casefold()

    request.mode = _detect_mode(lowered, stripped)
    request.stock, request.stock_name = _extract_stock(stripped)
    request.event_key = _extract_event_key(stripped)
    request.phase = _extract_phase(lowered)
    request.include_event_study = "含event study" in lowered or "include event study" in lowered
    request.start_date, request.end_date = _extract_date_range(stripped)

    all_dates = _extract_dates(stripped)
    if request.mode == "event_study":
        request.event_dates = all_dates
    elif request.mode == "event_collect":
        if all_dates and not request.start_date:
            request.event_date = all_dates[-1]
    else:
        if all_dates:
            request.event_date = all_dates[-1]

    if request.mode == "event_report":
        request.topic = f"{request.stock_name or request.stock or '事件'}法說會"
    elif request.mode == "event_study":
        request.topic = f"{request.stock_name or request.stock or '事件'}法說會"

    return request


def hydrate_request_interactively(request: ChatRequest) -> None:
    if not request.stock:
        stock_input = input("請輸入股票代碼或 Yahoo symbol，例如 2330 或 2330.TW：").strip()
        if stock_input:
            request.stock = normalize_symbol(stock_input)
            request.stock_name = request.stock_name or _STOCK_ALIASES.get(request.stock.split(".", 1)[0], "")

    if request.stock and not request.stock_name:
        inferred_name = _STOCK_ALIASES.get(request.stock.split(".", 1)[0], "")
        if inferred_name:
            request.stock_name = inferred_name
        else:
            request.stock_name = input("股票中文名稱（可留空）：").strip()

    if request.mode in {"event_report", "heat_scan", "event_collect"} and not request.event_key:
        event_key = input("季度 event key（例如 2025Q1，可留空）：").strip()
        if event_key:
            request.event_key = normalize_event_key(request.event_type, event_key)

    if request.mode in {"event_report", "heat_scan"} and not request.event_date:
        request.event_date = _prompt_date("事件日期 YYYY-MM-DD：")

    if request.mode == "event_collect" and not request.event_date:
        event_date = input("事件日期 YYYY-MM-DD（可留空）：").strip()
        request.event_date = _normalize_date_string(event_date)

    if request.mode == "event_study" and not request.event_dates:
        raw_dates = input("請輸入事件日期，格式 YYYY-MM-DD,YYYY-MM-DD：").strip()
        request.event_dates = _extract_dates(raw_dates)

    if request.mode in {"event_report", "event_collect"} and (not request.start_date or not request.end_date):
        if request.event_date:
            request.notes.append("未提供日期區間，將使用 event_date 的預設視窗。")
        else:
            request.start_date = _prompt_date("開始日期 YYYY-MM-DD：")
            request.end_date = _prompt_date("結束日期 YYYY-MM-DD：")


def apply_request_defaults(request: ChatRequest) -> None:
    if request.event_key:
        request.event_key = normalize_event_key(request.event_type, request.event_key)

    if request.mode in {"event_report", "event_collect"} and request.event_date and (not request.start_date or not request.end_date):
        event_dt = datetime.strptime(request.event_date, "%Y-%m-%d")
        request.start_date = request.start_date or (event_dt - timedelta(days=16)).strftime("%Y-%m-%d")
        default_end_offset = 7 if request.mode == "event_report" else 3
        request.end_date = request.end_date or (event_dt + timedelta(days=default_end_offset)).strftime("%Y-%m-%d")

    if request.mode == "event_study" and not request.topic:
        request.topic = f"{request.stock_name or request.stock or '事件'}法說會"


def render_request_summary(request: ChatRequest) -> str:
    lines = [
        f"- mode: {request.mode}",
        f"- stock: {request.stock or '-'}",
        f"- stock_name: {request.stock_name or '-'}",
    ]

    if request.mode == "event_study":
        lines.append(f"- event_dates: {', '.join(request.event_dates) if request.event_dates else '-'}")
        lines.append(f"- topic: {request.topic or '-'}")
    else:
        lines.extend(
            [
                f"- event_type: {request.event_type}",
                f"- event_key: {request.event_key or '-'}",
                f"- event_date: {request.event_date or '-'}",
            ]
        )
        if request.mode in {"event_report", "event_collect"}:
            lines.append(f"- start_date: {request.start_date or '-'}")
            lines.append(f"- end_date: {request.end_date or '-'}")
        if request.mode == "heat_scan":
            lines.append(f"- phase: {request.phase}")
        if request.mode == "event_report":
            lines.append(f"- include_event_study: {request.include_event_study}")

    for note in request.notes:
        lines.append(f"- note: {note}")
    return "\n".join(lines)


def execute_request(request: ChatRequest) -> None:
    from agent import event_study
    from pipeline import event_collect, event_report, heat_scan

    if request.mode == "event_collect":
        output_path = event_collect(
            stock=request.stock,
            event_type=request.event_type,
            start_date=request.start_date,
            end_date=request.end_date,
            stock_name=request.stock_name,
            event_date=request.event_date,
            event_key=request.event_key,
        )
        print(f"\n✓ Event records saved: {output_path}")
        return

    if request.mode == "heat_scan":
        output_path = heat_scan(
            stock=request.stock,
            event_type=request.event_type,
            event_date=request.event_date,
            stock_name=request.stock_name,
            event_key=request.event_key,
            comparison_event_date=request.comparison_event_date,
            phase=request.phase,
        )
        print(f"\n✓ Heat analysis saved: {output_path}")
        return

    if request.mode == "event_report":
        output_paths = event_report(
            stock=request.stock,
            event_type=request.event_type,
            start_date=request.start_date,
            end_date=request.end_date,
            event_date=request.event_date,
            stock_name=request.stock_name,
            event_key=request.event_key,
            comparison_event_date=request.comparison_event_date,
            include_event_study=request.include_event_study,
            topic=request.topic,
        )
        print(f"\n✓ Event report JSON saved: {output_paths['json_path']}")
        print(f"✓ Event report Markdown saved: {output_paths['markdown_path']}")
        return

    if request.mode == "event_study":
        report_path = event_study(
            stock=request.stock,
            event_dates=request.event_dates,
            topic=request.topic,
        )
        if report_path:
            print(f"\n✓ Report saved: {report_path}")
        else:
            print("\n⚠ Analysis completed but no report was saved.")
        return

    print("無法辨識任務，請輸入 `help` 查看範例。")


def _detect_mode(lowered: str, original: str) -> str:
    if "event study" in lowered or " ar " in f" {lowered} " or " car " in f" {lowered} ":
        return "event_study"
    if "熱度" in original or "heat" in lowered:
        return "heat_scan"
    if any(keyword in original for keyword in ("蒐集", "收集", "整理新聞", "抓新聞")) or "collect" in lowered:
        return "event_collect"
    if any(keyword in original for keyword in ("分析", "報告", "法說會")):
        return "event_report"
    return ""


def _extract_stock(text: str) -> tuple[str, str]:
    stock_name = ""
    for alias, symbol in _NAME_TO_STOCK.items():
        if alias.casefold() in text.casefold():
            stock_name = _STOCK_ALIASES.get(symbol.split(".", 1)[0], alias.upper() if alias.isascii() else alias)
            return symbol, stock_name

    stock_match = _STOCK_PATTERN.search(text)
    if not stock_match:
        return "", ""

    stock = normalize_symbol(stock_match.group(1))
    stock_name = _STOCK_ALIASES.get(stock.split(".", 1)[0], "")
    return stock, stock_name


def _extract_event_key(text: str) -> str:
    match = _QUARTER_PATTERN.search(text)
    if not match:
        return ""
    return f"{match.group(1)}Q{match.group(2)}"


def _extract_phase(lowered: str) -> str:
    if "pre_event" in lowered or "事件前" in lowered:
        return "pre_event"
    if "post_event" in lowered or "事件後" in lowered:
        return "post_event"
    return "both"


def _extract_date_range(text: str) -> tuple[str, str]:
    match = _DATE_RANGE_PATTERN.search(text)
    if not match:
        return "", ""
    return _normalize_date_string(match.group(1)), _normalize_date_string(match.group(2))


def _extract_dates(text: str) -> list[str]:
    return [_normalize_date_string(match.group(0)) for match in _DATE_PATTERN.finditer(text)]


def _prompt_date(prompt: str) -> str:
    while True:
        value = _normalize_date_string(input(prompt).strip())
        if value:
            return value
        print("日期格式需為 YYYY-MM-DD。")


def _normalize_date_string(value: str) -> str:
    if not value:
        return ""
    try:
        return datetime.strptime(value.replace("/", "-"), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""
