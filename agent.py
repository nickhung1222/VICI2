"""LLM Orchestrator: Gemini tool-use loop for experimental LLM-driven modes.

Handles event_study and news_scan modes. These are retained as experimental
or optional paths, while the deterministic primary workflow
(event_collect, heat_scan, event_report) lives in pipeline.py.

Provider: Google Gemini (configured via GEMINI_API_KEY in .env).
"""

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

from tools.chart import generate_car_chart
from tools.event_study import run_event_study
from tools.news_scraper import fetch_article_content, search_news
from tools.report import save_report
from tools.stock_data import fetch_stock_data

# ---------------------------------------------------------------------------
# Tool definitions (provider-agnostic JSON schema)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "scrape_news",
        "description": (
            "搜尋台灣財經新聞。優先從鉅亨網與結構化 archive 路徑取得相關新聞列表。"
            "回傳每篇文章的標題、日期、來源、URL、摘要，以及 cnyes news_id（可用於取得完整內容）。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "搜尋關鍵字，例如 '台積電法說會' 或 '央行升息'",
                },
                "date_from": {
                    "type": "string",
                    "description": "開始日期 YYYY-MM-DD（選填）",
                },
                "date_to": {
                    "type": "string",
                    "description": "結束日期 YYYY-MM-DD（選填）",
                },
                "max_results": {
                    "type": "integer",
                    "description": "最多回傳幾篇文章（預設 20）",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "fetch_article_content",
        "description": (
            "取得單篇新聞文章的完整內容。"
            "如果有 news_id（來自 cnyes），優先用 API 取得乾淨文字；否則用 URL 抓取。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "文章完整 URL",
                },
                "news_id": {
                    "type": "string",
                    "description": "cnyes 新聞 ID（選填，有的話優先使用）",
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "fetch_stock_data",
        "description": (
            "取得台股歷史價格資料和台灣加權指數（大盤）。"
            "回傳每日報酬率，用於後續 Event Study 計算。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Yahoo Finance 台股代碼，例如 '2330.TW'（台積電）、'2884.TW'（富邦金）",
                },
                "start_date": {
                    "type": "string",
                    "description": "開始日期 YYYY-MM-DD（建議比最早事件日提前至少 180 天）",
                },
                "end_date": {
                    "type": "string",
                    "description": "結束日期 YYYY-MM-DD",
                },
            },
            "required": ["symbol", "start_date", "end_date"],
        },
    },
    {
        "name": "run_event_study",
        "description": (
            "執行事件研究：使用市場模型（OLS）計算每個事件日的超額報酬（AR）和累積超額報酬（CAR）。"
            "估計窗口：事件日前 130 到 11 個交易日（120 天）。"
            "事件窗口：[-5, +5] 個交易日。"
            "多個事件日時，計算平均 CAR 和 cross-sectional t 統計量。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "stock_returns": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "每日股票報酬率列表（來自 fetch_stock_data 的 stock_returns）",
                },
                "market_returns": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "每日大盤報酬率列表（來自 fetch_stock_data 的 market_returns）",
                },
                "dates": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "對應的日期列表 YYYY-MM-DD（來自 fetch_stock_data 的 dates）",
                },
                "event_dates": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "事件日期列表，格式 YYYY-MM-DD",
                },
                "estimation_window": {
                    "type": "integer",
                    "description": "估計窗口天數（預設 120）",
                },
                "event_window_pre": {
                    "type": "integer",
                    "description": "事件前天數（預設 5）",
                },
                "event_window_post": {
                    "type": "integer",
                    "description": "事件後天數（預設 5）",
                },
                "reaction_shift_trading_days": {
                    "type": "integer",
                    "description": "若事件在收盤後公布，將 t=0 往後平移的交易日數；例如台股盤後法說會可設為 1。",
                },
            },
            "required": ["stock_returns", "market_returns", "dates", "event_dates"],
        },
    },
    {
        "name": "generate_chart",
        "description": (
            "產生平均 CAR 走勢圖，儲存為 PNG。"
            "圖表包含：平均 CAR 線、95% 信賴區間、事件日（t=0）垂直虛線、各個事件的 CAR 灰色背景線。"
            "回傳圖表檔案路徑。"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "car_data": {
                    "type": "object",
                    "description": "run_event_study 的回傳結果",
                },
                "title": {
                    "type": "string",
                    "description": "圖表標題（中文），例如 '台積電法說會 平均 CAR [-5,+5]'",
                },
                "symbol": {
                    "type": "string",
                    "description": "股票代碼（用於檔名），例如 '2330.TW'",
                },
            },
            "required": ["car_data", "title"],
        },
    },
    {
        "name": "save_report",
        "description": "將完整分析報告儲存為 Markdown 格式，存到 outputs/reports/ 目錄。回傳檔案路徑。",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "完整 Markdown 報告內容",
                },
                "topic": {
                    "type": "string",
                    "description": "事件主題名稱（用於檔名），例如 'TSMC法說會'",
                },
            },
            "required": ["content", "topic"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executor
# ---------------------------------------------------------------------------

def execute_tool(name: str, inputs: dict) -> str:
    """Execute a tool call and return the result as a string."""
    if name == "scrape_news":
        try:
            articles = search_news(
                query=inputs["query"],
                date_from=inputs.get("date_from"),
                date_to=inputs.get("date_to"),
                max_results=inputs.get("max_results", 20),
            )
            return json.dumps(articles, ensure_ascii=False, indent=2)
        except Exception as e:
            return f"Error scraping news: {e}"

    elif name == "fetch_article_content":
        try:
            content = fetch_article_content(
                url=inputs["url"],
                news_id=inputs.get("news_id", ""),
            )
            return content[:5000]  # Truncate to avoid huge context
        except Exception as e:
            return f"Error fetching article: {e}"

    elif name == "fetch_stock_data":
        try:
            data = fetch_stock_data(
                symbol=inputs["symbol"],
                start_date=inputs["start_date"],
                end_date=inputs["end_date"],
            )
            return json.dumps(data, ensure_ascii=False)
        except Exception as e:
            return f"Error fetching stock data: {e}"

    elif name == "run_event_study":
        try:
            result = run_event_study(
                stock_returns=inputs["stock_returns"],
                market_returns=inputs["market_returns"],
                dates=inputs["dates"],
                event_dates=inputs["event_dates"],
                estimation_window=inputs.get("estimation_window", 120),
                event_window_pre=inputs.get("event_window_pre", 5),
                event_window_post=inputs.get("event_window_post", 5),
                reaction_shift_trading_days=inputs.get("reaction_shift_trading_days", 0),
            )
            return json.dumps(result, ensure_ascii=False, indent=2)
        except Exception as e:
            return f"Error running event study: {e}"

    elif name == "generate_chart":
        try:
            filepath = generate_car_chart(
                car_data=inputs["car_data"],
                title=inputs["title"],
                symbol=inputs.get("symbol", ""),
            )
            return filepath
        except Exception as e:
            return f"Error generating chart: {e}"

    elif name == "save_report":
        try:
            filepath = save_report(
                content=inputs["content"],
                topic=inputs.get("topic", "event_study"),
            )
            return filepath
        except Exception as e:
            return f"Error saving report: {e}"

    else:
        return f"Error: unknown tool '{name}'"


# ---------------------------------------------------------------------------
# System prompt loader
# ---------------------------------------------------------------------------

def _load_system_prompt() -> str:
    prompts_dir = Path(__file__).parent / "prompts"
    system_md = (prompts_dir / "system.md").read_text(encoding="utf-8")
    report_format_md = (prompts_dir / "report_format.md").read_text(encoding="utf-8")
    return f"{system_md}\n\n---\n\n## 報告格式模板\n\n{report_format_md}"


# ---------------------------------------------------------------------------
# Gemini helper
# ---------------------------------------------------------------------------

def _extract_gemini_text(candidate: Any) -> str:
    if candidate is None or getattr(candidate, "content", None) is None:
        return ""
    texts = []
    for part in candidate.content.parts or []:
        text = getattr(part, "text", None)
        if text:
            texts.append(text)
    return "\n".join(texts).strip()


def _to_gemini_schema(schema: dict) -> dict:
    """Recursively convert JSON Schema to Gemini-compatible format."""
    result = {}
    if "type" in schema:
        result["type"] = schema["type"].upper()
    if "description" in schema:
        result["description"] = schema["description"]
    if "properties" in schema:
        result["properties"] = {k: _to_gemini_schema(v) for k, v in schema["properties"].items()}
    if "required" in schema:
        result["required"] = schema["required"]
    if "items" in schema:
        result["items"] = _to_gemini_schema(schema["items"])
    return result


# ---------------------------------------------------------------------------
# Main orchestration loop (Gemini)
# ---------------------------------------------------------------------------

def _run_gemini_loop(
    client,
    model_id: str,
    contents: list,
    config,
    retry_nudge: str = "請繼續。",
    verbose_empty: bool = False,
) -> Optional[str]:
    """Shared Gemini tool-use loop. Returns the saved report path or None."""
    import google.genai as genai
    from google.genai import types

    report_path = None
    empty_count = 0
    MAX_EMPTY = 5

    while True:
        response = client.models.generate_content(
            model=model_id,
            contents=contents,
            config=config,
        )
        candidates = getattr(response, "candidates", None) or []
        candidate = candidates[0] if candidates else None

        if candidate is None or candidate.content is None or not candidate.content.parts:
            empty_count += 1
            if empty_count >= MAX_EMPTY:
                if verbose_empty:
                    print(f"  ✗ model returned empty response {MAX_EMPTY} times, stopping.")
                break
            if verbose_empty:
                print(f"  ⚠ empty response ({empty_count}/{MAX_EMPTY}), retrying...")
            contents.append(types.Content(role="user", parts=[types.Part(text=retry_nudge)]))
            continue

        empty_count = 0
        contents.append(candidate.content)

        function_calls = [
            part.function_call
            for part in candidate.content.parts
            if part.function_call is not None
        ]

        if not function_calls:
            break

        function_responses = []
        for fc in function_calls:
            tool_inputs = dict(fc.args)
            print(f"  → {fc.name}({list(tool_inputs.keys())})")
            result = execute_tool(fc.name, tool_inputs)

            if fc.name == "save_report":
                report_path = result
                print(f"  ✓ report saved: {result}")

            function_responses.append(
                types.Part.from_function_response(
                    name=fc.name,
                    response={"result": result},
                )
            )

        contents.append(types.Content(role="user", parts=function_responses))

    return report_path


def _run_event_study_gemini(
    stock: str,
    event_dates: list[str],
    topic: str,
    system_prompt: str,
) -> Optional[str]:
    import google.genai as genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    model_id = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    function_declarations = [
        types.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=_to_gemini_schema(t["input_schema"]),
        )
        for t in TOOLS
    ]
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        tools=[types.Tool(function_declarations=function_declarations)],
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    earliest_event = min(event_dates)
    latest_event = max(event_dates)
    data_start = (datetime.strptime(earliest_event, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y-%m-%d")
    data_end = (datetime.strptime(latest_event, "%Y-%m-%d") + timedelta(days=15)).strftime("%Y-%m-%d")

    user_message = (
        f"今天日期：{date.today().isoformat()}\n\n"
        f"請對以下事件執行完整的事件研究分析：\n\n"
        f"- **股票代碼**：{stock}\n"
        f"- **事件主題**：{topic}\n"
        f"- **事件日期**：{', '.join(event_dates)}\n"
        f"- **建議股價資料範圍**：{data_start} 到 {data_end}\n\n"
        f"若事件屬於台股盤後公布（例如法說會），請在呼叫 run_event_study 時將 "
        f"`reaction_shift_trading_days` 設為 1，讓下一個交易日作為 t=0。\n\n"
        f"請依照系統提示的工作流程完整執行：新聞抓取 → 情緒分析 → 股價資料 → 事件研究 → 圖表 → 儲存報告。"
    )

    contents = [types.Content(role="user", parts=[types.Part(text=user_message)])]
    return _run_gemini_loop(client, model_id, contents, config, retry_nudge="請繼續下一步。", verbose_empty=True)


def _run_news_scan_gemini(query: str, days: int, system_prompt: str) -> Optional[str]:
    import google.genai as genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    model_id = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

    function_declarations = [
        types.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=_to_gemini_schema(t["input_schema"]),
        )
        for t in TOOLS
    ]
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        tools=[types.Tool(function_declarations=function_declarations)],
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    date_from = (date.today() - timedelta(days=days)).isoformat()
    user_message = (
        f"今天日期：{date.today().isoformat()}\n\n"
        f"請搜尋以下關鍵字的近期台灣財經新聞並進行情緒分析：\n\n"
        f"- **搜尋關鍵字**：{query}\n"
        f"- **搜尋範圍**：{date_from} 到今天\n\n"
        f"執行 News Scan 模式：搜尋新聞 → 取得前 5 篇完整內容 → 情緒分析 → 儲存摘要報告。"
    )

    contents = [types.Content(role="user", parts=[types.Part(text=user_message)])]
    return _run_gemini_loop(client, model_id, contents, config, retry_nudge="請繼續。", verbose_empty=False)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def event_study(stock: str, event_dates: list[str], topic: str) -> str:
    """Run full event study: news + sentiment + AR/CAR + chart + report.

    Args:
        stock: Yahoo Finance symbol, e.g. '2330.TW'
        event_dates: List of event dates ['YYYY-MM-DD', ...]
        topic: Event description, e.g. 'TSMC法說會'

    Returns:
        Path to the saved report file.
    """
    if not os.environ.get("GEMINI_API_KEY"):
        raise ValueError("GEMINI_API_KEY not set. Add it to your .env file.")

    system_prompt = _load_system_prompt()

    print(f"[agent] mode: event_study")
    print(f"[agent] stock: {stock}")
    print(f"[agent] topic: {topic}")
    print(f"[agent] event dates: {event_dates}")
    print()

    return _run_event_study_gemini(stock, event_dates, topic, system_prompt)


def news_scan(query: str, days: int = 30) -> str:
    """Scan recent Taiwan financial news for a topic and analyze sentiment.

    Args:
        query: Search keywords
        days: Look-back period in days (default 30)

    Returns:
        Path to the saved report file.
    """
    if not os.environ.get("GEMINI_API_KEY"):
        raise ValueError("GEMINI_API_KEY not set. Add it to your .env file.")

    system_prompt = _load_system_prompt()

    print(f"[agent] mode: news_scan")
    print(f"[agent] query: {query}")
    print(f"[agent] days: {days}")
    print()

    return _run_news_scan_gemini(query, days, system_prompt)

