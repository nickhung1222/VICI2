> 📅 **最後更新**：2026-04-03　｜　由 Claude 根據專案掃描自動整理，如需更新請直接告知。

---

## 給 Claude 的指示（文件維護規則）

**這份文件（CLAUDE.md）與 README.md 需要跟專案保持同步。**

Claude 在每次對話中，如果發現以下任何一種情況，**必須在回覆結尾主動詢問使用者是否要更新 CLAUDE.md 和 README.md**：

- 新增了 `tools/` 模組或工具函數
- `agent.py` 的 `TOOLS` 清單有變動（新增、刪除、修改工具定義）
- `requirements.txt` 有新增套件
- 新增了 CLI 參數或執行模式
- `prompts/` 的系統提示有重大修改
- 目錄結構有變動（新增資料夾或重要檔案）

詢問方式範例：「我注意到專案結構有些變動，需要我幫你更新 CLAUDE.md 和 README.md 嗎？」

更新時，同步刷新頂部的「最後更新」日期。

---

## GitHub Tracking Rule

When a change is a major product or workflow update, Claude should explicitly ask whether the user wants the current batch committed and pushed to GitHub for tracking.

Treat a change as major when any of the following is true:

- A new CLI mode, CLI argument, or top-level workflow is added or changed
- A new `tools/` module, schema, or report/output contract is introduced
- Prompt behavior or event-analysis logic changes materially
- Multiple core files are changed together as one feature slice
- New tests are added for a new capability, not just a tiny bug fix

Do not push automatically without user confirmation if the workspace contains unrelated dirty changes or the target branch is unclear.

Preferred commit message style: concise English messages such as `feat: add event report workflow` or `fix: handle missing article content`.

---

# VICI2 — Taiwan News Event Study Agent

## 專案概述

VICI2 是一個台灣財經新聞事件研究 Agent，以 LLM 作為 Orchestrator，自動執行：
- 結構化事件蒐集（event-first collector，第一階段重構）
- 法說會官方來源蒐集（MOPS + IR artifacts）
- 法說會 verified digest（`official_artifacts` / `earnings_digest` / `todo_items`）
- 新聞抓取（法說會目前優先使用 Goodinfo 個股日期索引；其他事件仍可用 Cnyes、Google News RSS、yfinance）
- 獨立 Cnyes 個股新聞區間查詢（`tools/cnyes_stock_news.py`，不接入主流程）
- 中文情緒分析（看多 / 看空 / 中性）
- 事件研究計算（AR / CAR，市場模型 OLS）
- 圖表產生與 Markdown 報告輸出

LLM Provider：**Google Gemini**（`gemini-2.0-flash`），透過 `google-genai` SDK 呼叫。

---

## 目錄結構

```
VICI2/
├── agent.py          # LLM Orchestrator：tool use loop（核心邏輯）
├── main.py           # CLI 入口
├── tools/            # 各功能模組
│   ├── event_collector.py # 結構化事件蒐集（Phase 1）
│   ├── event_sources.py   # 法說會官方來源、artifact 發現與 digest
│   ├── earnings_validation.py # 法說會固定 gold sample 與 regression summary helper
│   ├── cnyes_stock_news.py # 獨立 Cnyes 個股新聞區間查詢
│   ├── schemas.py        # schema 與 normalization helper
│   ├── news_scraper.py   # 新聞抓取
│   ├── stock_data.py     # 台股股價（yfinance）
│   ├── event_study.py    # AR / CAR 計算
│   ├── chart.py          # 圖表產生（matplotlib）
│   └── report.py         # 報告儲存
├── prompts/
│   ├── system.md         # Agent 系統提示
│   └── report_format.md  # 報告格式模板
├── tests/            # pytest 測試
├── outputs/          # 產出（reports / charts / events）
├── STOCK_EVENT_TIMELINE_SPEC.md  # skill 規則拆分與重構規格
├── requirements.txt
└── .env              # API 金鑰（不進 git）
```

---

## 技術棧

| 類別 | 工具 |
|------|------|
| LLM | Google Gemini（`google-genai>=1.0.0`） |
| 股價資料 | `yfinance` |
| 數值計算 | `pandas`, `numpy`, `scipy` |
| 網頁抓取 | `requests`, `beautifulsoup4`, `lxml`, Playwright CLI fallback |
| PDF 抽取 | `pypdf`, `pdfplumber` |
| 圖表 | `matplotlib` |
| 環境變數 | `python-dotenv` |
| 測試 | `pytest` |

---

## 執行方式

```bash
# Event Collect 模式（結構化事件蒐集）
python main.py --mode event_collect \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --start-date 2025-04-01 \
    --end-date 2025-04-17 \
    --event-date 2025-04-17

# Event Study 模式（完整流程）
python main.py --mode event_study \
    --stock 2330.TW \
    --event-dates 2025-01-16,2025-04-17 \
    --topic "TSMC法說會"

# News Scan 模式（舊版 query-first 掃描）
python main.py --mode news_scan --query "央行升息" --days 30

# Standalone Cnyes Stock News（獨立模組）
python -m tools.cnyes_stock_news \
    --stock 2330 \
    --date-from 2026-04-01 \
    --date-to 2026-04-03 \
    --stock-name 台積電
```

---

## 環境設定

需在 `.env` 設定以下變數（參考 `.env.example`）：

```
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.0-flash
```

絕對不要將 `.env` 提交到 git。

---

## Agent 工作流程

`agent.py` 實作 LLM tool use loop，Gemini 自行決定呼叫工具的順序。

### Event Collect 模式（第一階段重構）
1. `event_collect()` 呼叫 `tools/event_collector.py`
2. 將股票、事件類型、日期範圍正規化為 collection plan
3. 對 `法說會` 先走 `tools/event_sources.py` 收集 MOPS record、IR artifacts、verified digest 與 todo
4. 再透過現有新聞來源收集結構化 event/news records
5. 使用 `save_event_record()` 輸出 JSON 到 `outputs/events/`

### Event Study 模式（7 步驟）
1. `scrape_news` — 搜尋事件主題相關新聞
2. `fetch_article_content` — 取得前 5 篇完整文章
3. 情緒分析（LLM 直接推論，無額外工具）
4. `fetch_stock_data` — 取得目標股票 + 大盤（TAIEX）歷史報酬率
5. `run_event_study` — 計算 AR / CAR（估計窗口 120 天，事件窗口 ±5 天；盤後事件可將 t=0 位移到下一交易日）
6. `generate_chart` — 產生 CAR 走勢圖（PNG）
7. `save_report` — 輸出 Markdown 報告到 `outputs/reports/`

### News Scan 模式（簡化流程）
1. `scrape_news` → 2. `fetch_article_content` → 3. 情緒分析 → 4. `save_report`

---

## 工具模組說明

### `tools/news_scraper.py`
- `search_news(query, date_from, date_to, max_results)` — 搜尋鉅亨網 + MoneyDJ
- `fetch_article_content(url, news_id)` — 取得文章全文（優先用 cnyes news_id API）

### `tools/cnyes_stock_news.py`
- `fetch_cnyes_stock_news(stock, date_from, date_to, stock_name="", match_mode="balanced", max_results=200)` — 獨立抓取鉅亨個股新聞區間結果
- `python -m tools.cnyes_stock_news ...` — 輸出 JSON，包含 `published_at`、`title`、`url`、`relevance`

### `tools/event_collector.py`
- `collect_event_records(...)` — 以事件導向輸入建立結構化事件紀錄
- `build_collection_queries(...)` — 將股票標的與事件類型轉成 event-first query plan
- 對 `法說會` 額外輸出 `official_artifacts`、`earnings_digest`、`todo_items`

### `tools/event_sources.py`
- `fetch_mops_investor_conference(...)` — 取得法說會官方日期、摘要與官方頁連結
- `collect_official_event_records(...)` — 組裝 MOPS record + IR artifacts + verified digest + todo
- 只保留帶 `evidence/source_ref` 的 verified metrics、management tone、Q&A

### `tools/schemas.py`
- `normalize_symbol(...)` — 將股票代碼正規化為 Yahoo Finance 樣式
- `build_stock_target(...)` — 統一股票標的 schema
- `dedupe_records(...)` — 對結構化紀錄做穩定去重

### `tools/stock_data.py`
- `fetch_stock_data(symbol, start_date, end_date)` — 回傳 `{dates, stock_returns, market_returns}`
- 大盤代碼：`^TWII`（台灣加權指數）

### `tools/event_study.py`
- `run_event_study(stock_returns, market_returns, dates, event_dates, ...)` — OLS 市場模型
- 估計窗口：事件前 130～11 個交易日（120 天）
- 事件窗口：`[-5, +5]`，多事件取平均 CAR + cross-sectional t 統計量
- 若事件在收盤後公布，可用 `reaction_shift_trading_days=1` 讓下一個交易日成為市場反應日 `t=0`

### `tools/chart.py`
- `generate_car_chart(car_data, title, symbol)` — 產生 CAR 圖（含 95% CI）

### `tools/report.py`
- `save_report(content, topic)` — 儲存 Markdown 到 `outputs/reports/`
- `save_event_record(...)` — 儲存單次事件記錄

---

## 開發規範

### 語言
- 所有程式碼、文件、commit 訊息使用**英文**
- 報告輸出、LLM prompt、工具 description 使用**繁體中文**

### 程式風格
- Python 3.11+，型別提示（type hints）優先
- 每個 `tools/` 模組只做一件事，保持單一職責
- tool 執行結果以 JSON 字串回傳給 LLM（`json.dumps(... ensure_ascii=False)`）
- 工具輸出過長時截斷（例如文章內容截斷至 5000 字元）
- 新的 collector 邏輯優先輸出固定 schema，避免把 source selection 留給 prompt 決定

### 新增工具
在 `agent.py` 的 `TOOLS` 清單新增 JSON Schema 定義，再在 `execute_tool()` 加入對應的 `elif` 分支。Gemini schema 需透過 `_to_gemini_schema()` 轉換（type 要大寫）。

### Skill 與 Repo 的邊界
- `stock-event-timeline` 只作為設計參考與研究 workflow 規格
- 可重跑的 collector、schema、scoring、report builder 應落在 repo code，不應只存在 skill 文字中
- 拆分規格見 `STOCK_EVENT_TIMELINE_SPEC.md`

### 錯誤處理
所有工具執行需 `try/except`，失敗時回傳 `"Error <tool_name>: <message>"` 字串（不直接拋出例外），讓 LLM 自行判斷是否重試。

### 測試
```bash
pytest tests/
```
新增功能時同步在 `tests/` 撰寫對應測試。

---

## 常見問題

**Q：GEMINI_API_KEY 沒設定怎麼辦？**
執行前會拋出 `ValueError`，請確認 `.env` 已正確設定。

**Q：事件研究資料不足怎麼辦？**
`run_event_study` 需要足夠的歷史資料（估計窗口 120 天 + buffer）。建議 `start_date` 至少早於最早事件日 180 天。對盤後事件，請區分公告日與市場反應日，不要直接把公告日視為 `t=0`。

**Q：如何切換 Gemini 模型？**
修改 `.env` 中的 `GEMINI_MODEL`，例如改為 `gemini-1.5-pro`。

**Q：`event_collect` 和 `news_scan` 差在哪裡？**
`event_collect` 是 event-first，輸出結構化 JSON，適合作為後續分析上游資料；`news_scan` 是舊版 query-first 掃描，適合快速臨時查詢。

**Q：獨立的鉅亨個股新聞查詢要走哪裡？**
使用 `python -m tools.cnyes_stock_news --stock <code> --date-from YYYY-MM-DD --date-to YYYY-MM-DD`。這個入口不接入 `main.py` 既有 mode，也不影響 event/report 主流程。
