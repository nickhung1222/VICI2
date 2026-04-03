# VICI2 — Taiwan News Event Study Agent

> 台灣財經新聞事件研究 Agent，以 LLM 作為 Orchestrator，自動完成事件蒐集、新聞抓取、情緒分析、事件研究計算與報告輸出。

---

## 功能特色

- **新聞抓取**：以 Cnyes 類別/文章頁為主庫，Goodinfo 採 HTTP-first + browser-fallback 的個股日期索引入口，輔以 Google News RSS 與 yfinance 補洞
- **獨立 Cnyes 股票新聞查詢**：提供 `tools/cnyes_stock_news.py`，可直接用股票代號 + 時間區間抓鉅亨個股新聞，輸出發布時間、標題、連結與相關度標記
- **結構化事件蒐集**：以 normalized article records 建立可重跑的 event collection JSON，並保留 primary / secondary source breakdown
- **官方事件來源**：法說會可透過 MOPS 官方來源補事件日期與公告資料
- **官方 artifact 蒐集**：法說會會輸出 `official_artifacts`，保存公司、事件、URL、抓取時間、格式、驗證狀態與 excerpt
- **反幻覺法說 digest**：法說會新增 `earnings_digest`，只保留帶 `evidence/source_ref` 的 verified metrics、management tone 與 Q&A
- **缺口與待辦追蹤**：每次 `event_collect` / `event_report` 都會輸出 `data_gaps` 與 `todo_items`
- **Hybrid 預期抽取**：先做規則式 metric candidate extraction，再用 Gemini 做 schema 化補齊與 evidence 對齊
- **中文情緒分析**：判斷每篇新聞的看多 / 看空 / 中性傾向，計算加權平均情緒分數
- **事件研究**：以 OLS 市場模型計算超額報酬（AR）與累積超額報酬（CAR），並支援盤後事件以次一交易日作為市場反應日（t=0）
- **圖表輸出**：自動產生 CAR 走勢圖（含 95% 信賴區間）
- **Markdown 報告**：完整分析報告儲存至 `outputs/reports/`

---

## 快速開始

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 設定環境變數

複製 `.env.example` 並填入你的 Gemini API 金鑰：

```bash
cp .env.example .env
```

編輯 `.env`：

```
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.0-flash
```

### 3. 執行

**Event Collect 模式**（第一階段重構：結構化事件蒐集，輸出 JSON）

```bash
python main.py --mode event_collect \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --start-date 2025-04-01 \
    --end-date 2025-04-17 \
    --event-date 2025-04-17 \
    --event-key 2025Q1
```

**Heat Scan 模式**（事件前熱度分析， recurring event 比去年同事件）

```bash
python main.py --mode heat_scan \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --event-date 2025-04-17 \
    --event-key 2025Q1
```

**Event Report 模式**（整合事件資料、熱度與 hybrid 預期 vs 實際）

```bash
python main.py --mode event_report \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --start-date 2025-04-01 \
    --end-date 2025-04-18 \
    --event-date 2025-04-17 \
    --event-key 2025Q1 \
    --include-event-study
```

**Event Study 模式**（完整流程：新聞 → 情緒 → AR/CAR → 圖表 → 報告）

```bash
python main.py --mode event_study \
    --stock 2330.TW \
    --event-dates 2025-01-16,2025-04-17 \
    --topic "TSMC法說會"
```

**News Scan 模式**（舊版 query-first 新聞情緒掃描）

```bash
python main.py --mode news_scan --query "央行升息" --days 30
```

**Standalone Cnyes Stock News**（獨立模組，不接進主流程）

```bash
python -m tools.cnyes_stock_news \
    --stock 2330 \
    --date-from 2026-04-01 \
    --date-to 2026-04-03 \
    --stock-name 台積電
```

---

## 重構方向

目前專案同時存在兩條資料路徑：

- `news_scan`：query-first，適合臨時掃描，但對事件研究的可重現性較弱
- `tools.cnyes_stock_news`：獨立的 Cnyes 個股新聞區間查詢，不改動既有 event/report workflow
- `event_collect`：event-first，將股票、事件類型、日期範圍正規化後輸出結構化 JSON
- `heat_scan`：針對指定事件輸出結構化熱度分析
- `event_report`：把事件蒐集、熱度與 hybrid 預期 vs 實際整合成 JSON + Markdown

第一階段重構已新增 `event_collect`，目標是讓後續的情緒分析、熱度分析與 event study 都能建立在固定 schema 上，而不是直接依賴 prompt 驅動的新聞搜尋。

目前 `event_collect`、`heat_scan` 與 `event_report` 對 `法說會` 都先固定使用 Goodinfo 作為新聞來源，並套用 `D-7 ~ D-1` 的事件前視窗，不混入法說會當天新聞。

目前 `法說會` 的官方資料層已擴充為：

- `MOPS`：事件日期、公告摘要、官方頁連結
- `IR artifacts`：presentation / earnings release / management report / transcript / webcast replay
- `earnings_digest`：只保留帶 evidence 與 source_ref 的 verified metrics / tone / Q&A
- `todo_items`：把 blocking / non_blocking 缺口一起存下，方便後續回補

Goodinfo 在主流程中的定位是「索引入口」，不是全文主庫：
- 先從 Goodinfo 取得日期、來源、標題與原始連結
- 再由原始新聞站補正文
- 若 Goodinfo 的 HTTP 路徑結果不足，會自動降級到 Playwright browser fallback，但仍以 10 秒內完成單次查詢為目標

目前比較策略已明確拆分：

- `法說會` 等 recurring event：預設比去年同一事件，例如 `2025Q4` 對 `2024Q4`
- `重大消息`、一次性事件：維持比事件前 `1~7` 天對更早一段期間的週平均

對 recurring event，建議在 `event_collect` 明確帶入 `--event-key`，避免用法說日期誤推季度。

目前 `event_report` 的 `event_study` 為 deterministic optional block；不開啟 `--include-event-study` 時，仍會正常產出事件報告。

針對台股 `法說會`，目前 `event_report --include-event-study` 預設會把法說會日期視為公告日，並以**下一個交易日**作為市場反應日 `t=0`，避免把盤後公布資訊錯置到同日報酬。

`stock-event-timeline` skill 現在只作為設計參考，不是執行時依賴。拆分規格見：

- `STOCK_EVENT_TIMELINE_SPEC.md`

---

## 專案結構

```
VICI2/
├── agent.py          # LLM Orchestrator（tool use loop）
├── main.py           # CLI 入口
├── tools/
│   ├── event_collector.py # 結構化事件蒐集（第一階段重構）
│   ├── heat_analysis.py   # 事件前熱度分析與比較策略
│   ├── expectation_analysis.py # 預期 vs 實際比較
│   ├── cnyes_stock_news.py # 獨立 Cnyes 個股新聞區間查詢 CLI / module
│   ├── schemas.py        # 統一 schema 與 normalization helper
│   ├── news_archive.py   # normalized 新聞主庫/索引/補充來源整合
│   ├── news_scraper.py   # 新聞抓取與 legacy fallback 介面
│   ├── event_sources.py  # 官方來源 adapter（目前含 MOPS 法說會）
│   ├── earnings_validation.py # 法說會固定 gold sample 與 regression summary helper
│   ├── stock_data.py     # 台股股價資料（yfinance）
│   ├── event_study.py    # AR / CAR 計算（OLS 市場模型）
│   ├── chart.py          # CAR 走勢圖產生（matplotlib）
│   └── report.py         # 報告組裝與儲存
├── prompts/
│   ├── system.md         # Agent 系統提示
│   └── report_format.md  # 報告格式模板
├── tests/            # pytest 測試
├── outputs/          # 分析產出（reports / charts / events）
├── STOCK_EVENT_TIMELINE_SPEC.md  # skill 規則拆分與重構規格
├── requirements.txt
├── .env.example
└── CLAUDE.md         # Claude 開發指引
```

---

## 技術棧

| 類別 | 工具 |
|------|------|
| LLM | Google Gemini (`gemini-2.0-flash`) via `google-genai` |
| 股價資料 | `yfinance`（大盤：`^TWII`） |
| 數值計算 | `pandas`, `numpy`, `scipy` |
| 網頁抓取 | `requests`, `beautifulsoup4`, `lxml` |
| PDF 抽取 | `pypdf`, `pdfplumber` |
| 圖表 | `matplotlib` |
| 測試 | `pytest` |

---

## 測試

```bash
pytest tests/
```

---

## Agent Collaboration

This repository keeps agent workflow rules in `AGENTS.md`.

When a change is large enough to affect product behavior, workflow, CLI usage, schema, or report output, the agent should ask whether the current batch should be committed and pushed to GitHub for tracking.

The detailed decision rules and push workflow live in `AGENTS.md`.

---

## 注意事項

- `.env` 包含 API 金鑰，請勿提交至 git（已加入 `.gitignore`）
- Event Study 需要足夠的歷史股價資料，建議 `start_date` 早於事件日至少 **180 天**
- 對盤後事件，報告中的 `event_date` 可能與 event study 的 `reaction_date` 不同；`reaction_date` 才是 CAR 視窗的 `t=0`
- 工具執行失敗時會回傳錯誤字串，LLM 會自行判斷是否重試
- `event_collect` 目前已可輸出 `official_artifacts`、`earnings_digest`、`todo_items`；對沒有證據的欄位會保守留空並標示缺口
