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

## 架構

### 兩條執行路徑

| 模組 | 負責模式 | 特性 |
|------|---------|------|
| `agent.py` | `event_study`、`news_scan` | LLM（Gemini）驅動，決定工具呼叫順序 |
| `pipeline.py` | `event_collect`、`heat_scan`、`event_report` | 確定性執行，每步固定，輸出可重現 |

`main.py` 依模式分別從兩個模組 import，路由到對應執行路徑。

### 新聞來源整合

- **有 stock_code（主流程）**：`news_archive.py` 整合兩個主要來源，輸出統一 normalized schema：
  - **Cnyes symbol news API**（`cnyes_stock_news.py`）：近期約 2 個月的精準個股新聞
  - **Goodinfo**：長期歷史索引，法說會事件優先使用
- **關鍵字查詢（無 stock_code）**：`news_scraper.py` fallback 路徑，使用 Cnyes 搜尋 API + Google News RSS
- Normalized record 欄位：`headline` / `published_at` / `source_article_id`（已統一，不使用舊版 `title` / `date` / `news_id`）

### 法說會資料層

- `MOPS`：事件日期、公告摘要、官方頁連結
- `IR artifacts`：presentation / earnings release / management report / transcript / webcast replay
- `earnings_digest`：只保留帶 `evidence/source_ref` 的 verified metrics / management tone / Q&A
- `todo_items`：blocking / non_blocking 缺口，方便後續回補

---

## 專案結構

```
VICI2/
├── agent.py          # Gemini LLM tool-use loop（event_study / news_scan）
├── pipeline.py       # 確定性 pipeline（event_collect / heat_scan / event_report）
├── main.py           # CLI 入口
├── tools/
│   ├── event_collector.py    # 結構化事件蒐集（第一階段重構）
│   ├── heat_analysis.py      # 事件前熱度分析與比較策略
│   ├── expectation_analysis.py # 預期 vs 實際比較
│   ├── cnyes_stock_news.py   # Cnyes 個股新聞區間查詢（standalone + 整合進主流程）
│   ├── schemas.py            # 統一 schema 與 normalization helper
│   ├── news_archive.py       # normalized 新聞主庫：cnyes symbol news + Goodinfo
│   ├── news_scraper.py       # 新聞抓取（關鍵字路徑：Cnyes 搜尋 + Google News RSS）
│   ├── event_sources.py      # 官方來源 adapter（MOPS 法說會）
│   ├── earnings_validation.py # 法說會固定 gold sample 與 regression summary helper
│   ├── stock_data.py         # 台股股價資料（yfinance）
│   ├── event_study.py        # AR / CAR 計算（OLS 市場模型）
│   ├── chart.py              # CAR 走勢圖產生（matplotlib）
│   └── report.py             # 報告組裝與儲存
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
