# VICI2 — Taiwan Earnings Call Narrative & Heat Analysis Engine

> 以法說會為核心的台股事件研究工具，主流程聚焦事件導向蒐集、市場前後敘事整理與事件熱度分析。

---

## 目前主打能力

- **結構化事件蒐集**：以 normalized article records 建立可重跑的 event collection JSON，並保留 source breakdown
- **市場前後敘事整理**：`event_report` 直接依 `pre_event` / `event_day` / `post_event` 記錄組出市場前後敘事與轉折
- **法說後敘事收斂**：事件後新聞先寬抓，再用 deterministic relevance filter 優先保留較像法說後解讀 / 法人反應的高信心文章
- **事件前後熱度分析**：`heat_scan` 支援 `--phase pre_event|post_event|both`，可分別輸出事件前與事件後的 heat scan；事件前使用 multi-panel comparison（coverage / recency / source mix），事件後聚焦 coverage comparison 與資料缺口
- **單一正式報告**：目前正式主輸出為 `event_report` 的 Markdown 報告，聚焦事件摘要、事件前後敘事、敘事轉折與熱度分析
- **其他模式保留**：`event_collect` / `heat_scan` 輸出結構化 JSON；`event_study` 仍可獨立作為次級驗證能力使用，但不作為主報告正文的一部分
- **資料覆蓋限制**：目前新聞資料來源實際可用區間主要自 `2024-10` 起；更早日期的法說會可能只能輸出空報告或 `資料不足`

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

**Chat 模式**（自然語言互動入口）

```bash
python main.py --mode chat
```

進入後可直接輸入：

- `幫我分析台積電 2025Q1 法說會`
- `幫我做聯發科法說會前後熱度分析`
- `蒐集鴻海 2025-03-01 到 2025-03-20 的法說會新聞`
- `做台積電 2025-01-16,2025-04-17 的 event study`

第一版 `chat mode` 目前採用 rule-based 意圖判斷，支援：
- 啟動時自動介紹可做的事與範例問法
- 從自然語言判斷 `event_report` / `heat_scan` / `event_collect` / `event_study`
- 缺必要欄位時逐步追問
- 執行前列出解析後參數並請使用者確認

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

**Heat Scan 模式**（事件前後熱度分析，可指定 phase）

```bash
python main.py --mode heat_scan \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --event-date 2025-04-17 \
    --event-key 2025Q1 \
    --phase both
```

**Event Report 模式**（主流程：整合事件資料、前後敘事、敘事轉折與熱度）

```bash
python main.py --mode event_report \
    --stock 2330.TW \
    --stock-name 台積電 \
    --event-type 法說會 \
    --start-date 2025-04-01 \
    --end-date 2025-04-18 \
    --event-date 2025-04-17 \
    --event-key 2025Q1
```

**Event Study 模式**（可選次級驗證：新聞 → 情緒 → AR/CAR → 圖表 → 報告）

```bash
python main.py --mode event_study \
    --stock 2330.TW \
    --event-dates 2025-01-16,2025-04-17 \
    --topic "TSMC法說會"
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

### 核心主流程

| 模組 | 負責模式 | 特性 |
|------|---------|------|
| `pipeline.py` | `event_collect`、`heat_scan`、`event_report` | 正式主流程，確定性執行，輸出可重現 |
| `agent.py` | `event_study` | 附屬 adapter，僅保留 optional 的 LLM 驅動事件研究 |
| `chat_cli.py` | `chat` | 對話式入口，將自然語言任務映射到既有 pipeline |

`main.py` 的核心工作流由 `pipeline.py` 承擔；`agent.py` 不再視為平行主架構，只保留 `event_study` 附屬能力。

### 新聞來源整合

- **有 stock_code（主流程）**：`news_archive.py` 整合兩個主要來源，輸出統一 normalized schema：
  - **Cnyes symbol news API**（`cnyes_stock_news.py`）：近期約 2 個月的精準個股新聞
  - **Goodinfo**：長期歷史索引，作為法說會新聞補充來源
- **關鍵字查詢（無 stock_code）**：`news_scraper.py` fallback 路徑，使用 Cnyes 搜尋 API + Google News RSS
- Normalized record 欄位：`headline` / `published_at` / `source_article_id`（已統一，不使用舊版 `title` / `date` / `news_id`）

### 法說會資料層與報告輸出

- `MOPS`：事件日期、公告摘要、官方頁連結
- `event_date resolver`：目前正式支援範圍限於台灣市值前 10 大公司，且歷史季度以 `2024Q3` 之後為主；有 `event_key` 時優先以 `EMOPS historical information` 對歷史季度做官方比對，若失敗再以 `Yahoo 股市行事曆` 補抓帶季度訊息的法說事件；無 `event_key` 時再以 `MOPS` latest snapshot 驗證 / 補齊；媒體新聞不再反推正式 `event_date`
- `IR artifacts`：presentation / earnings release / management report / transcript / webcast replay
- `earnings_digest`：best-effort 只保留帶 `evidence/source_ref` 的 verified metrics / management tone / Q&A
- `event_report`：正式 Markdown 報告目前只呈現事件摘要、市場事件前敘事、市場事件後敘事、前後敘事轉折、熱度分析
- `todo_items`：blocking / non_blocking 缺口，方便後續回補
- `official_artifacts` / `earnings_digest` / `todo_items`：目前保留在結構化輸出，暫不列入正式報告正文
- 歷史資料限制：目前正式新聞資料來源對法說會的實際可用區間主要自 `2024-10` 起

---

## 專案結構

```
VICI2/
├── agent.py          # event_study 專用 Gemini adapter
├── chat_cli.py       # chat mode 對話式 CLI 入口
├── pipeline.py       # 確定性 pipeline（event_collect / heat_scan / event_report）
├── main.py           # CLI 入口
├── tools/
│   ├── event_collector.py    # 結構化事件蒐集（第一階段重構）
│   ├── heat_analysis.py      # 事件前後 heat scan 與比較策略
│   ├── expectation_analysis.py # 未來可加值模組：預期 vs 實際比較
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
- `event_report` 是目前正式主輸出；正式 Markdown 報告目前只保留 5 個段落：事件摘要、事件前敘事、事件後敘事、敘事轉折、熱度分析
- `event_study` 可獨立輸出報告，但屬於附屬驗證能力，不是正式主流程
- 目前新聞資料來源實際可用區間主要自 `2024-10` 起；`2024/09` 以前的法說會常見結果是空報告或 `資料不足`
- Event Study 需要足夠的歷史股價資料，建議 `start_date` 早於事件日至少 **180 天**
- 對盤後事件，報告中的 `event_date` 可能與 event study 的 `reaction_date` 不同；`reaction_date` 才是 CAR 視窗的 `t=0`
- `event_collect` 仍可輸出 `official_artifacts`、`earnings_digest`、`todo_items`；但這些目前視為補強資料或待做事項，不列入正式報告正文
- 法說會若提供 `event_key`，目前只對台灣市值前 10 大公司且 `2024Q3` 之後的季度啟用 historical resolver；系統會先查 `EMOPS historical` 歷史公告，再 fallback `Yahoo 股市行事曆` 的 `相關訊息` 季度字樣；支援英文季度與民國年 / 中文季度別名。若仍無法確認同一季度，才保留原輸入並標記 `unverified`
- `expectation_analysis.py` 保留為 module-level capability，但不納入目前主流程承諾
- query-first 的新聞掃描入口目前已下架；若未來要擴充，建議以獨立附屬工具重建，不要混入 event-first 主流程
