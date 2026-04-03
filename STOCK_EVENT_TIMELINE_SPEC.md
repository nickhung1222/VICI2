# Stock Event Timeline Split Spec

## Goal

Use `stock-event-timeline` as a design reference, not as a runtime dependency.

The project should absorb the reusable retrieval rules into code so event collection,
news digestion, and heat analysis become testable, repeatable, and callable from the
existing CLI and agent workflow.

---

## Keep In Skill

These items belong in the skill because they describe how an agent should conduct
research, ask for missing context, and choose an output style.

### Triggering And Task Framing

- Detect whether the request is about 法說會, 漲價, 庫藏股, 重訊, 股息, 合作案, or general stock events.
- Decide whether the user wants `新聞報告`, `熱度分析`, or `完整報告`.
- Ask for missing `時間範圍` before running a timeline-style task.

### Research Workflow Guidance

- Prefer primary sources first, especially MOPS for official Taiwan market events.
- Search both Chinese and English coverage when the company has meaningful global coverage.
- Decide when heat analysis is required based on whether a specific event date exists.
- Use search patterns that are event-first rather than keyword-first.

### Analyst Judgment Rules

- Preserve key numbers and management guidance in summaries.
- Avoid vague wording that drops the actual signal.
- Mark social counts and trend data as estimated when they are not exact.

### Presentation Rules

- Choose between concise prose, structured JSON, or full report sections based on user intent.
- Group news by quarter or by event date when the request spans multiple calls or repeated events.

---

## Move Into Repo Code

These items should live in the project because they are product logic, data contracts,
or repeatable pipelines.

### 1. Event Collector

Create a repo-owned collector that accepts explicit event inputs instead of only a raw query.

Recommended input shape:

```json
{
  "event_type": "earnings_call",
  "time_range": {
    "start": "2025-01-01",
    "end": "2025-12-31"
  },
  "stocks": [
    {
      "symbol": "2330.TW",
      "code": "2330",
      "name": "台積電"
    }
  ],
  "event_date": "2025-04-17"
}
```

Responsibilities:

- Normalize user input into a machine-usable query plan.
- Resolve stock names, codes, and Yahoo symbols.
- Build source-specific queries for official records and news coverage.
- Return structured event records rather than free-form search output.

### 2. Source Adapters

Implement source-specific fetchers in the repo rather than leaving source choice to a prompt.

Recommended priority:

1. Official sources for event existence and exact dates
2. Financial media for preview, expectation, and interpretation
3. Social and trend signals only when heat analysis is requested

Minimum code responsibilities:

- MOPS adapter for official event records
- News adapters for Taiwan financial media
- Optional social/trend adapters behind a separate analysis step
- Source normalization into one shared record format

### 3. Canonical Event Schema

Add a project-owned schema for event records and news digest records.

Recommended event record:

```json
{
  "stock_code": "2330",
  "stock_name": "台積電",
  "symbol": "2330.TW",
  "event_type": "法說會",
  "event_date": "2025-04-17",
  "source_type": "official",
  "source_name": "公開資訊觀測站",
  "source_url": "https://...",
  "headline": "2025 Q1 法說會",
  "summary": "法說會日期與會議資料。",
  "language": "zh-TW"
}
```

Recommended news digest record:

```json
{
  "stock_code": "2330",
  "event_type": "法說會",
  "event_date": "2025-04-17",
  "article_date": "2025-04-15",
  "article_type": "法說前預期",
  "source_name": "鉅亨網",
  "source_url": "https://...",
  "headline": "AI 需求支撐台積電法說預期",
  "summary": "保留營收、毛利率、資本支出等關鍵數字與市場預期。"
}
```

### 4. Heat Analysis Engine

Move the actual calculations into code.

Repo responsibilities:

- Compute Window A: `D-7 ~ D-1`
- Compute Window B: `D-37 ~ D-8`
- Calculate `heat_ratio`
- Score news heat, social discussion, and trends with fixed rules
- Return explicit missing-data flags when a source is unavailable

Recommended output:

```json
{
  "analysis_target": "2330 台積電",
  "event_date": "2025-04-17",
  "news_heat_ratio": 2.1,
  "news_heat_label": "高",
  "social_discussion_level": "normal",
  "trend_signal": "partial",
  "composite_heat_score": 58,
  "heat_label": "正常熱度",
  "data_gaps": [
    "google_trends_unavailable"
  ]
}
```

### 5. Report Builders

Turn the skill's output modes into code-level report builders.

Recommended repo modes:

- `event_collect`: structured JSON event and news collection
- `heat_scan`: structured heat analysis
- `event_report`: merged event collection + heat + event study report

The LLM can still write narrative text, but the report sections should be assembled from
project-owned structured data.

### 6. Validation And Tests

This must be code, not skill text.

Add tests for:

- record normalization
- deduplication by stock + event_date + event_type
- heat ratio calculation
- missing-source handling
- report assembly from structured inputs

---

## What The Skill Should Not Own

Do not leave these as skill-only behavior:

- runtime source selection
- production schema definitions
- event-date normalization logic
- scoring formulas used by downstream analysis
- report assembly required by the CLI
- any logic needed for repeatable batch runs

If these stay only in the skill, the repo remains prompt-driven and hard to verify.

---

## Recommended Project Refactor

### Replace Query-First News Scan

Current `news_scan` is query-first and relies on broad media search plus LLM selection.
That is useful for ad hoc scanning, but weak for repeatable event research.

Target design:

1. Parse event request into a structured collection plan
2. Collect official event records
3. Collect related news digest records
4. Optionally run heat analysis when `event_date` exists
5. Feed the structured results into sentiment analysis and event study
6. Generate Markdown and JSON outputs

### Suggested Module Split

Recommended additions:

- `tools/event_collector.py`
- `tools/event_sources.py`
- `tools/heat_analysis.py`
- `tools/schemas.py`

Possible updates:

- keep `tools/news_scraper.py` as one source adapter rather than the top-level collector
- de-emphasize `news_scan` in favor of `event_collect` or `event_report`

---

## Migration Path

### Phase 1

- Define the canonical schemas
- Add a collector that wraps current news scraping behind structured inputs
- Keep existing `event_study` and report generation unchanged

### Phase 2

- Add official-source support for event dates and event metadata
- Add heat analysis as a separate module
- Make `news_scan` a compatibility mode instead of the primary path

### Phase 3

- Refactor the Gemini prompt so it consumes structured event records
- Reduce prompt responsibility for source selection and retrieval planning
- Treat the LLM as summarizer and analyst, not as the retrieval controller

---

## Bottom Line

The skill remains useful as a research playbook.

The repo should own:

- source adapters
- schemas
- scoring rules
- collector flow
- test coverage
- CLI modes

That is the boundary that turns the project from a prompt-led workflow into a productized
event research pipeline.
