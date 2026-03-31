# 事件研究報告模板

```markdown
# {事件名稱} 事件研究報告

**股票代碼**：{symbol}
**分析日期**：{analysis_date}
**事件日期**：{event_dates}
**樣本數**：{n_events} 個事件

---

## 一、事件概述

{event_description}

---

## 二、新聞情緒分析

### 新聞列表

| 日期 | 來源 | 標題 | 情緒分數 | 分類 |
|------|------|------|---------|------|
{news_table}

### 情緒摘要

- **整體傾向**：{overall_sentiment}（平均分數：{avg_score}）
- **看多比例**：{bullish_pct}%
- **看空比例**：{bearish_pct}%
- **關鍵主題**：{key_themes}

---

## 三、超額報酬分析（Event Study）

### 市場模型參數

- 估計窗口：120 個交易日（事件日前 130 至 11 天）
- 事件窗口：[-5, +5] 交易日
- 基準指數：台灣加權指數（^TWII）
- 有效事件數：{n_events}（跳過：{n_skipped}）

### CAR 結果

| 事件窗口 | 平均 CAR | 標準誤 | t 統計量 |
|---------|---------|--------|---------|
| [-5, -1] | {car_pre}% | {se_pre} | {t_pre} |
| [0, 0]  | {car_event}% | {se_event} | {t_event} |
| [+1, +5] | {car_post}% | {se_post} | {t_post} |
| [-5, +5] | {car_full}% | {se_full} | {t_full} |

### 圖表

![CAR 走勢圖]({chart_path})

---

## 四、情緒與市場走勢驗證

{verification_analysis}

**驗證結果**：{verification_result}
- 情緒方向：{sentiment_direction}
- 市場反應：{market_direction}
- 一致性：{consistency}

---

## 五、結論與交易含義

{conclusion}

### 觀察到的規律性

{patterns}

### 量化信號參考

{trading_implications}

---

*報告由 VICI2 台灣新聞事件研究 Agent 自動生成*
*生成時間：{generated_at}*
```
