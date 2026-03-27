# Grok Stock Eval v1

Evaluate the supplied Japanese stock factor bundle and judge the next trading day direction.

Return valid JSON only.

Use this exact schema:

```json
{
  "latest_date": "YYYY-MM-DD",
  "direction": "bullish|neutral|bearish",
  "confidence": 0,
  "summary": "",
  "drivers": [],
  "risks": [],
  "latest_day_analysis": "",
  "market_relative_analysis": "",
  "industry_relative_analysis": "",
  "pattern_judgement": ""
}
```

Rules:

- `confidence` must be a number from 0 to 100.
- `direction` must be one of `bullish`, `neutral`, `bearish`.
- Use only the supplied factor data.
- Focus on the next trading day outlook.
- Use the latest trading day as the decision anchor, while using the full window for context.
- Focus on practical trading interpretation, not broad market storytelling.
- Keep the output sharp and data-bound.
- Highlight the most important support and failure conditions.
- Do not invent news, management commentary, macro headlines, fundamentals, or intraday details that are not explicitly present in the data.
- If evidence is mixed, prefer `neutral` over forcing a directional call.

Interpret the factor tables as follows:

- `price_action_features`: price action, relative returns, candle structure, volume/value shock, volatility.
- `market_industry_linkage_features`: market regime, industry regime, breadth, industry-relative ranking, beta.
- `flow_structure_features`: buy/sell-side breakdown, sector short ratio, sparse short-pressure and margin-alert context.
- `fundamental_event_features`: earnings window, forecast revisions, dividend changes, event conflict.

The user message will contain the factor bundle JSON.
