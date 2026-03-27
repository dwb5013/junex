# Factor Definitions

This document defines the current factor schema produced by the analytics layer.
The goal is to keep implementation, documentation, and downstream LLM interpretation aligned.

## Scope

Current scope covers four factor groups:

- Price action factors
- Market and industry linkage factors
- Flow structure factors
- Fundamental event factors

Current output tables:

- `analytics.price_action_features`
- `analytics.market_industry_linkage_features`
- `analytics.flow_structure_features`
- `analytics.fundamental_event_features`

## Shared Conventions

Primary key:

- `code`
- `trade_date`

Time ordering:

- All lagged and rolling calculations are ordered by `trade_date asc`

Null rule:

- If a required lagged value is missing, the factor is `NULL`
- If a denominator is `0` or missing, the factor is `NULL`
- If the rolling window does not have enough valid rows for the underlying calculation, DuckDB returns `NULL`

Equity classification rule:

- The current implementation uses the latest available `market_data.equity_master` snapshot for each `code`
- It does not require `equity_master.Date == trade_date`

Industry benchmark rule:

- The current implementation uses a static `S33 -> 東証業種別 index code` mapping
- Example: `S33 = 3250` maps to `industry_index_code = 0047`
- If `s33` is missing or unmapped, industry benchmark factors return `NULL`

## Table 1: `analytics.price_action_features`

### Data Sources

- `market_data.equity_daily_bar`
- `market_data.topix_daily_bar`
- `market_data.equity_master`
- `market_data.index_daily_bar`

### Price Convention

- Equities use adjusted fields: `AdjO`, `AdjH`, `AdjL`, `AdjC`, `AdjVo`
- Turnover uses `Va`
- TOPIX uses `C`
- Industry index uses `C`

### Base Columns

- `trade_date`
  - Meaning: Trading date
  - Source: `market_data.equity_daily_bar.Date`

- `code`
  - Meaning: Equity code
  - Source: `market_data.equity_daily_bar.Code`

- `s33`
  - Meaning: TSE 33-sector code from the latest available equity classification snapshot for the code
  - Source: `market_data.equity_master.S33`

- `s33_name`
  - Meaning: TSE 33-sector name from the latest available equity classification snapshot for the code
  - Source: `market_data.equity_master.S33Nm`

- `industry_index_code`
  - Meaning: J-Quants index code used as the primary industry benchmark
  - Source: Static mapping from `s33` to the corresponding `東証業種別` index code

- `open`
  - Meaning: Adjusted open price
  - Formula: `AdjO`

- `high`
  - Meaning: Adjusted high price
  - Formula: `AdjH`

- `low`
  - Meaning: Adjusted low price
  - Formula: `AdjL`

- `close`
  - Meaning: Adjusted close price
  - Formula: `AdjC`

- `volume`
  - Meaning: Adjusted volume
  - Formula: `AdjVo`

- `value`
  - Meaning: Trading value
  - Formula: `Va`

### Absolute Return Factors

- `ret_1d`
  - Meaning: 1-day discrete return
  - Formula: `close_t / close_t-1 - 1`

- `ret_3d`
  - Meaning: 3-day cumulative discrete return
  - Formula: `close_t / close_t-3 - 1`

- `ret_5d`
  - Meaning: 5-day cumulative discrete return
  - Formula: `close_t / close_t-5 - 1`

- `ret_20d`
  - Meaning: 20-day cumulative discrete return
  - Formula: `close_t / close_t-20 - 1`

### TOPIX Benchmark Factors

- `topix_ret_1d`
  - Meaning: 1-day TOPIX discrete return
  - Formula: `topix_close_t / topix_close_t-1 - 1`

- `topix_ret_3d`
  - Meaning: 3-day TOPIX cumulative discrete return
  - Formula: `topix_close_t / topix_close_t-3 - 1`

- `topix_ret_5d`
  - Meaning: 5-day TOPIX cumulative discrete return
  - Formula: `topix_close_t / topix_close_t-5 - 1`

- `topix_ret_20d`
  - Meaning: 20-day TOPIX cumulative discrete return
  - Formula: `topix_close_t / topix_close_t-20 - 1`

- `excess_ret_1d`
  - Meaning: 1-day return relative to TOPIX
  - Formula: `ret_1d - topix_ret_1d`

- `excess_ret_3d`
  - Meaning: 3-day return relative to TOPIX
  - Formula: `ret_3d - topix_ret_3d`

- `excess_ret_5d`
  - Meaning: 5-day return relative to TOPIX
  - Formula: `ret_5d - topix_ret_5d`

- `excess_ret_20d`
  - Meaning: 20-day return relative to TOPIX
  - Formula: `ret_20d - topix_ret_20d`

### Industry Benchmark Factors

- `industry_ret_1d`
  - Meaning: 1-day primary industry benchmark return
  - Formula: `industry_close_t / industry_close_t-1 - 1`

- `industry_ret_3d`
  - Meaning: 3-day primary industry benchmark cumulative return
  - Formula: `industry_close_t / industry_close_t-3 - 1`

- `industry_ret_5d`
  - Meaning: 5-day primary industry benchmark cumulative return
  - Formula: `industry_close_t / industry_close_t-5 - 1`

- `industry_ret_20d`
  - Meaning: 20-day primary industry benchmark cumulative return
  - Formula: `industry_close_t / industry_close_t-20 - 1`

- `industry_excess_ret_1d`
  - Meaning: 1-day return relative to the primary industry benchmark
  - Formula: `ret_1d - industry_ret_1d`

- `industry_excess_ret_3d`
  - Meaning: 3-day return relative to the primary industry benchmark
  - Formula: `ret_3d - industry_ret_3d`

- `industry_excess_ret_5d`
  - Meaning: 5-day return relative to the primary industry benchmark
  - Formula: `ret_5d - industry_ret_5d`

- `industry_excess_ret_20d`
  - Meaning: 20-day return relative to the primary industry benchmark
  - Formula: `ret_20d - industry_ret_20d`

### Candle Structure Factors

- `intraday_range_pct`
  - Meaning: Daily high-low range scaled by previous close
  - Formula: `(high_t - low_t) / close_t-1`

- `body_ratio`
  - Meaning: Candle body size as a share of the daily range
  - Formula: `abs(close_t - open_t) / (high_t - low_t)`

- `upper_shadow_ratio`
  - Meaning: Upper shadow size as a share of the daily range
  - Formula: `(high_t - max(open_t, close_t)) / (high_t - low_t)`

- `lower_shadow_ratio`
  - Meaning: Lower shadow size as a share of the daily range
  - Formula: `(min(open_t, close_t) - low_t) / (high_t - low_t)`

- `close_position`
  - Meaning: Closing location within the daily range
  - Formula: `(close_t - low_t) / (high_t - low_t)`
  - Range note: When defined, the value is expected to be between `0` and `1`

### Gap Factor

- `gap_pct`
  - Meaning: Opening gap relative to previous close
  - Formula: `open_t / close_t-1 - 1`

### Liquidity Shock Factors

- `volume_shock_20d_inclusive`
  - Meaning: Current adjusted volume divided by the rolling 20-row average adjusted volume, including the current row
  - Formula: `volume_t / avg(volume_t-19 ... volume_t)`

- `value_shock_20d_inclusive`
  - Meaning: Current trading value divided by the rolling 20-row average trading value, including the current row
  - Formula: `value_t / avg(value_t-19 ... value_t)`

### Range and Volatility Factors

- `true_range`
  - Meaning: Standard true range using previous close
  - Formula: `max(high_t - low_t, abs(high_t - close_t-1), abs(low_t - close_t-1))`

- `mean_true_range_5`
  - Meaning: Rolling 5-row mean of true range, including the current row
  - Formula: `avg(true_range over last 5 rows including t)`

- `mean_true_range_20`
  - Meaning: Rolling 20-row mean of true range, including the current row
  - Formula: `avg(true_range over last 20 rows including t)`

- `logret_vol_5`
  - Meaning: Unannualized rolling standard deviation of 1-day log returns over the last 5 rows, including the current row
  - Formula: `stddev(log(close_i / close_i-1)) over last 5 rows including t`

- `logret_vol_20`
  - Meaning: Unannualized rolling standard deviation of 1-day log returns over the last 20 rows, including the current row
  - Formula: `stddev(log(close_i / close_i-1)) over last 20 rows including t`

### Limit Flags

- `hit_limit_up`
  - Meaning: Daily limit-up flag
  - Formula: `1 if UL = '1' else 0`

- `hit_limit_down`
  - Meaning: Daily limit-down flag
  - Formula: `1 if LL = '1' else 0`

## Table 2: `analytics.market_industry_linkage_features`

### Purpose

This table describes the context around an equity’s daily price action:

- Whether the market environment was broad or narrow
- Whether the industry environment was strong or weak
- Where the stock ranked inside its own industry on the day
- How strongly the stock co-moved with TOPIX over rolling windows

### Data Sources

- `analytics.price_action_features`
- `market_data.topix_daily_bar`
- `market_data.index_daily_bar`

### Base Columns

- `trade_date`
  - Meaning: Trading date

- `code`
  - Meaning: Equity code

- `s33`
  - Meaning: TSE 33-sector code used for industry grouping

- `s33_name`
  - Meaning: TSE 33-sector name used for industry grouping

### Market Regime Factors

- `topix_ret_1d`
  - Meaning: 1-day TOPIX discrete return
  - Formula: inherited from `analytics.price_action_features.topix_ret_1d`

- `topix_ret_5d`
  - Meaning: 5-day TOPIX cumulative discrete return
  - Formula: inherited from `analytics.price_action_features.topix_ret_5d`

- `topix_close_vs_ma20`
  - Meaning: TOPIX level relative to its rolling 20-row moving average
  - Formula: `topix_close_t / topix_ma20_t - 1`
  - Source: `market_data.topix_daily_bar.C`

### Industry Regime Factors

- `industry_ret_1d`
  - Meaning: 1-day primary industry benchmark return
  - Formula: inherited from `analytics.price_action_features.industry_ret_1d`

- `industry_ret_5d`
  - Meaning: 5-day primary industry benchmark return
  - Formula: inherited from `analytics.price_action_features.industry_ret_5d`

- `industry_close_vs_ma20`
  - Meaning: Industry index level relative to its rolling 20-row moving average
  - Formula: `industry_close_t / industry_ma20_t - 1`
  - Source: `market_data.index_daily_bar.C` for `industry_index_code`

- `industry_excess_ret_1d`
  - Meaning: 1-day return relative to the primary industry benchmark
  - Formula: `ret_1d - industry_ret_1d`

- `industry_excess_ret_5d`
  - Meaning: 5-day return relative to the primary industry benchmark
  - Formula: `ret_5d - industry_ret_5d`

### Market Breadth Factors

- `market_up_ratio`
  - Meaning: Share of tracked equities with positive `ret_1d` on the same trading date
  - Formula: `avg(1.0 if ret_1d > 0 else 0.0) over all stocks on trade_date`

- `market_new_high_20d_ratio`
  - Meaning: Share of tracked equities whose `close` is at or above their rolling 20-row high on the same trading date
  - Formula: `avg(1.0 if close_t >= rolling_high_20d_t else 0.0) over all stocks on trade_date`

- `market_volume_up_ratio`
  - Meaning: Share of tracked equities that both rose on the day and traded above their inclusive 20-row average volume
  - Formula: `avg(1.0 if ret_1d > 0 and volume_shock_20d_inclusive > 1 else 0.0) over all stocks on trade_date`

### Industry Breadth Factors

- `industry_up_ratio`
  - Meaning: Share of same-`s33` equities with positive `ret_1d` on the same trading date
  - Formula: `avg(1.0 if ret_1d > 0 else 0.0) over stocks in same trade_date + s33`

- `industry_new_high_20d_ratio`
  - Meaning: Share of same-`s33` equities whose `close` is at or above their rolling 20-row high on the same trading date
  - Formula: `avg(1.0 if close_t >= rolling_high_20d_t else 0.0) over stocks in same trade_date + s33`

- `industry_volume_up_ratio`
  - Meaning: Share of same-`s33` equities that both rose on the day and traded above their inclusive 20-row average volume
  - Formula: `avg(1.0 if ret_1d > 0 and volume_shock_20d_inclusive > 1 else 0.0) over stocks in same trade_date + s33`

### Cross-Sectional Strength Factors

- `industry_strength_pct_1d`
  - Meaning: Percentile rank of the stock’s `ret_1d` inside its own `trade_date + s33` group
  - Formula: `percent_rank() over (trade_date, s33 order by ret_1d)`

- `industry_strength_pct_5d`
  - Meaning: Percentile rank of the stock’s `ret_5d` inside its own `trade_date + s33` group
  - Formula: `percent_rank() over (trade_date, s33 order by ret_5d)`

- `industry_strength_pct_20d`
  - Meaning: Percentile rank of the stock’s `ret_20d` inside its own `trade_date + s33` group
  - Formula: `percent_rank() over (trade_date, s33 order by ret_20d)`

### Rolling Sensitivity Factors

- `beta_20`
  - Meaning: 20-row rolling beta of stock 1-day return to TOPIX 1-day return
  - Formula: `covar_samp(ret_1d, topix_ret_1d) / var_samp(topix_ret_1d)` over the last 20 rows for each code

- `beta_60`
  - Meaning: 60-row rolling beta of stock 1-day return to TOPIX 1-day return
  - Formula: `covar_samp(ret_1d, topix_ret_1d) / var_samp(topix_ret_1d)` over the last 60 rows for each code

### Interpretation Notes

- `market_up_ratio` and `industry_up_ratio` are breadth indicators, not trend indicators
- `topix_close_vs_ma20` and `industry_close_vs_ma20` are level-vs-trend indicators, not returns
- `industry_strength_pct_*` values are relative rankings inside the same `s33` group on the same day
- `beta_20` and `beta_60` are rolling exposure estimates and can be `NULL` when the window is too short or TOPIX variance is `0`

## Market And Industry Linkage Enhancements Not Yet Implemented

The following related enhancements are not part of the current implementation:

- Fallback from `S33` benchmark to `TOPIX-17` benchmark when a primary `S33` mapping is unavailable
- Market-wide cross-sectional percentile ranks
- Industry trend-state flags such as `industry_ma5_vs_ma20`
- Residual return or alpha-style factors after removing market beta

## Table 3: `analytics.flow_structure_features`

### Purpose

This table describes how daily order flow and short-selling related data were structured around the stock:

- Whether buying was driven by cash buying, margin new buying, or short-cover style buying
- Whether selling was driven by cash selling, non-margin short selling, or margin close selling
- Whether the industry had elevated short-selling pressure on the day
- Whether the stock was under daily margin-alert pressure or large disclosed short-position pressure

### Data Sources

- `analytics.price_action_features`
- `market_data.market_breakdown`
- `market_data.short_ratio`
- `market_data.margin_alert`
- `market_data.short_sale_report`

### Base Columns

- `trade_date`
  - Meaning: Trading date

- `code`
  - Meaning: Equity code

- `s33`
  - Meaning: TSE 33-sector code used for joining sector-level short-ratio data

- `s33_name`
  - Meaning: TSE 33-sector name used for explanation and grouping

### Trade Value Totals

- `total_buy_va`
  - Meaning: Total buy-side daily trading value reconstructed from breakdown components
  - Formula: `LongBuyVa + MrgnBuyNewVa + MrgnBuyCloseVa`

- `total_sell_va`
  - Meaning: Total sell-side daily trading value reconstructed from breakdown components
  - Formula: `LongSellVa + ShrtNoMrgnVa + MrgnSellNewVa + MrgnSellCloseVa`

- `total_trade_va`
  - Meaning: Total two-sided trading value covered by the reconstructed breakdown
  - Formula: `total_buy_va + total_sell_va`

### Buy-Side Structure Factors

- `long_buy_share_of_buy_va`
  - Meaning: Share of buy-side trading value driven by cash buying
  - Formula: `LongBuyVa / total_buy_va`

- `margin_buy_new_share_of_buy_va`
  - Meaning: Share of buy-side trading value driven by new margin long positions
  - Formula: `MrgnBuyNewVa / total_buy_va`

- `margin_buy_close_share_of_buy_va`
  - Meaning: Share of buy-side trading value driven by closing existing short-side margin positions
  - Formula: `MrgnBuyCloseVa / total_buy_va`

### Sell-Side Structure Factors

- `long_sell_share_of_sell_va`
  - Meaning: Share of sell-side trading value driven by cash selling
  - Formula: `LongSellVa / total_sell_va`

- `short_nomargin_share_of_sell_va`
  - Meaning: Share of sell-side trading value driven by non-margin short selling
  - Formula: `ShrtNoMrgnVa / total_sell_va`

- `margin_sell_new_share_of_sell_va`
  - Meaning: Share of sell-side trading value driven by opening new margin short positions
  - Formula: `MrgnSellNewVa / total_sell_va`

- `margin_sell_close_share_of_sell_va`
  - Meaning: Share of sell-side trading value driven by closing existing margin long positions
  - Formula: `MrgnSellCloseVa / total_sell_va`

### Net Flow Factors

- `net_cash_va`
  - Meaning: Net cash flow implied by cash buying minus cash selling
  - Formula: `LongBuyVa - LongSellVa`

- `net_margin_new_va`
  - Meaning: Net change in new margin positioning on the day
  - Formula: `MrgnBuyNewVa - MrgnSellNewVa`

- `net_margin_close_va`
  - Meaning: Net closing flow on the day
  - Formula: `MrgnBuyCloseVa - MrgnSellCloseVa`

### Sector Short-Selling Background

- `SellExShortVa`
  - Meaning: Sector-level ordinary sell trading value from `short-ratio`

- `ShrtWithResVa`
  - Meaning: Sector-level short-selling trading value under price restriction from `short-ratio`

- `ShrtNoResVa`
  - Meaning: Sector-level short-selling trading value without price restriction from `short-ratio`

- `sector_short_ratio`
  - Meaning: Sector-level share of short-selling related trading value within total sector sell-side trading value represented in `short-ratio`
  - Formula: `(ShrtWithResVa + ShrtNoResVa) / (SellExShortVa + ShrtWithResVa + ShrtNoResVa)`

### Margin Alert Factors

- `margin_alert_sl_ratio`
  - Meaning: Most recent available daily-publication margin balance short/long ratio up to the trading date
  - Source: latest `market_data.margin_alert` record with `PubDate <= trade_date`

- `margin_alert_long_out_chg`
  - Meaning: Most recent available change in long outstanding margin balance up to the trading date
  - Source: latest `market_data.margin_alert.LongOutChg` with `PubDate <= trade_date`
  - Type note: stored as string because the official API allows mixed string/number values

- `margin_alert_short_out_chg`
  - Meaning: Most recent available change in short outstanding margin balance up to the trading date
  - Source: latest `market_data.margin_alert.ShrtOutChg` with `PubDate <= trade_date`
  - Type note: stored as string because the official API allows mixed string/number values

- `margin_alert_pub_date`
  - Meaning: Publication date of the margin-alert record used for the row

### Short Sale Report Factors

- `short_sale_pos_to_so`
  - Meaning: Most recent available disclosed short position ratio to shares outstanding up to the trading date
  - Source: latest `market_data.short_sale_report` record with `CalcDate <= trade_date`

- `short_sale_prev_ratio`
  - Meaning: Previous disclosed short position ratio carried in the latest short-sale-report record

- `short_sale_ratio_change`
  - Meaning: Change in disclosed short position ratio versus the previous reported ratio
  - Formula: `short_sale_pos_to_so - short_sale_prev_ratio`

- `short_sale_calc_date`
  - Meaning: Calculation date of the short-sale-report record used for the row

### Interpretation Notes

- `breakdown` is the primary source for stock-level trade-structure inference
- `short_ratio` is a sector-level context factor, not a stock-level order-flow factor
- `margin_alert_*` factors are sparse and only exist for stocks that enter daily-publication style monitoring
- `short_sale_*` factors are sparse and threshold-based because only large disclosed short positions are reported
- `margin_alert_long_out_chg` and `margin_alert_short_out_chg` are intentionally stored as strings to preserve official mixed-type values such as `-`

## Table 4: `analytics.fundamental_event_features`

### Purpose

This table describes the state of recent earnings, guidance, and dividend events around each stock-date:

- Whether the row is on an earnings release day or inside a recent earnings event window
- Whether the next scheduled earnings date is approaching
- Whether management guidance has recently been revised upward, downward, or in mixed fashion
- Whether dividend announcements have recently changed shareholder return expectations

### Data Sources

- `analytics.price_action_features`
- `market_data.fin_summary`
- `market_data.fin_dividend`
- `market_data.earnings_calendar`

### Base Columns

- `trade_date`
  - Meaning: Trading date

- `code`
  - Meaning: Equity code

### Earnings Event State

- `is_earnings_day`
  - Meaning: Whether the trading date itself is an earnings disclosure date
  - Formula: `1 if latest_earnings_date = trade_date else 0`

- `latest_earnings_date`
  - Meaning: Most recent earnings-related disclosure date on or before the trading date
  - Source: latest `market_data.fin_summary.Date` with `Date <= trade_date`

- `days_since_earnings`
  - Meaning: Calendar days since the most recent earnings-related disclosure
  - Formula: `trade_date - latest_earnings_date`

- `trading_days_since_earnings`
  - Meaning: Trading-day distance since the most recent earnings-related disclosure
  - Formula: `count(price_action rows between latest_earnings_date and trade_date) - 1`

- `next_earnings_date`
  - Meaning: Next scheduled earnings date on or after the trading date
  - Source: earliest `market_data.earnings_calendar.Date` with `Date >= trade_date`

- `days_to_next_earnings`
  - Meaning: Calendar days until the next scheduled earnings date
  - Formula: `next_earnings_date - trade_date`

- `earnings_event_window_1d`
  - Meaning: Whether the row is inside the first trading day after the most recent earnings disclosure
  - Formula: `1 if trading_days_since_earnings between 0 and 1 else 0`

- `earnings_event_window_3d`
  - Meaning: Whether the row is inside the first three trading days after the most recent earnings disclosure
  - Formula: `1 if trading_days_since_earnings between 0 and 3 else 0`

- `earnings_event_window_5d`
  - Meaning: Whether the row is inside the first five trading days after the most recent earnings disclosure
  - Formula: `1 if trading_days_since_earnings between 0 and 5 else 0`

### Financial Summary Attributes

- `latest_doc_type`
  - Meaning: `DocType` carried by the most recent financial summary row used for the trading date

- `material_change_subject`
  - Meaning: `MaterialChangesInSubsidiaries`-style subject text carried by the most recent financial summary row
  - Source note: preserved as raw official string

- `chg_by_as_rev_flag`
  - Meaning: Whether the most recent summary indicates a revision due to accounting-standard change
  - Formula: `1 if latest summary ChgByASRev is truthy else 0`

- `chg_no_as_rev_flag`
  - Meaning: Whether the most recent summary indicates no accounting-standard revision change flag
  - Formula: `1 if latest summary ChgNoASRev is truthy else 0`

- `chg_ac_est_flag`
  - Meaning: Whether the most recent summary indicates an accounting estimate change flag
  - Formula: `1 if latest summary ChgAcEst is truthy else 0`

### Guidance Revision Factors

- `latest_forecast_op`
  - Meaning: Latest forecast operating profit used for the trading date
  - Source: `market_data.fin_summary.ForecastOperatingProfit`
  - Type note: parsed with `try_cast(nullif(value, '') as double)`

- `latest_forecast_net`
  - Meaning: Latest forecast net income used for the trading date
  - Source: `market_data.fin_summary.ForecastProfit`
  - Type note: parsed with `try_cast(nullif(value, '') as double)`

- `latest_forecast_eps`
  - Meaning: Latest forecast EPS used for the trading date
  - Source: `market_data.fin_summary.ForecastEarningsPerShare`
  - Type note: parsed with `try_cast(nullif(value, '') as double)`

- `prev_forecast_op`
  - Meaning: Previous available forecast operating profit from the disclosure immediately before the latest one

- `prev_forecast_net`
  - Meaning: Previous available forecast net income from the disclosure immediately before the latest one

- `prev_forecast_eps`
  - Meaning: Previous available forecast EPS from the disclosure immediately before the latest one

- `forecast_op_revision_pct`
  - Meaning: Relative revision in forecast operating profit versus the prior disclosure
  - Formula: `(latest_forecast_op / prev_forecast_op) - 1`

- `forecast_net_revision_pct`
  - Meaning: Relative revision in forecast net income versus the prior disclosure
  - Formula: `(latest_forecast_net / prev_forecast_net) - 1`

- `forecast_eps_revision_pct`
  - Meaning: Relative revision in forecast EPS versus the prior disclosure
  - Formula: `(latest_forecast_eps / prev_forecast_eps) - 1`

- `forecast_revision_direction`
  - Meaning: Directional classification of the latest comparable guidance revision
  - Values:
    - `upward`
    - `downward`
    - `mixed`
    - `NULL`
  - Rule note:
    - `upward` when comparable revision fields are nonnegative and at least one positive
    - `downward` when comparable revision fields are nonpositive and at least one negative
    - `mixed` when comparable revision fields exist but signs conflict

### Dividend Event Factors

- `is_dividend_announcement_day`
  - Meaning: Whether the trading date itself is a dividend announcement date
  - Formula: `1 if latest_dividend_date = trade_date else 0`

- `latest_dividend_date`
  - Meaning: Most recent dividend announcement date on or before the trading date
  - Source: latest grouped `market_data.fin_dividend.PubDate` with `PubDate <= trade_date`

- `days_since_dividend_announcement`
  - Meaning: Calendar days since the latest dividend announcement
  - Formula: `trade_date - latest_dividend_date`

- `latest_dividend_rate`
  - Meaning: Sum of numeric `DivRate` values carried by the latest dividend announcement date used for the row
  - Type note: numeric strings are aggregated with `try_cast`; nonnumeric strings are ignored in the numeric sum

- `prev_dividend_rate`
  - Meaning: Sum of numeric `DivRate` values carried by the previous dividend announcement date

- `dividend_revision_pct`
  - Meaning: Relative change in latest dividend rate versus previous dividend rate
  - Formula: `(latest_dividend_rate / prev_dividend_rate) - 1`

- `dividend_revision_direction`
  - Meaning: Directional classification of the latest comparable dividend-rate change
  - Values:
    - `increase`
    - `decrease`
    - `maintain`
    - `NULL`

### Event Summary Flags

- `guidance_positive_flag`
  - Meaning: Whether the latest comparable guidance revision is directionally positive
  - Formula: `1 if forecast_revision_direction = 'upward' else 0`

- `guidance_negative_flag`
  - Meaning: Whether the latest comparable guidance revision is directionally negative
  - Formula: `1 if forecast_revision_direction = 'downward' else 0`

- `fundamental_event_conflict_flag`
  - Meaning: Whether the latest event set contains conflicting directional signals across guidance and dividend revisions
  - Rule note:
    - Set to `1` when one side is positive and another side is negative
    - Otherwise `0`

### Interpretation Notes

- This table is event-state oriented, not valuation oriented
- `days_since_earnings` is calendar-day based, while `trading_days_since_earnings` is trading-row based
- `forecast_*` fields depend on comparable previous disclosures; when no comparable prior disclosure exists, revision fields are `NULL`
- `dividend_*` fields only capture numeric `DivRate` revisions in the current implementation
- `earnings_event_window_*` flags are designed for short-horizon post-event analysis, especially next-day and next-week behavior

## Cross-Table Enhancements Not Yet Implemented

The following related enhancements are not part of the current implementation:

- Fallback from `S33` benchmark to `TOPIX-17` benchmark when a primary `S33` mapping is unavailable
- Market-wide cross-sectional percentile ranks
- Industry trend-state flags such as `industry_ma5_vs_ma20`
- Residual return or alpha-style factors after removing market beta
- Fundamental event strength scores or surprise normalization
- Balance-sheet and cash-flow change factors from `fins/details`
