from __future__ import annotations

from app.db import DuckDBRepository


S33_TO_TSE_SECTOR_INDEX = [
    ("0050", "0040"),
    ("1050", "0041"),
    ("2050", "0042"),
    ("3050", "0043"),
    ("3100", "0044"),
    ("3150", "0045"),
    ("3200", "0046"),
    ("3250", "0047"),
    ("3300", "0048"),
    ("3350", "0049"),
    ("3400", "004A"),
    ("3450", "004B"),
    ("3500", "004C"),
    ("3550", "004D"),
    ("3600", "004E"),
    ("3650", "004F"),
    ("3700", "0050"),
    ("3750", "0051"),
    ("3800", "0052"),
    ("4050", "0053"),
    ("5050", "0054"),
    ("5100", "0055"),
    ("5150", "0056"),
    ("5200", "0057"),
    ("5250", "0058"),
    ("6050", "0059"),
    ("6100", "005A"),
    ("7050", "005B"),
    ("7100", "005C"),
    ("7150", "005D"),
    ("7200", "005E"),
    ("8050", "005F"),
    ("9050", "0060"),
]


def _build_price_action_features_sql() -> str:
    industry_index_map_values = ",\n        ".join(
        f"('{sector_code}', '{index_code}')" for sector_code, index_code in S33_TO_TSE_SECTOR_INDEX
    )
    return f"""
create schema if not exists analytics;

create or replace table analytics.price_action_features as
with stock as (
    select
        cast("Date" as date) as trade_date,
        "Code" as code,
        "AdjO" as open,
        "AdjH" as high,
        "AdjL" as low,
        "AdjC" as close,
        "AdjVo" as volume,
        "Va" as value,
        "UL" as limit_up,
        "LL" as limit_down,
        lag("AdjC") over (partition by "Code" order by cast("Date" as date)) as prev_close,
        lag("AdjC", 3) over (partition by "Code" order by cast("Date" as date)) as close_3d_ago,
        lag("AdjC", 5) over (partition by "Code" order by cast("Date" as date)) as close_5d_ago,
        lag("AdjC", 20) over (partition by "Code" order by cast("Date" as date)) as close_20d_ago,
        avg("AdjVo") over (
            partition by "Code"
            order by cast("Date" as date)
            rows between 19 preceding and current row
        ) as avg_20d_volume,
        avg("Va") over (
            partition by "Code"
            order by cast("Date" as date)
            rows between 19 preceding and current row
        ) as avg_20d_value,
        ln(
            "AdjC" / nullif(
                lag("AdjC") over (partition by "Code" order by cast("Date" as date)),
                0
            )
        ) as log_return_1d
    from market_data.equity_daily_bar
    where "AdjC" is not null
),
stock_with_vol as (
    select
        *,
        stddev_samp(log_return_1d) over (
            partition by code
            order by trade_date
            rows between 4 preceding and current row
        ) as logret_vol_5,
        stddev_samp(log_return_1d) over (
            partition by code
            order by trade_date
            rows between 19 preceding and current row
        ) as logret_vol_20
    from stock
),
equity_classification as (
    select
        cast("Date" as date) as snapshot_date,
        "Code" as code,
        "S33" as s33,
        "S33Nm" as s33_name
    from market_data.equity_master
),
industry_index_map as (
    select *
    from (
        values
        {industry_index_map_values}
    ) as mapping(s33, industry_index_code)
),
topix as (
    select
        cast("Date" as date) as trade_date,
        "C" as topix_close,
        lag("C") over (order by cast("Date" as date)) as topix_prev_close,
        lag("C", 3) over (order by cast("Date" as date)) as topix_close_3d_ago,
        lag("C", 5) over (order by cast("Date" as date)) as topix_close_5d_ago,
        lag("C", 20) over (order by cast("Date" as date)) as topix_close_20d_ago
    from market_data.topix_daily_bar
),
industry_index as (
    select
        cast(idx."Date" as date) as trade_date,
        idx."Code" as industry_index_code,
        idx."C" as industry_close,
        lag(idx."C") over (
            partition by idx."Code"
            order by cast(idx."Date" as date)
        ) as industry_prev_close,
        lag(idx."C", 3) over (
            partition by idx."Code"
            order by cast(idx."Date" as date)
        ) as industry_close_3d_ago,
        lag(idx."C", 5) over (
            partition by idx."Code"
            order by cast(idx."Date" as date)
        ) as industry_close_5d_ago,
        lag(idx."C", 20) over (
            partition by idx."Code"
            order by cast(idx."Date" as date)
        ) as industry_close_20d_ago
    from market_data.index_daily_bar idx
    join industry_index_map iim on idx."Code" = iim.industry_index_code
),
stock_with_classification as (
    select
        s.*,
        ec.s33,
        ec.s33_name
    from stock_with_vol s
    left join lateral (
        select
            s33,
            s33_name
        from equity_classification ec
        where ec.code = s.code
        order by ec.snapshot_date desc
        limit 1
    ) ec on true
),
joined as (
    select
        s.*,
        iim.industry_index_code,
        t.topix_close,
        t.topix_prev_close,
        t.topix_close_3d_ago,
        t.topix_close_5d_ago,
        t.topix_close_20d_ago,
        ii.industry_close,
        ii.industry_prev_close,
        ii.industry_close_3d_ago,
        ii.industry_close_5d_ago,
        ii.industry_close_20d_ago,
        greatest(
            s.high - s.low,
            abs(s.high - s.prev_close),
            abs(s.low - s.prev_close)
        ) as true_range
    from stock_with_classification s
    left join industry_index_map iim
        on s.s33 = iim.s33
    left join topix t
        on s.trade_date = t.trade_date
    left join industry_index ii
        on s.trade_date = ii.trade_date and iim.industry_index_code = ii.industry_index_code
)
select
    trade_date,
    code,
    s33,
    s33_name,
    industry_index_code,
    open,
    high,
    low,
    close,
    volume,
    value,
    (close / nullif(prev_close, 0) - 1.0) as ret_1d,
    (close / nullif(close_3d_ago, 0) - 1.0) as ret_3d,
    (close / nullif(close_5d_ago, 0) - 1.0) as ret_5d,
    (close / nullif(close_20d_ago, 0) - 1.0) as ret_20d,
    (topix_close / nullif(topix_prev_close, 0) - 1.0) as topix_ret_1d,
    (topix_close / nullif(topix_close_3d_ago, 0) - 1.0) as topix_ret_3d,
    (topix_close / nullif(topix_close_5d_ago, 0) - 1.0) as topix_ret_5d,
    (topix_close / nullif(topix_close_20d_ago, 0) - 1.0) as topix_ret_20d,
    (industry_close / nullif(industry_prev_close, 0) - 1.0) as industry_ret_1d,
    (industry_close / nullif(industry_close_3d_ago, 0) - 1.0) as industry_ret_3d,
    (industry_close / nullif(industry_close_5d_ago, 0) - 1.0) as industry_ret_5d,
    (industry_close / nullif(industry_close_20d_ago, 0) - 1.0) as industry_ret_20d,
    (close / nullif(prev_close, 0) - 1.0) - (topix_close / nullif(topix_prev_close, 0) - 1.0) as excess_ret_1d,
    (close / nullif(close_3d_ago, 0) - 1.0) - (topix_close / nullif(topix_close_3d_ago, 0) - 1.0) as excess_ret_3d,
    (close / nullif(close_5d_ago, 0) - 1.0) - (topix_close / nullif(topix_close_5d_ago, 0) - 1.0) as excess_ret_5d,
    (close / nullif(close_20d_ago, 0) - 1.0) - (topix_close / nullif(topix_close_20d_ago, 0) - 1.0) as excess_ret_20d,
    (close / nullif(prev_close, 0) - 1.0) - (industry_close / nullif(industry_prev_close, 0) - 1.0) as industry_excess_ret_1d,
    (close / nullif(close_3d_ago, 0) - 1.0) - (industry_close / nullif(industry_close_3d_ago, 0) - 1.0) as industry_excess_ret_3d,
    (close / nullif(close_5d_ago, 0) - 1.0) - (industry_close / nullif(industry_close_5d_ago, 0) - 1.0) as industry_excess_ret_5d,
    (close / nullif(close_20d_ago, 0) - 1.0) - (industry_close / nullif(industry_close_20d_ago, 0) - 1.0) as industry_excess_ret_20d,
    (high - low) / nullif(prev_close, 0) as intraday_range_pct,
    abs(close - open) / nullif(high - low, 0) as body_ratio,
    (high - greatest(open, close)) / nullif(high - low, 0) as upper_shadow_ratio,
    (least(open, close) - low) / nullif(high - low, 0) as lower_shadow_ratio,
    (close - low) / nullif(high - low, 0) as close_position,
    (open / nullif(prev_close, 0) - 1.0) as gap_pct,
    volume / nullif(avg_20d_volume, 0) as volume_shock_20d_inclusive,
    value / nullif(avg_20d_value, 0) as value_shock_20d_inclusive,
    true_range,
    avg(true_range) over (
        partition by code
        order by trade_date
        rows between 4 preceding and current row
    ) as mean_true_range_5,
    avg(true_range) over (
        partition by code
        order by trade_date
        rows between 19 preceding and current row
    ) as mean_true_range_20,
    logret_vol_5,
    logret_vol_20,
    case when limit_up = '1' then 1 else 0 end as hit_limit_up,
    case when limit_down = '1' then 1 else 0 end as hit_limit_down
from joined
order by code, trade_date;
"""


PRICE_ACTION_FEATURES_SQL = _build_price_action_features_sql()


MARKET_INDUSTRY_LINKAGE_FEATURES_SQL = """
create schema if not exists analytics;

create or replace table analytics.market_industry_linkage_features as
with base as (
    select
        trade_date,
        code,
        s33,
        s33_name,
        industry_index_code,
        close,
        ret_1d,
        ret_5d,
        ret_20d,
        topix_ret_1d,
        topix_ret_5d,
        industry_ret_1d,
        industry_ret_5d,
        volume_shock_20d_inclusive,
        max(close) over (
            partition by code
            order by trade_date
            rows between 19 preceding and current row
        ) as rolling_high_20d
    from analytics.price_action_features
),
topix_state as (
    select
        cast("Date" as date) as trade_date,
        "C" as topix_close,
        avg("C") over (
            order by cast("Date" as date)
            rows between 19 preceding and current row
        ) as topix_ma20
    from market_data.topix_daily_bar
),
industry_state as (
    select
        cast("Date" as date) as trade_date,
        "Code" as industry_index_code,
        "C" as industry_close,
        avg("C") over (
            partition by "Code"
            order by cast("Date" as date)
            rows between 19 preceding and current row
        ) as industry_ma20
    from market_data.index_daily_bar
),
market_breadth as (
    select
        trade_date,
        avg(case when ret_1d > 0 then 1.0 else 0.0 end) as market_up_ratio,
        avg(case when close >= rolling_high_20d then 1.0 else 0.0 end) as market_new_high_20d_ratio,
        avg(case when ret_1d > 0 and volume_shock_20d_inclusive > 1 then 1.0 else 0.0 end) as market_volume_up_ratio
    from base
    group by trade_date
),
industry_breadth as (
    select
        trade_date,
        s33,
        avg(case when ret_1d > 0 then 1.0 else 0.0 end) as industry_up_ratio,
        avg(case when close >= rolling_high_20d then 1.0 else 0.0 end) as industry_new_high_20d_ratio,
        avg(case when ret_1d > 0 and volume_shock_20d_inclusive > 1 then 1.0 else 0.0 end) as industry_volume_up_ratio
    from base
    where s33 is not null
    group by trade_date, s33
),
ranked as (
    select
        b.*,
        (ts.topix_close / nullif(ts.topix_ma20, 0) - 1.0) as topix_close_vs_ma20,
        (ist.industry_close / nullif(ist.industry_ma20, 0) - 1.0) as industry_close_vs_ma20,
        mb.market_up_ratio,
        mb.market_new_high_20d_ratio,
        mb.market_volume_up_ratio,
        ib.industry_up_ratio,
        ib.industry_new_high_20d_ratio,
        ib.industry_volume_up_ratio,
        percent_rank() over (
            partition by b.trade_date, b.s33
            order by b.ret_1d
        ) as industry_strength_pct_1d,
        percent_rank() over (
            partition by b.trade_date, b.s33
            order by b.ret_5d
        ) as industry_strength_pct_5d,
        percent_rank() over (
            partition by b.trade_date, b.s33
            order by b.ret_20d
        ) as industry_strength_pct_20d
    from base b
    left join topix_state ts using (trade_date)
    left join industry_state ist
        on b.trade_date = ist.trade_date and b.industry_index_code = ist.industry_index_code
    left join market_breadth mb using (trade_date)
    left join industry_breadth ib
        on b.trade_date = ib.trade_date and b.s33 = ib.s33
),
with_beta as (
    select
        *,
        covar_samp(ret_1d, topix_ret_1d) over (
            partition by code
            order by trade_date
            rows between 19 preceding and current row
        ) / nullif(
            var_samp(topix_ret_1d) over (
                partition by code
                order by trade_date
                rows between 19 preceding and current row
            ),
            0
        ) as beta_20,
        covar_samp(ret_1d, topix_ret_1d) over (
            partition by code
            order by trade_date
            rows between 59 preceding and current row
        ) / nullif(
            var_samp(topix_ret_1d) over (
                partition by code
                order by trade_date
                rows between 59 preceding and current row
            ),
            0
        ) as beta_60
    from ranked
)
select
    trade_date,
    code,
    s33,
    s33_name,
    topix_ret_1d,
    topix_ret_5d,
    topix_close_vs_ma20,
    industry_ret_1d,
    industry_ret_5d,
    industry_close_vs_ma20,
    ret_1d - industry_ret_1d as industry_excess_ret_1d,
    ret_5d - industry_ret_5d as industry_excess_ret_5d,
    market_up_ratio,
    market_new_high_20d_ratio,
    market_volume_up_ratio,
    industry_up_ratio,
    industry_new_high_20d_ratio,
    industry_volume_up_ratio,
    industry_strength_pct_1d,
    industry_strength_pct_5d,
    industry_strength_pct_20d,
    beta_20,
    beta_60
from with_beta
order by code, trade_date;
"""


def build_price_action_features(repository: DuckDBRepository) -> int:
    repository.execute(PRICE_ACTION_FEATURES_SQL)
    result = repository.query("select count(*) as row_count from analytics.price_action_features")
    return int(result["row_count"].iloc[0])


def build_market_industry_linkage_features(repository: DuckDBRepository) -> int:
    repository.execute(MARKET_INDUSTRY_LINKAGE_FEATURES_SQL)
    result = repository.query("select count(*) as row_count from analytics.market_industry_linkage_features")
    return int(result["row_count"].iloc[0])


FLOW_STRUCTURE_FEATURES_SQL = """
create schema if not exists analytics;

create or replace table analytics.flow_structure_features as
with base as (
    select
        p.trade_date,
        p.code,
        p.s33,
        p.s33_name,
        b.LongSellVa,
        b.ShrtNoMrgnVa,
        b.MrgnSellNewVa,
        b.MrgnSellCloseVa,
        b.LongBuyVa,
        b.MrgnBuyNewVa,
        b.MrgnBuyCloseVa,
        sr.SellExShortVa,
        sr.ShrtWithResVa,
        sr.ShrtNoResVa,
        ma.SLRatio,
        ma.LongOutChg,
        ma.ShrtOutChg,
        ma.PubDate as margin_alert_pub_date,
        ssr.ShrtPosToSO,
        ssr.PrevRptRatio,
        ssr.CalcDate as short_sale_calc_date
    from analytics.price_action_features p
    left join market_data.market_breakdown b
        on p.trade_date = cast(b."Date" as date) and p.code = b."Code"
    left join market_data.short_ratio sr
        on p.trade_date = cast(sr."Date" as date) and p.s33 = sr."S33"
    left join lateral (
        select *
        from market_data.margin_alert ma
        where p.code = ma."Code"
          and try_cast(ma."PubDate" as date) = (
              select max(try_cast(ma2."PubDate" as date))
              from market_data.margin_alert ma2
              where ma2."Code" = p.code
                and try_cast(ma2."PubDate" as date) <= p.trade_date
          )
    ) ma on true
    left join lateral (
        select *
        from market_data.short_sale_report ssr
        where p.code = ssr."Code"
          and try_cast(ssr."CalcDate" as date) = (
              select max(try_cast(ssr2."CalcDate" as date))
              from market_data.short_sale_report ssr2
              where ssr2."Code" = p.code
                and try_cast(ssr2."CalcDate" as date) <= p.trade_date
          )
    ) ssr on true
),
totals as (
    select
        *,
        coalesce(LongSellVa, 0) + coalesce(ShrtNoMrgnVa, 0) + coalesce(MrgnSellNewVa, 0) + coalesce(MrgnSellCloseVa, 0) as total_sell_va,
        coalesce(LongBuyVa, 0) + coalesce(MrgnBuyNewVa, 0) + coalesce(MrgnBuyCloseVa, 0) as total_buy_va,
        coalesce(SellExShortVa, 0) + coalesce(ShrtWithResVa, 0) + coalesce(ShrtNoResVa, 0) as sector_total_sell_va
    from base
)
select
    trade_date,
    code,
    s33,
    s33_name,
    total_buy_va,
    total_sell_va,
    total_buy_va + total_sell_va as total_trade_va,
    LongBuyVa / nullif(total_buy_va, 0) as long_buy_share_of_buy_va,
    MrgnBuyNewVa / nullif(total_buy_va, 0) as margin_buy_new_share_of_buy_va,
    MrgnBuyCloseVa / nullif(total_buy_va, 0) as margin_buy_close_share_of_buy_va,
    LongSellVa / nullif(total_sell_va, 0) as long_sell_share_of_sell_va,
    ShrtNoMrgnVa / nullif(total_sell_va, 0) as short_nomargin_share_of_sell_va,
    MrgnSellNewVa / nullif(total_sell_va, 0) as margin_sell_new_share_of_sell_va,
    MrgnSellCloseVa / nullif(total_sell_va, 0) as margin_sell_close_share_of_sell_va,
    coalesce(LongBuyVa, 0) - coalesce(LongSellVa, 0) as net_cash_va,
    coalesce(MrgnBuyNewVa, 0) - coalesce(MrgnSellNewVa, 0) as net_margin_new_va,
    coalesce(MrgnBuyCloseVa, 0) - coalesce(MrgnSellCloseVa, 0) as net_margin_close_va,
    SellExShortVa,
    ShrtWithResVa,
    ShrtNoResVa,
    (coalesce(ShrtWithResVa, 0) + coalesce(ShrtNoResVa, 0)) / nullif(sector_total_sell_va, 0) as sector_short_ratio,
    SLRatio as margin_alert_sl_ratio,
    LongOutChg as margin_alert_long_out_chg,
    ShrtOutChg as margin_alert_short_out_chg,
    margin_alert_pub_date,
    ShrtPosToSO as short_sale_pos_to_so,
    PrevRptRatio as short_sale_prev_ratio,
    ShrtPosToSO - PrevRptRatio as short_sale_ratio_change,
    short_sale_calc_date
from totals
order by code, trade_date;
"""


def build_flow_structure_features(repository: DuckDBRepository) -> int:
    repository.execute(
        """
        create schema if not exists market_data;
        create table if not exists market_data.market_breakdown (
            "Date" DATE,
            "Code" VARCHAR,
            "LongSellVa" DOUBLE,
            "ShrtNoMrgnVa" DOUBLE,
            "MrgnSellNewVa" DOUBLE,
            "MrgnSellCloseVa" DOUBLE,
            "LongBuyVa" DOUBLE,
            "MrgnBuyNewVa" DOUBLE,
            "MrgnBuyCloseVa" DOUBLE
        );
        create table if not exists market_data.short_ratio (
            "Date" DATE,
            "S33" VARCHAR,
            "SellExShortVa" DOUBLE,
            "ShrtWithResVa" DOUBLE,
            "ShrtNoResVa" DOUBLE
        );
        create table if not exists market_data.margin_alert (
            "PubDate" DATE,
            "Code" VARCHAR,
            "AppDate" DATE,
            "SLRatio" DOUBLE,
            "LongOutChg" VARCHAR,
            "ShrtOutChg" VARCHAR
        );
        create table if not exists market_data.short_sale_report (
            "DiscDate" DATE,
            "CalcDate" DATE,
            "Code" VARCHAR,
            "ShrtPosToSO" DOUBLE,
            "PrevRptRatio" DOUBLE
        );
        """
    )
    repository.execute(FLOW_STRUCTURE_FEATURES_SQL)
    result = repository.query("select count(*) as row_count from analytics.flow_structure_features")
    return int(result["row_count"].iloc[0])


FUNDAMENTAL_EVENT_FEATURES_SQL = """
create schema if not exists analytics;
create schema if not exists market_data;

create table if not exists market_data.fin_summary (
    "DiscDate" DATE,
    "Code" VARCHAR,
    "DiscNo" VARCHAR,
    "DocType" VARCHAR,
    "FOP" VARCHAR,
    "FNP" VARCHAR,
    "FEPS" VARCHAR,
    "MatChgSub" VARCHAR,
    "ChgByASRev" VARCHAR,
    "ChgNoASRev" VARCHAR,
    "ChgAcEst" VARCHAR
);
create table if not exists market_data.fin_dividend (
    "PubDate" DATE,
    "Code" VARCHAR,
    "RefNo" VARCHAR,
    "DivRate" VARCHAR
);
create table if not exists market_data.earnings_calendar (
    "Date" DATE,
    "Code" VARCHAR
);

create or replace table analytics.fundamental_event_features as
with base as (
    select
        trade_date,
        code
    from analytics.price_action_features
),
summary_events as (
    select
        cast("DiscDate" as date) as disc_date,
        "Code" as code,
        "DiscNo" as disc_no,
        "DocType" as doc_type,
        try_cast(nullif("FOP", '') as double) as forecast_op,
        try_cast(nullif("FNP", '') as double) as forecast_net,
        try_cast(nullif("FEPS", '') as double) as forecast_eps,
        nullif("MatChgSub", '') as material_change_subject,
        case when lower(coalesce("ChgByASRev", '')) in ('true', '1') then 1 else 0 end as chg_by_as_rev_flag,
        case when lower(coalesce("ChgNoASRev", '')) in ('true', '1') then 1 else 0 end as chg_no_as_rev_flag,
        case when lower(coalesce("ChgAcEst", '')) in ('true', '1') then 1 else 0 end as chg_ac_est_flag
    from market_data.fin_summary
),
dividend_events as (
    select
        cast("PubDate" as date) as pub_date,
        "Code" as code,
        sum(coalesce(try_cast(nullif("DivRate", '') as double), 0.0)) as dividend_rate_total
    from market_data.fin_dividend
    group by 1, 2
),
enriched as (
    select
        b.trade_date,
        b.code,
        latest_summary.disc_date as latest_earnings_date,
        latest_summary.doc_type as latest_doc_type,
        latest_summary.material_change_subject,
        latest_summary.chg_by_as_rev_flag,
        latest_summary.chg_no_as_rev_flag,
        latest_summary.chg_ac_est_flag,
        latest_summary.forecast_op as latest_forecast_op,
        latest_summary.forecast_net as latest_forecast_net,
        latest_summary.forecast_eps as latest_forecast_eps,
        prev_summary.forecast_op as prev_forecast_op,
        prev_summary.forecast_net as prev_forecast_net,
        prev_summary.forecast_eps as prev_forecast_eps,
        next_earnings.next_earnings_date,
        latest_dividend.latest_dividend_date,
        latest_dividend.latest_dividend_rate,
        prev_dividend.prev_dividend_rate,
        (
            select count(*) - 1
            from analytics.price_action_features px
            where px.code = b.code
              and latest_summary.disc_date is not null
              and px.trade_date between latest_summary.disc_date and b.trade_date
        ) as trading_days_since_earnings
    from base b
    left join lateral (
        select *
        from summary_events s
        where s.code = b.code
          and s.disc_date <= b.trade_date
        order by s.disc_date desc, s.disc_no desc
        limit 1
    ) latest_summary on true
    left join lateral (
        select *
        from summary_events s
        where s.code = b.code
          and latest_summary.disc_date is not null
          and s.disc_date < latest_summary.disc_date
        order by s.disc_date desc, s.disc_no desc
        limit 1
    ) prev_summary on true
    left join lateral (
        select
            cast("Date" as date) as next_earnings_date
        from market_data.earnings_calendar ec
        where ec."Code" = b.code
          and cast(ec."Date" as date) >= b.trade_date
        order by cast(ec."Date" as date) asc
        limit 1
    ) next_earnings on true
    left join lateral (
        select
            d.pub_date as latest_dividend_date,
            d.dividend_rate_total as latest_dividend_rate
        from dividend_events d
        where d.code = b.code
          and d.pub_date <= b.trade_date
        order by d.pub_date desc
        limit 1
    ) latest_dividend on true
    left join lateral (
        select
            d.dividend_rate_total as prev_dividend_rate
        from dividend_events d
        where d.code = b.code
          and latest_dividend.latest_dividend_date is not null
          and d.pub_date < latest_dividend.latest_dividend_date
        order by d.pub_date desc
        limit 1
    ) prev_dividend on true
)
select
    trade_date,
    code,
    case when latest_earnings_date = trade_date then 1 else 0 end as is_earnings_day,
    latest_earnings_date,
    date_diff('day', latest_earnings_date, trade_date) as days_since_earnings,
    trading_days_since_earnings,
    next_earnings_date,
    date_diff('day', trade_date, next_earnings_date) as days_to_next_earnings,
    case when trading_days_since_earnings between 0 and 1 then 1 else 0 end as earnings_event_window_1d,
    case when trading_days_since_earnings between 0 and 3 then 1 else 0 end as earnings_event_window_3d,
    case when trading_days_since_earnings between 0 and 5 then 1 else 0 end as earnings_event_window_5d,
    latest_doc_type,
    material_change_subject,
    chg_by_as_rev_flag,
    chg_no_as_rev_flag,
    chg_ac_est_flag,
    latest_forecast_op,
    latest_forecast_net,
    latest_forecast_eps,
    prev_forecast_op,
    prev_forecast_net,
    prev_forecast_eps,
    (latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0) as forecast_op_revision_pct,
    (latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0) as forecast_net_revision_pct,
    (latest_forecast_eps - prev_forecast_eps) / nullif(abs(prev_forecast_eps), 0) as forecast_eps_revision_pct,
    case
        when latest_forecast_op is null and latest_forecast_net is null and latest_forecast_eps is null then null
        when coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) > 0
         and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) >= 0
         and coalesce((latest_forecast_eps - prev_forecast_eps) / nullif(abs(prev_forecast_eps), 0), 0) >= 0
            then 'upward'
        when coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) < 0
         and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) <= 0
         and coalesce((latest_forecast_eps - prev_forecast_eps) / nullif(abs(prev_forecast_eps), 0), 0) <= 0
            then 'downward'
        when prev_forecast_op is null and prev_forecast_net is null and prev_forecast_eps is null then null
        else 'mixed'
    end as forecast_revision_direction,
    case when latest_dividend_date = trade_date then 1 else 0 end as is_dividend_announcement_day,
    latest_dividend_date,
    date_diff('day', latest_dividend_date, trade_date) as days_since_dividend_announcement,
    latest_dividend_rate,
    prev_dividend_rate,
    (latest_dividend_rate - prev_dividend_rate) / nullif(abs(prev_dividend_rate), 0) as dividend_revision_pct,
    case
        when latest_dividend_rate is null then null
        when prev_dividend_rate is null then null
        when latest_dividend_rate > prev_dividend_rate then 'increase'
        when latest_dividend_rate < prev_dividend_rate then 'decrease'
        else 'maintain'
    end as dividend_revision_direction,
    case
        when coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) > 0
         and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) > 0
            then 1
        else 0
    end as guidance_positive_flag,
    case
        when coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) < 0
         and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) < 0
            then 1
        else 0
    end as guidance_negative_flag,
    case
        when (
            coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) > 0
            and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) < 0
        ) or (
            coalesce((latest_forecast_op - prev_forecast_op) / nullif(abs(prev_forecast_op), 0), 0) < 0
            and coalesce((latest_forecast_net - prev_forecast_net) / nullif(abs(prev_forecast_net), 0), 0) > 0
        ) then 1
        else 0
    end as fundamental_event_conflict_flag
from enriched
order by code, trade_date;
"""


def build_fundamental_event_features(repository: DuckDBRepository) -> int:
    repository.execute(FUNDAMENTAL_EVENT_FEATURES_SQL)
    result = repository.query("select count(*) as row_count from analytics.fundamental_event_features")
    return int(result["row_count"].iloc[0])


NEXT_DAY_LABELS_SQL = """
create schema if not exists analytics;

create or replace table analytics.next_day_labels as
with base as (
    select
        paf.trade_date,
        paf.code,
        paf.close,
        paf.topix_ret_1d,
        paf.industry_ret_1d,
        lead(paf.trade_date) over (
            partition by paf.code
            order by paf.trade_date
        ) as next_trade_date,
        lead(paf.close) over (
            partition by paf.code
            order by paf.trade_date
        ) as next_close,
        lead(paf.close, 3) over (
            partition by paf.code
            order by paf.trade_date
        ) as close_plus_3d,
        lead(paf.topix_ret_1d) over (
            partition by paf.code
            order by paf.trade_date
        ) as next_topix_ret_1d,
        lead(paf.industry_ret_1d) over (
            partition by paf.code
            order by paf.trade_date
        ) as next_industry_ret_1d
    from analytics.price_action_features paf
),
labeled as (
    select
        trade_date,
        code,
        next_trade_date,
        (next_close / nullif(close, 0) - 1.0) as label_next_ret_1d,
        case
            when next_close is null or close is null then null
            when (next_close / nullif(close, 0) - 1.0) > 0 then 1
            else 0
        end as label_next_up_1d,
        (next_close / nullif(close, 0) - 1.0) - next_topix_ret_1d as label_next_excess_ret_1d,
        (next_close / nullif(close, 0) - 1.0) - next_industry_ret_1d as label_next_industry_excess_ret_1d,
        (close_plus_3d / nullif(close, 0) - 1.0) as label_next_ret_3d,
        case
            when close_plus_3d is null or close is null then null
            when (close_plus_3d / nullif(close, 0) - 1.0) > 0 then 1
            else 0
        end as label_next_up_3d,
        case
            when next_close is null or close is null then null
            when (next_close / nullif(close, 0) - 1.0) >= 0.02 then 'bullish'
            when (next_close / nullif(close, 0) - 1.0) <= -0.02 then 'bearish'
            else 'neutral'
        end as label_next_direction_1d
    from base
)
select *
from labeled
order by code, trade_date;
"""


def build_next_day_labels(repository: DuckDBRepository) -> int:
    repository.execute(NEXT_DAY_LABELS_SQL)
    result = repository.query("select count(*) as row_count from analytics.next_day_labels")
    return int(result["row_count"].iloc[0])
