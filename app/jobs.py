from __future__ import annotations

from datetime import date, timedelta
import gzip
import json
import re
from pathlib import Path

import duckdb
import pandas as pd
from pandas.api import types as ptypes
from pandas import DatetimeTZDtype

from app.aggregator import merge_records
from app.clients import ExternalAPIClient, JQuantsClient
from app.config import Settings
from app.db import DuckDBRepository, save_summary
from app.features import (
    build_flow_structure_features,
    build_fundamental_event_features,
    build_market_industry_linkage_features,
    build_next_day_labels,
    build_price_action_features,
)
from app.llm import (
    get_llm_provider,
    get_openai_batch_runner,
    load_bundle_inputs,
    load_provider_prompt,
    prompt_filename,
    slugify_model_name,
)
from app.logging import get_logger
from app.models import MetricRecord
from app.stats import summarize_by_category


logger = get_logger(__name__)

FIN_DIVIDEND_MIXED_TYPE_COLUMNS = [
    "DivRate",
    "DistAmt",
    "RetEarn",
    "DeemDiv",
    "DeemCapGains",
    "NetAssetDecRatio",
    "CommDivRate",
    "SpecDivRate",
]

MARGIN_ALERT_MIXED_TYPE_COLUMNS = [
    "ShrtOutChg",
    "ShrtOutRatio",
    "LongOutChg",
    "LongOutRatio",
    "ShrtNegOutChg",
    "ShrtStdOutChg",
    "LongNegOutChg",
    "LongStdOutChg",
]


async def run_batch_job(settings: Settings) -> dict[str, float]:
    """Fetch, normalize, aggregate, summarize, and persist batch results."""

    client = ExternalAPIClient(
        base_url=settings.api_base_url,
        timeout=settings.api_timeout_seconds,
    )

    logger.info("batch_job_started", dataset_a=settings.dataset_a_path, dataset_b=settings.dataset_b_path)
    upstream_a = await client.fetch_json(settings.dataset_a_path)
    upstream_b = await client.fetch_json(settings.dataset_b_path)

    records_a = [_normalize_item("dataset-a", item) for item in upstream_a]
    records_b = [_normalize_item("dataset-b", item) for item in upstream_b]

    merged = merge_records(records_a, records_b)
    summary = summarize_by_category(merged)
    repository = _build_repository(settings)
    save_summary(repository, summary)
    logger.info("batch_job_finished", record_count=len(merged), category_count=len(summary))
    return summary


async def sync_equity_master(settings: Settings, *, code: str | None = None, date: str | None = None) -> int:
    """Fetch /v2/equities/master and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("equity_master_sync_started", code=code, date=date)
    dataframe = client.fetch_equities_master(code=code, date=date)
    upserted_count = repository.upsert_table("market_data.equity_master", dataframe, key_columns=["Date", "Code"])
    logger.info("equity_master_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_reference_market_segments(settings: Settings) -> int:
    """Sync official market segment code table into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("reference_market_segments_sync_started")
    dataframe = client.fetch_market_segments()
    replaced_count = repository.replace_table("reference.market_segments", dataframe)
    logger.info("reference_market_segments_sync_finished", row_count=replaced_count)
    return replaced_count


async def sync_reference_sector17(settings: Settings) -> int:
    """Sync official 17-sector code table into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("reference_sector17_sync_started")
    dataframe = client.fetch_sector17()
    replaced_count = repository.replace_table("reference.sector17", dataframe)
    logger.info("reference_sector17_sync_finished", row_count=replaced_count)
    return replaced_count


async def sync_reference_sector33(settings: Settings) -> int:
    """Sync official 33-sector code table into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("reference_sector33_sync_started")
    dataframe = client.fetch_sector33()
    replaced_count = repository.replace_table("reference.sector33", dataframe)
    logger.info("reference_sector33_sync_finished", row_count=replaced_count)
    return replaced_count


async def sync_equity_daily_bars(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/equities/bars/daily and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("equity_daily_bar_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_equity_daily_bars(code=code, date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.equity_daily_bar", dataframe, key_columns=["Date", "Code"])
    logger.info("equity_daily_bar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_market_calendar(
    settings: Settings,
    *,
    holiday_division: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/markets/calendar and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info(
        "market_calendar_sync_started",
        holiday_division=holiday_division,
        from_date=from_date,
        to_date=to_date,
    )
    dataframe = client.fetch_market_calendar(
        holiday_division=holiday_division,
        from_date=from_date,
        to_date=to_date,
    )
    upserted_count = repository.upsert_table("market_data.market_calendar", dataframe, key_columns=["Date"])
    logger.info("market_calendar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_margin_interest(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/markets/margin-interest and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("margin_interest_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_margin_interest(code=code, date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.margin_interest", dataframe, key_columns=["Date", "Code"])
    logger.info("margin_interest_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_market_breakdown(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/markets/breakdown and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("market_breakdown_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_market_breakdown(code=code, date=date, from_date=from_date, to_date=to_date)
    _ensure_market_breakdown_schema(repository, dataframe)
    upserted_count = repository.upsert_table("market_data.market_breakdown", dataframe, key_columns=["Date", "Code"])
    logger.info("market_breakdown_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_short_ratio(
    settings: Settings,
    *,
    s33: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/markets/short-ratio and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("short_ratio_sync_started", s33=s33, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_short_ratio(s33=s33, date=date, from_date=from_date, to_date=to_date)
    _ensure_short_ratio_schema(repository, dataframe)
    upserted_count = repository.upsert_table("market_data.short_ratio", dataframe, key_columns=["Date", "S33"])
    logger.info("short_ratio_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_margin_alert(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/markets/margin-alert and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("margin_alert_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = _normalize_margin_alert_frame(
        client.fetch_margin_alert(code=code, date=date, from_date=from_date, to_date=to_date)
    )
    _ensure_margin_alert_schema(repository, dataframe)
    upserted_count = repository.upsert_table(
        "market_data.margin_alert",
        dataframe,
        key_columns=["PubDate", "Code", "AppDate"],
    )
    logger.info("margin_alert_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_short_sale_report(
    settings: Settings,
    *,
    code: str | None = None,
    disclosed_date: str | None = None,
    disclosed_date_from: str | None = None,
    disclosed_date_to: str | None = None,
    calculated_date: str | None = None,
) -> int:
    """Fetch /v2/markets/short-sale-report and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info(
        "short_sale_report_sync_started",
        code=code,
        disclosed_date=disclosed_date,
        disclosed_date_from=disclosed_date_from,
        disclosed_date_to=disclosed_date_to,
        calculated_date=calculated_date,
    )
    dataframe = client.fetch_short_sale_report(
        code=code,
        disclosed_date=disclosed_date,
        disclosed_date_from=disclosed_date_from,
        disclosed_date_to=disclosed_date_to,
        calculated_date=calculated_date,
    )
    upserted_count = repository.upsert_table(
        "market_data.short_sale_report",
        dataframe,
        key_columns=["DiscDate", "CalcDate", "Code", "SSName", "DICName", "FundName"],
    )
    logger.info("short_sale_report_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_topix_daily_bars(
    settings: Settings,
    *,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/indices/bars/daily/topix and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("topix_daily_bar_sync_started", date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_topix_daily_bars(date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.topix_daily_bar", dataframe, key_columns=["Date"])
    logger.info("topix_daily_bar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_index_daily_bars(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Import index daily bars from J-Quants bulk files into DuckDB."""

    if code:
        raise ValueError("Index bulk sync does not support code-scoped imports")
    client = _build_jquants_client(settings)
    repository = _build_repository(settings)
    if not any([date, from_date, to_date]):
        raise ValueError("date or from_date/to_date is required for bulk index sync")

    logger.info("index_daily_bar_bulk_sync_started", date=date, from_date=from_date, to_date=to_date)
    total_upserted = _import_index_bulk_files(
        client=client,
        repository=repository,
        bulk_root=Path(settings.jquants_bulk_download_dir),
        start_date=from_date or date or "",
        end_date=to_date or date or from_date or "",
    )
    logger.info("index_daily_bar_bulk_sync_finished", row_count=total_upserted)
    return total_upserted


async def sync_earnings_calendar(settings: Settings) -> int:
    """Fetch /v2/equities/earnings-calendar and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("earnings_calendar_sync_started")
    dataframe = client.fetch_earnings_calendar()
    upserted_count = repository.upsert_table("market_data.earnings_calendar", dataframe, key_columns=["Date", "Code"])
    logger.info("earnings_calendar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_fins_summary(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/fins/summary and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("fins_summary_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_fins_summary(code=code, date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.fin_summary", dataframe, key_columns=["DiscNo"])
    logger.info("fins_summary_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_fins_dividend(
    settings: Settings,
    *,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/fins/dividend and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("fins_dividend_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = _normalize_fin_dividend_frame(
        client.fetch_fins_dividend(code=code, date=date, from_date=from_date, to_date=to_date)
    )
    _ensure_fin_dividend_schema(repository, dataframe)
    upserted_count = repository.upsert_table("market_data.fin_dividend", dataframe, key_columns=["RefNo"])
    logger.info("fins_dividend_sync_finished", row_count=upserted_count)
    return upserted_count


async def backfill_index_daily_bars(
    settings: Settings,
    *,
    start_date: str = "2008-05-07",
    end_date: str | None = None,
) -> int:
    """Backfill /v2/indices/bars/daily from J-Quants bulk files."""

    final_end_date = end_date or date.today().isoformat()
    client = _build_jquants_client(settings)
    repository = _build_repository(settings)
    total_upserted = _import_index_bulk_files(
        client=client,
        repository=repository,
        bulk_root=Path(settings.jquants_bulk_download_dir),
        start_date=start_date,
        end_date=final_end_date,
    )
    logger.info("index_daily_bar_backfill_finished", start_date=start_date, end_date=final_end_date, total_upserted=total_upserted)
    return total_upserted


async def backfill_fins_dividend(
    settings: Settings,
    *,
    start_date: str = "2013-02-20",
    end_date: str | None = None,
) -> int:
    """Backfill /v2/fins/dividend for all securities day by day."""

    final_end_date = end_date or date.today().isoformat()
    client = _build_jquants_client(settings)
    repository = _build_repository(settings)
    total_upserted = 0

    current = date.fromisoformat(start_date)
    last = date.fromisoformat(final_end_date)
    while current <= last:
        dataframe = _normalize_fin_dividend_frame(client.fetch_fins_dividend(date=current.isoformat()))
        _ensure_fin_dividend_schema(repository, dataframe)
        upserted = repository.upsert_table("market_data.fin_dividend", dataframe, key_columns=["RefNo"])
        total_upserted += upserted
        logger.info(
            "fins_dividend_backfill_progress",
            publication_date=current.isoformat(),
            upserted=upserted,
            total_upserted=total_upserted,
        )
        current += timedelta(days=1)

    logger.info(
        "fins_dividend_backfill_finished",
        start_date=start_date,
        end_date=final_end_date,
        total_upserted=total_upserted,
    )
    return total_upserted


async def build_price_action_feature_table(settings: Settings) -> int:
    """Build analytics.price_action_features from daily stock and TOPIX bars."""

    repository = _build_repository(settings)
    logger.info("price_action_feature_build_started")
    row_count = build_price_action_features(repository)
    logger.info("price_action_feature_build_finished", row_count=row_count)
    return row_count


async def build_market_industry_linkage_feature_table(settings: Settings) -> int:
    """Build analytics.market_industry_linkage_features from price action features."""

    repository = _build_repository(settings)
    logger.info("market_industry_linkage_feature_build_started")
    row_count = build_market_industry_linkage_features(repository)
    logger.info("market_industry_linkage_feature_build_finished", row_count=row_count)
    return row_count


async def build_flow_structure_feature_table(settings: Settings) -> int:
    """Build analytics.flow_structure_features from breakdown and short-ratio data."""

    repository = _build_repository(settings)
    logger.info("flow_structure_feature_build_started")
    row_count = build_flow_structure_features(repository)
    logger.info("flow_structure_feature_build_finished", row_count=row_count)
    return row_count


async def build_fundamental_event_feature_table(settings: Settings) -> int:
    """Build analytics.fundamental_event_features from financial and event data."""

    repository = _build_repository(settings)
    logger.info("fundamental_event_feature_build_started")
    row_count = build_fundamental_event_features(repository)
    logger.info("fundamental_event_feature_build_finished", row_count=row_count)
    return row_count


async def build_next_day_label_table(settings: Settings) -> int:
    """Build analytics.next_day_labels from price action features."""

    repository = _build_repository(settings)
    logger.info("next_day_label_build_started")
    row_count = build_next_day_labels(repository)
    logger.info("next_day_label_build_finished", row_count=row_count)
    return row_count


async def import_llm_prediction(
    settings: Settings,
    *,
    input_path: str,
    code: str,
    provider: str,
    model: str,
    prompt_version: str = "v1",
) -> int:
    """Import one structured LLM prediction JSON into analytics.llm_predictions."""

    repository = _build_repository(settings)
    path = Path(input_path).expanduser()
    payload = json.loads(path.read_text(encoding="utf-8"))

    trade_date = payload.get("latest_date") or payload.get("trade_date")
    if not trade_date:
        raise ValueError("Prediction JSON must include latest_date or trade_date")

    dataframe = _llm_prediction_dataframe_from_payload(
        payload=payload,
        code=code,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
    )

    logger.info(
        "llm_prediction_import_started",
        input_path=str(path),
        code=code,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        trade_date=trade_date,
    )
    _ensure_llm_predictions_schema(repository, dataframe)
    upserted_count = repository.upsert_table(
        "analytics.llm_predictions",
        dataframe,
        key_columns=["trade_date", "code", "provider", "model", "prompt_version"],
    )
    logger.info("llm_prediction_import_finished", row_count=upserted_count)
    return upserted_count


def _llm_prediction_dataframe_from_payload(
    *,
    payload: dict,
    code: str,
    provider: str,
    model: str,
    prompt_version: str,
) -> pd.DataFrame:
    trade_date = payload.get("latest_date") or payload.get("trade_date")
    if not trade_date:
        raise ValueError("Prediction JSON must include latest_date or trade_date")
    dataframe = pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "code": code,
                "provider": provider,
                "model": model,
                "prompt_version": prompt_version,
                "pred_direction": payload.get("direction"),
                "pred_confidence": payload.get("confidence"),
                "pred_pattern_judgement": payload.get("pattern_judgement"),
                "pred_summary": payload.get("summary"),
                "pred_drivers_json": json.dumps(payload.get("drivers", []), ensure_ascii=False),
                "pred_risks_json": json.dumps(payload.get("risks", []), ensure_ascii=False),
                "pred_latest_day_analysis": payload.get("latest_day_analysis"),
                "pred_market_relative_analysis": payload.get("market_relative_analysis"),
                "pred_industry_relative_analysis": payload.get("industry_relative_analysis"),
                "raw_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                "imported_at": pd.Timestamp.now(tz="UTC"),
            }
        ]
    )
    return _normalize_llm_prediction_frame(dataframe)


async def build_llm_prediction_eval_table(settings: Settings) -> int:
    """Build analytics.llm_prediction_eval by joining predictions with next-day labels."""

    repository = _build_repository(settings)
    logger.info("llm_prediction_eval_build_started")
    _ensure_llm_predictions_schema(repository, _empty_llm_prediction_frame())
    repository.execute(
        """
        create schema if not exists analytics;

        create table if not exists analytics.llm_predictions (
            trade_date date,
            code varchar,
            provider varchar,
            model varchar,
            prompt_version varchar,
            pred_direction varchar,
            pred_confidence double,
            pred_pattern_judgement varchar,
            pred_summary varchar,
            pred_drivers_json varchar,
            pred_risks_json varchar,
            pred_latest_day_analysis varchar,
            pred_market_relative_analysis varchar,
            pred_industry_relative_analysis varchar,
            raw_json varchar,
            imported_at timestamp with time zone
        );

        create or replace table analytics.llm_prediction_eval as
        select
            p.trade_date,
            p.code,
            p.provider,
            p.model,
            p.prompt_version,
            p.pred_direction,
            p.pred_confidence,
            p.pred_pattern_judgement,
            p.pred_summary,
            p.pred_drivers_json,
            p.pred_risks_json,
            p.raw_json,
            p.imported_at,
            l.next_trade_date,
            l.label_next_ret_1d as actual_next_ret_1d,
            l.label_next_up_1d as actual_next_up_1d,
            l.label_next_excess_ret_1d as actual_next_excess_ret_1d,
            l.label_next_industry_excess_ret_1d as actual_next_industry_excess_ret_1d,
            l.label_next_direction_1d as actual_next_direction_1d,
            case
                when p.pred_direction is null or l.label_next_direction_1d is null then null
                when p.pred_direction = l.label_next_direction_1d then 1
                else 0
            end as is_direction_correct,
            case
                when p.pred_direction is null or l.label_next_up_1d is null then null
                when p.pred_direction = 'bullish' and l.label_next_up_1d = 1 then 1
                when p.pred_direction = 'bearish' and l.label_next_up_1d = 0 then 1
                when p.pred_direction = 'neutral' then null
                else 0
            end as is_binary_direction_correct
        from analytics.llm_predictions p
        left join analytics.next_day_labels l
            on p.trade_date = l.trade_date and p.code = l.code
        order by p.trade_date, p.code, p.provider, p.model, p.prompt_version;
        """
    )
    result = repository.query("select count(*) as row_count from analytics.llm_prediction_eval")
    row_count = int(result["row_count"].iloc[0])
    logger.info("llm_prediction_eval_build_finished", row_count=row_count)
    return row_count


async def prepare_llm_eval_bundle(
    settings: Settings,
    *,
    code: str,
    target_date: str | None = None,
    window_trading_days: int = 20,
    output_dir: str | None = None,
) -> dict[str, object]:
    """Create a semi-automated LLM evaluation bundle for one stock/date."""

    requested_date = target_date or date.today().isoformat()
    safe_code = code.replace("'", "''")
    safe_requested_date = requested_date.replace("'", "''")
    bundle_root = Path(output_dir).expanduser() if output_dir else Path("var/llm_eval")
    bundle_dir = bundle_root / f"{code}_{requested_date}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "llm_eval_bundle_prepare_started",
        code=code,
        target_date=requested_date,
        window_trading_days=window_trading_days,
        bundle_dir=str(bundle_dir),
    )

    with duckdb.connect(settings.duckdb_path, read_only=True) as connection:
        calendar_row = connection.execute(
            f"""
            select "HolDiv"
            from market_data.market_calendar
            where cast("Date" as date) = cast('{safe_requested_date}' as date)
            limit 1
            """
        ).fetchone()
        if calendar_row is not None and str(calendar_row[0]) != "1":
            raise ValueError(f"{requested_date} is not a cash-equity trading day")

        available_dates = connection.execute(
            f"""
            select trade_date
            from analytics.price_action_features
            where code = '{safe_code}' and trade_date <= cast('{safe_requested_date}' as date)
            order by trade_date desc
            limit {int(window_trading_days)}
            """
        ).fetchdf()
        if available_dates.empty:
            raise ValueError(f"No factor rows found for code={code} on or before trade_date={requested_date}")

        trade_dates = sorted(str(value)[:10] for value in available_dates["trade_date"].tolist())
        latest_date = trade_dates[-1]

        table_map = {
            "price_action_features": "analytics.price_action_features",
            "market_industry_linkage_features": "analytics.market_industry_linkage_features",
            "flow_structure_features": "analytics.flow_structure_features",
            "fundamental_event_features": "analytics.fundamental_event_features",
        }
        factor_tables: dict[str, list[dict[str, object]]] = {}
        for key, table_name in table_map.items():
            rows = connection.execute(
                f"""
                select *
                from {table_name}
                where code = '{safe_code}'
                  and trade_date in (
                    select trade_date
                    from analytics.price_action_features
                    where code = '{safe_code}' and trade_date <= cast('{safe_requested_date}' as date)
                    order by trade_date desc
                    limit {int(window_trading_days)}
                  )
                order by trade_date
                """
            ).df()
            factor_tables[key] = [
                {
                    column: value.isoformat() if hasattr(value, "isoformat") else value
                    for column, value in record.items()
                }
                for record in rows.to_dict(orient="records")
            ]

    bundle = {
        "code": code,
        "requested_date": requested_date,
        "latest_date": latest_date,
        "window_trading_days": window_trading_days,
        "trade_dates": trade_dates,
        "factor_tables": factor_tables,
    }

    bundle_path = bundle_dir / "bundle.json"
    predictions_dir = bundle_dir / "predictions"
    prompts_dir = bundle_dir / "prompts"
    predictions_dir.mkdir(exist_ok=True)
    prompts_dir.mkdir(exist_ok=True)

    bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    app_prompt_dir = Path(__file__).resolve().parent / "prompts"
    for provider_name in ("openai", "gemini", "grok"):
        source = app_prompt_dir / prompt_filename(provider_name=provider_name, prompt_version="v1")
        target = prompts_dir / source.name
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    logger.info("llm_eval_bundle_prepare_finished", code=code, latest_date=latest_date, bundle_dir=str(bundle_dir))
    return {
        "bundle_dir": str(bundle_dir),
        "bundle_path": str(bundle_path),
        "prompts_dir": str(prompts_dir),
        "predictions_dir": str(predictions_dir),
        "latest_date": latest_date,
        "window_trading_days": window_trading_days,
    }


async def finalize_llm_eval_bundle(
    settings: Settings,
    *,
    bundle_dir: str,
    provider: str,
    model: str,
    prompt_version: str = "v1",
    input_path: str | None = None,
) -> dict[str, object]:
    """Import one prediction file from a prepared bundle and rebuild evaluation output."""

    bundle_path = Path(bundle_dir).expanduser() / "bundle.json"
    if not bundle_path.exists():
        raise ValueError(f"Bundle file not found: {bundle_path}")

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    prediction_path = Path(input_path).expanduser() if input_path else Path(bundle_dir).expanduser() / "predictions" / f"{provider}.json"
    if not prediction_path.exists():
        raise ValueError(f"Prediction file not found: {prediction_path}")

    upserted = await import_llm_prediction(
        settings,
        input_path=str(prediction_path),
        code=str(bundle["code"]),
        provider=provider,
        model=model,
        prompt_version=prompt_version,
    )
    row_count = await build_llm_prediction_eval_table(settings)

    repository = _build_repository(settings)
    safe_code = str(bundle["code"]).replace("'", "''")
    safe_provider = provider.replace("'", "''")
    safe_model = model.replace("'", "''")
    safe_prompt_version = prompt_version.replace("'", "''")
    safe_trade_date = str(bundle["latest_date"]).replace("'", "''")
    evaluation = repository.query(
        f"""
        select *
        from analytics.llm_prediction_eval
        where code = '{safe_code}'
          and trade_date = cast('{safe_trade_date}' as date)
          and provider = '{safe_provider}'
          and model = '{safe_model}'
          and prompt_version = '{safe_prompt_version}'
        limit 1
        """
    )
    evaluation_row = (
        {
            column: value.isoformat() if hasattr(value, "isoformat") else value
            for column, value in evaluation.iloc[0].to_dict().items()
        }
        if not evaluation.empty
        else None
    )

    logger.info(
        "llm_eval_bundle_finalize_finished",
        bundle_dir=bundle_dir,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        upserted=upserted,
        eval_row_count=row_count,
    )
    return {
        "upserted": upserted,
        "eval_row_count": row_count,
        "evaluation": evaluation_row,
    }


async def summarize_llm_prediction_eval(
    settings: Settings,
    *,
    provider: str | None = None,
    model: str | None = None,
    prompt_version: str | None = None,
    code: str | None = None,
) -> dict[str, object]:
    """Summarize LLM prediction evaluation metrics by provider/model/prompt version."""

    repository = _build_repository(settings)
    def _sql_literal(value: str) -> str:
        return value.replace("'", "''")

    filters: list[str] = []
    if provider:
        filters.append(f"provider = '{_sql_literal(provider)}'")
    if model:
        filters.append(f"model = '{_sql_literal(model)}'")
    if prompt_version:
        filters.append(f"prompt_version = '{_sql_literal(prompt_version)}'")
    if code:
        filters.append(f"code = '{_sql_literal(code)}'")

    where_clause = f"where {' and '.join(filters)}" if filters else ""

    logger.info(
        "llm_prediction_eval_summary_started",
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        code=code,
    )

    summary = repository.query(
        f"""
        select
            provider,
            model,
            prompt_version,
            count(*) as sample_count,
            avg(cast(is_direction_correct as double)) as direction_accuracy,
            avg(cast(is_binary_direction_correct as double)) as binary_direction_accuracy,
            avg(actual_next_ret_1d) as avg_next_ret_1d,
            avg(actual_next_excess_ret_1d) as avg_next_excess_ret_1d,
            avg(actual_next_industry_excess_ret_1d) as avg_next_industry_excess_ret_1d,
            avg(pred_confidence) as avg_confidence
        from analytics.llm_prediction_eval
        {where_clause}
        group by provider, model, prompt_version
        order by provider, model, prompt_version
        """
    )

    latest_samples = repository.query(
        f"""
        select
            trade_date,
            code,
            provider,
            model,
            prompt_version,
            pred_direction,
            pred_confidence,
            actual_next_direction_1d,
            is_direction_correct,
            is_binary_direction_correct,
            actual_next_ret_1d
        from analytics.llm_prediction_eval
        {where_clause}
        order by trade_date desc, code, provider, model
        limit 20
        """
    )

    result = {
        "filters": {
            "provider": provider,
            "model": model,
            "prompt_version": prompt_version,
            "code": code,
        },
        "summary": summary.to_dict(orient="records"),
        "latest_samples": latest_samples.to_dict(orient="records"),
    }

    logger.info(
        "llm_prediction_eval_summary_finished",
        group_count=len(result["summary"]),
        sample_rows=len(result["latest_samples"]),
    )
    return result


async def run_llm_eval_auto(
    settings: Settings,
    *,
    code: str,
    provider: str,
    model: str,
    target_date: str | None = None,
    prompt_version: str = "v1",
    window_trading_days: int = 20,
    output_dir: str | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, object]:
    """Prepare bundle, call provider SDK, persist prediction, and evaluate in one run."""

    prepared = await prepare_llm_eval_bundle(
        settings,
        code=code,
        target_date=target_date,
        window_trading_days=window_trading_days,
        output_dir=output_dir,
    )
    bundle_dir = str(prepared["bundle_dir"])
    bundle = load_bundle_inputs(bundle_dir)
    prompt = load_provider_prompt(
        bundle_dir=bundle_dir,
        provider_name=provider,
        prompt_version=prompt_version,
    )
    llm_provider = get_llm_provider(provider_name=provider, settings=settings)
    prediction = llm_provider.predict_from_bundle(
        bundle=bundle,
        prompt=prompt,
        model=model,
        reasoning_effort=reasoning_effort,
    )

    prediction_path = Path(bundle_dir) / "predictions" / f"{provider}__{slugify_model_name(model)}__{prompt_version}.json"
    prediction_path.write_text(json.dumps(prediction, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    finalized = await finalize_llm_eval_bundle(
        settings,
        bundle_dir=bundle_dir,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        input_path=str(prediction_path),
    )
    return {
        "bundle_dir": bundle_dir,
        "prediction_path": str(prediction_path),
        **finalized,
    }


async def run_llm_eval_auto_range(
    settings: Settings,
    *,
    code: str,
    provider: str,
    model: str,
    start_date: str,
    end_date: str,
    prompt_version: str = "v1",
    window_trading_days: int = 20,
    output_dir: str | None = None,
    overwrite_existing: bool = False,
    reasoning_effort: str | None = None,
) -> dict[str, object]:
    """Run full automatic LLM evaluation over a historical trading-date range."""

    repository = _build_repository(settings)
    _ensure_llm_predictions_schema(repository, _empty_llm_prediction_frame())
    safe_code = code.replace("'", "''")
    safe_provider = provider.replace("'", "''")
    safe_model = model.replace("'", "''")
    safe_prompt_version = prompt_version.replace("'", "''")
    safe_start_date = start_date.replace("'", "''")
    safe_end_date = end_date.replace("'", "''")

    trading_dates_df = repository.query(
        f"""
        select trade_date
        from analytics.price_action_features
        where code = '{safe_code}'
          and trade_date between cast('{safe_start_date}' as date) and cast('{safe_end_date}' as date)
        order by trade_date
        """
    )
    trading_dates = [str(value)[:10] for value in trading_dates_df["trade_date"].tolist()]

    if not trading_dates:
        raise ValueError(f"No trade dates found for code={code} between {start_date} and {end_date}")

    existing_dates: set[str] = set()
    if not overwrite_existing:
        existing_df = repository.query(
            f"""
            select cast(trade_date as varchar) as trade_date
            from analytics.llm_predictions
            where code = '{safe_code}'
              and provider = '{safe_provider}'
              and model = '{safe_model}'
              and prompt_version = '{safe_prompt_version}'
            """
        )
        existing_dates = {str(value)[:10] for value in existing_df["trade_date"].tolist()}

    logger.info(
        "llm_eval_auto_range_started",
        code=code,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        start_date=start_date,
        end_date=end_date,
        trading_day_count=len(trading_dates),
        overwrite_existing=overwrite_existing,
    )

    processed_dates: list[str] = []
    skipped_dates: list[str] = []
    failed_dates: list[dict[str, str]] = []

    for trade_date in trading_dates:
        if not overwrite_existing and trade_date in existing_dates:
            skipped_dates.append(trade_date)
            continue
        try:
            await run_llm_eval_auto(
                settings,
                code=code,
                provider=provider,
                model=model,
                target_date=trade_date,
                prompt_version=prompt_version,
                window_trading_days=window_trading_days,
                output_dir=output_dir,
                reasoning_effort=reasoning_effort,
            )
            processed_dates.append(trade_date)
        except Exception as exc:  # pragma: no cover - kept for robust batch execution
            logger.exception(
                "llm_eval_auto_range_item_failed",
                code=code,
                provider=provider,
                model=model,
                trade_date=trade_date,
            )
            failed_dates.append({"trade_date": trade_date, "error": str(exc)})

    summary = await summarize_llm_prediction_eval(
        settings,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        code=code,
    )
    result = {
        "code": code,
        "provider": provider,
        "model": model,
        "prompt_version": prompt_version,
        "start_date": start_date,
        "end_date": end_date,
        "total_trade_dates": len(trading_dates),
        "processed_count": len(processed_dates),
        "skipped_count": len(skipped_dates),
        "failed_count": len(failed_dates),
        "processed_dates": processed_dates,
        "skipped_dates": skipped_dates,
        "failed_dates": failed_dates,
        "summary": summary["summary"],
    }
    logger.info(
        "llm_eval_auto_range_finished",
        code=code,
        provider=provider,
        model=model,
        processed_count=len(processed_dates),
        skipped_count=len(skipped_dates),
        failed_count=len(failed_dates),
    )
    return result


async def prepare_openai_batch_range(
    settings: Settings,
    *,
    code: str,
    model: str,
    start_date: str,
    end_date: str,
    prompt_version: str = "v1",
    window_trading_days: int = 20,
    output_dir: str | None = None,
    overwrite_existing: bool = False,
    reasoning_effort: str | None = None,
) -> dict[str, object]:
    """Prepare OpenAI Batch JSONL input for a historical trading-date range."""

    repository = _build_repository(settings)
    _ensure_llm_predictions_schema(repository, _empty_llm_prediction_frame())
    safe_code = code.replace("'", "''")
    safe_model = model.replace("'", "''")
    safe_prompt_version = prompt_version.replace("'", "''")
    safe_start_date = start_date.replace("'", "''")
    safe_end_date = end_date.replace("'", "''")

    trading_dates_df = repository.query(
        f"""
        select trade_date
        from analytics.price_action_features
        where code = '{safe_code}'
          and trade_date between cast('{safe_start_date}' as date) and cast('{safe_end_date}' as date)
        order by trade_date
        """
    )
    trading_dates = [str(value)[:10] for value in trading_dates_df["trade_date"].tolist()]
    if not trading_dates:
        raise ValueError(f"No trade dates found for code={code} between {start_date} and {end_date}")

    existing_dates: set[str] = set()
    if not overwrite_existing:
        existing_df = repository.query(
            f"""
            select cast(trade_date as varchar) as trade_date
            from analytics.llm_predictions
            where code = '{safe_code}'
              and provider = 'chatgpt'
              and model = '{safe_model}'
              and prompt_version = '{safe_prompt_version}'
            """
        )
        existing_dates = {str(value)[:10] for value in existing_df["trade_date"].tolist()}

    batch_root = Path(output_dir).expanduser() if output_dir else Path("var/openai_batch")
    batch_dir = batch_root / f"{code}_{start_date}_{end_date}_{slugify_model_name(model)}_{prompt_version}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "openai_batch_prepare_started",
        code=code,
        model=model,
        prompt_version=prompt_version,
        start_date=start_date,
        end_date=end_date,
        trading_day_count=len(trading_dates),
        overwrite_existing=overwrite_existing,
        batch_dir=str(batch_dir),
    )

    batch_runner = get_openai_batch_runner(settings=settings)
    request_lines: list[dict[str, object]] = []
    manifest_items: list[dict[str, object]] = []
    skipped_dates: list[str] = []

    for trade_date in trading_dates:
        if not overwrite_existing and trade_date in existing_dates:
            skipped_dates.append(trade_date)
            continue

        prepared = await prepare_llm_eval_bundle(
            settings,
            code=code,
            target_date=trade_date,
            window_trading_days=window_trading_days,
            output_dir=str(batch_dir / "bundles"),
        )
        bundle_dir = str(prepared["bundle_dir"])
        bundle = load_bundle_inputs(bundle_dir)
        prompt = load_provider_prompt(
            bundle_dir=bundle_dir,
            provider_name="chatgpt",
            prompt_version=prompt_version,
        )
        custom_id = f"{code}__{trade_date}__chatgpt__{slugify_model_name(model)}__{prompt_version}"
        request_lines.append(
            batch_runner.build_request_line(
                custom_id=custom_id,
                bundle=bundle,
                prompt=prompt,
                model=model,
                reasoning_effort=reasoning_effort,
            )
        )
        manifest_items.append(
            {
                "custom_id": custom_id,
                "code": code,
                "trade_date": trade_date,
                "provider": "chatgpt",
                "model": model,
                "prompt_version": prompt_version,
                "reasoning_effort": reasoning_effort,
                "bundle_dir": bundle_dir,
            }
        )

    jsonl_path = batch_dir / "requests.jsonl"
    manifest_path = batch_dir / "manifest.json"
    jsonl_path.write_text(
        "\n".join(json.dumps(line, ensure_ascii=False) for line in request_lines) + ("\n" if request_lines else ""),
        encoding="utf-8",
    )
    manifest = {
        "code": code,
        "provider": "chatgpt",
        "model": model,
        "prompt_version": prompt_version,
        "start_date": start_date,
        "end_date": end_date,
        "window_trading_days": window_trading_days,
        "request_count": len(request_lines),
        "skipped_dates": skipped_dates,
        "reasoning_effort": reasoning_effort,
        "items": manifest_items,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    logger.info(
        "openai_batch_prepare_finished",
        code=code,
        request_count=len(request_lines),
        skipped_count=len(skipped_dates),
        batch_dir=str(batch_dir),
    )
    return {
        "batch_dir": str(batch_dir),
        "jsonl_path": str(jsonl_path),
        "manifest_path": str(manifest_path),
        "request_count": len(request_lines),
        "skipped_dates": skipped_dates,
    }


async def submit_openai_batch(
    settings: Settings,
    *,
    batch_dir: str,
    completion_window: str = "24h",
) -> dict[str, object]:
    """Submit a prepared OpenAI Batch request file."""

    root = Path(batch_dir).expanduser()
    jsonl_path = root / "requests.jsonl"
    if not jsonl_path.exists():
        raise ValueError(f"Batch request file not found: {jsonl_path}")

    batch_runner = get_openai_batch_runner(settings=settings)
    result = batch_runner.submit_batch(
        input_file_path=str(jsonl_path),
        completion_window=completion_window,
        metadata={"batch_dir": str(root)},
    )

    submission_path = root / "submission.json"
    submission_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    logger.info(
        "openai_batch_submit_finished",
        batch_dir=str(root),
        batch_id=result["batch_id"],
        status=result["status"],
    )
    return {
        "batch_dir": str(root),
        "submission_path": str(submission_path),
        **result,
    }


async def finalize_openai_batch(
    settings: Settings,
    *,
    batch_dir: str,
    batch_id: str | None = None,
) -> dict[str, object]:
    """Finalize an OpenAI Batch run if completed; otherwise return pending status."""

    root = Path(batch_dir).expanduser()
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"Batch manifest file not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    submission_path = root / "submission.json"
    if batch_id is None:
        if not submission_path.exists():
            raise ValueError("batch_id is required when submission.json is missing")
        submission = json.loads(submission_path.read_text(encoding="utf-8"))
        batch_id = submission.get("batch_id")
    if not batch_id:
        raise ValueError("Could not determine batch_id")

    batch_runner = get_openai_batch_runner(settings=settings)
    status = batch_runner.retrieve_batch(batch_id=batch_id)

    if status["status"] != "completed":
        logger.info(
            "openai_batch_finalize_pending",
            batch_dir=str(root),
            batch_id=batch_id,
            openai_status=status["status"],
        )
        return {
            "batch_dir": str(root),
            "batch_id": batch_id,
            "status": "pending",
            "openai_status": status["status"],
            "imported_count": 0,
            "evaluated": False,
        }

    output_file_id = status.get("output_file_id")
    if not output_file_id:
        raise ValueError(f"Batch {batch_id} completed without output_file_id")

    raw_output = batch_runner.download_file_text(file_id=output_file_id)
    output_path = root / "output.jsonl"
    output_path.write_text(raw_output, encoding="utf-8")

    manifest_by_custom_id = {item["custom_id"]: item for item in manifest.get("items", [])}
    repository = _build_repository(settings)
    imported_count = 0

    for line in raw_output.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        custom_id = row.get("custom_id")
        item = manifest_by_custom_id.get(custom_id)
        if item is None:
            continue
        response_body = (row.get("response") or {}).get("body") or {}
        message_content = (((response_body.get("choices") or [{}])[0].get("message") or {}).get("content"))
        if not message_content:
            continue
        if isinstance(message_content, list):
            text_parts = [part.get("text", "") for part in message_content if isinstance(part, dict)]
            prediction_payload = json.loads("".join(text_parts))
        else:
            prediction_payload = json.loads(message_content)

        dataframe = _llm_prediction_dataframe_from_payload(
            payload=prediction_payload,
            code=str(item["code"]),
            provider=str(item["provider"]),
            model=str(item["model"]),
            prompt_version=str(item["prompt_version"]),
        )
        _ensure_llm_predictions_schema(repository, dataframe)
        imported_count += repository.upsert_table(
            "analytics.llm_predictions",
            dataframe,
            key_columns=["trade_date", "code", "provider", "model", "prompt_version"],
        )

        bundle_dir = Path(str(item["bundle_dir"]))
        prediction_path = bundle_dir / "predictions" / f"chatgpt__{slugify_model_name(str(item['model']))}__{item['prompt_version']}.json"
        prediction_path.write_text(
            json.dumps(prediction_payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    eval_row_count = await build_llm_prediction_eval_table(settings)
    result = {
        "batch_dir": str(root),
        "batch_id": batch_id,
        "status": "completed",
        "openai_status": status["status"],
        "imported_count": imported_count,
        "evaluated": True,
        "eval_row_count": eval_row_count,
        "output_file_id": output_file_id,
        "output_path": str(output_path),
    }
    logger.info(
        "openai_batch_finalize_finished",
        batch_dir=str(root),
        batch_id=batch_id,
        imported_count=imported_count,
        eval_row_count=eval_row_count,
    )
    return result


async def export_stock_factor_snapshot(
    settings: Settings,
    *,
    code: str,
    target_date: str | None = None,
) -> dict[str, object]:
    """Export all factor tables for one stock on one date as a JSON-ready payload."""

    requested_date = target_date or date.today().isoformat()
    safe_code = code.replace("'", "''")
    safe_requested_date = requested_date.replace("'", "''")
    logger.info("stock_factor_snapshot_export_started", code=code, target_date=requested_date)

    factor_tables = {
        "price_action_features": "analytics.price_action_features",
        "market_industry_linkage_features": "analytics.market_industry_linkage_features",
        "flow_structure_features": "analytics.flow_structure_features",
        "fundamental_event_features": "analytics.fundamental_event_features",
    }

    with duckdb.connect(settings.duckdb_path, read_only=True) as connection:
        calendar_row = connection.execute(
            f"""
            select "HolDiv"
            from market_data.market_calendar
            where cast("Date" as date) = cast('{safe_requested_date}' as date)
            limit 1
            """
        ).fetchone()

        if calendar_row is not None and str(calendar_row[0]) != "1":
            payload: dict[str, object] = {
                "code": code,
                "requested_date": requested_date,
                "trade_date": requested_date,
                "status": "not_trading_day",
                "message": f"{requested_date} is not a cash-equity trading day",
                "factor_tables": {},
            }
            logger.info("stock_factor_snapshot_export_finished", code=code, target_date=requested_date, status="not_trading_day")
            return payload

        payload = {
            "code": code,
            "requested_date": requested_date,
            "trade_date": requested_date,
            "status": "ok",
            "factor_tables": {},
        }

        found_any = False
        for key, table_name in factor_tables.items():
            rows = connection.execute(
                f"""
                select *
                from {table_name}
                where code = '{safe_code}' and trade_date = cast('{safe_requested_date}' as date)
                limit 1
                """
            ).df()
            if rows.empty:
                payload["factor_tables"][key] = None
                continue

            found_any = True
            record = rows.iloc[0].to_dict()
            payload["factor_tables"][key] = {
                column: value.isoformat() if hasattr(value, "isoformat") else value
                for column, value in record.items()
            }

    if not found_any:
        payload["status"] = "no_data"
        payload["message"] = f"No factor rows found for code={code} on trade_date={requested_date}"

    logger.info(
        "stock_factor_snapshot_export_finished",
        code=code,
        target_date=requested_date,
        status=payload["status"],
    )
    return payload


async def run_daily_workflow(settings: Settings, *, target_date: str | None = None) -> dict[str, int]:
    """Run the standard end-of-day sync workflow for a single trading date."""

    final_date = target_date or date.today().isoformat()
    logger.info("daily_workflow_started", target_date=final_date)

    results = {
        "equity_master": await sync_equity_master(settings, date=final_date),
        "equity_daily_bar": await sync_equity_daily_bars(settings, date=final_date),
        "topix_daily_bar": await sync_topix_daily_bars(settings, date=final_date),
        "index_daily_bar": await sync_index_daily_bars(settings, date=final_date),
        "fin_summary": await sync_fins_summary(settings, date=final_date),
        "fin_dividend": await sync_fins_dividend(settings, date=final_date),
        "margin_interest": await sync_margin_interest(settings, date=final_date),
        "market_breakdown": await sync_market_breakdown(settings, date=final_date),
        "short_ratio": await sync_short_ratio(settings, date=final_date),
        "margin_alert": await sync_margin_alert(settings, date=final_date),
        "short_sale_report": await sync_short_sale_report(settings, calculated_date=final_date),
        "earnings_calendar": await sync_earnings_calendar(settings),
        "market_calendar": await sync_market_calendar(settings, from_date=final_date, to_date=final_date),
        "price_action_features": await build_price_action_feature_table(settings),
        "market_industry_linkage_features": await build_market_industry_linkage_feature_table(settings),
        "flow_structure_features": await build_flow_structure_feature_table(settings),
        "fundamental_event_features": await build_fundamental_event_feature_table(settings),
        "next_day_labels": await build_next_day_label_table(settings),
    }

    logger.info("daily_workflow_finished", target_date=final_date, **results)
    return results


def _normalize_item(source: str, item: dict) -> MetricRecord:
    return MetricRecord.model_validate(
        {
            "source": source,
            "category": item.get("category", "unknown"),
            "value": item.get("value", 0),
        }
    )


def _build_jquants_client(settings: Settings) -> JQuantsClient:
    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required for J-Quants sync jobs")
    return JQuantsClient(
        api_key=settings.jquants_api_key,
        rate_limit_per_minute=settings.jquants_rate_limit_per_minute,
    )


def _build_repository(settings: Settings) -> DuckDBRepository:
    return DuckDBRepository(settings.duckdb_path)


async def _fetch_trading_dates(*, client: JQuantsClient, start_date: str, end_date: str) -> list[date]:
    dataframe = client.fetch_market_calendar(from_date=start_date, to_date=end_date)
    if dataframe.empty:
        return []
    trading_rows = dataframe.loc[dataframe["HolDiv"] != "0", "Date"]
    return [pd.Timestamp(value).date() for value in trading_rows.to_list()]


def _import_index_bulk_files(
    *,
    client: JQuantsClient,
    repository: DuckDBRepository,
    bulk_root: Path,
    start_date: str,
    end_date: str,
) -> int:
    file_list = client.fetch_bulk_file_list(endpoint="/indices/bars/daily")
    selected_keys = _select_bulk_keys(file_list=file_list, start_date=start_date, end_date=end_date)

    total_upserted = 0
    for key in selected_keys:
        destination = bulk_root / key
        client.download_bulk_file(key=key, output_path=str(destination))
        dataframe = _load_bulk_csv_with_metadata(destination, source_api="/v2/bulk/get")
        if dataframe.empty:
            logger.info("index_daily_bar_bulk_file_skipped", key=key, reason="empty")
            continue
        upserted = repository.upsert_table("market_data.index_daily_bar", dataframe, key_columns=["Date", "Code"])
        total_upserted += upserted
        logger.info("index_daily_bar_bulk_file_imported", key=key, rows=upserted, total_upserted=total_upserted)

    return total_upserted


def _select_bulk_keys(*, file_list: pd.DataFrame, start_date: str, end_date: str) -> list[str]:
    if file_list.empty:
        return []

    start_month = pd.Timestamp(start_date).strftime("%Y%m")
    end_month = pd.Timestamp(end_date).strftime("%Y%m")
    months = {value.strftime("%Y%m") for value in pd.period_range(start_month, end_month, freq="M")}

    keys: list[str] = []
    for key in file_list["Key"].astype(str).tolist():
        month_match = re.search(r"(\d{6})(?!\d)", Path(key).stem)
        day_match = re.search(r"(\d{8})(?!\d)", Path(key).stem)
        if day_match and pd.Timestamp(day_match.group(1)).strftime("%Y%m") in months:
            keys.append(key)
            continue
        if month_match and month_match.group(1) in months:
            keys.append(key)

    return sorted(set(keys))


def _load_bulk_csv_with_metadata(path: Path, *, source_api: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    with gzip.open(path, mode="rt", encoding="utf-8") as handle:
        dataframe = pd.read_csv(handle, dtype={"Code": "string"})

    if dataframe.empty:
        return dataframe

    dataframe["source_api"] = source_api
    dataframe["fetched_at"] = pd.Timestamp.now(tz="UTC")
    return dataframe.sort_values([column for column in ["Date", "Code"] if column in dataframe.columns]).reset_index(
        drop=True
    )


def _normalize_fin_dividend_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    normalized = dataframe.copy()
    for column in FIN_DIVIDEND_MIXED_TYPE_COLUMNS:
        if column not in normalized.columns:
            continue
        series = normalized[column]
        normalized[column] = series.where(series.notna(), "").astype("string")

    return normalized


def _normalize_margin_alert_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    normalized = dataframe.copy()
    for column in MARGIN_ALERT_MIXED_TYPE_COLUMNS:
        if column not in normalized.columns:
            continue
        series = normalized[column]
        normalized[column] = series.where(series.notna(), "").astype("string")

    return normalized


def _empty_llm_prediction_frame() -> pd.DataFrame:
    return _normalize_llm_prediction_frame(
        pd.DataFrame(
            [
                {
                    "trade_date": "1970-01-01",
                    "code": "",
                    "provider": "",
                    "model": "",
                    "prompt_version": "",
                    "pred_direction": "",
                    "pred_confidence": 0.0,
                    "pred_pattern_judgement": "",
                    "pred_summary": "",
                    "pred_drivers_json": "[]",
                    "pred_risks_json": "[]",
                    "pred_latest_day_analysis": "",
                    "pred_market_relative_analysis": "",
                    "pred_industry_relative_analysis": "",
                    "raw_json": "{}",
                    "imported_at": pd.Timestamp.now(tz="UTC"),
                }
            ]
        )
    ).iloc[0:0]


def _normalize_llm_prediction_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty and len(dataframe.columns) == 0:
        return dataframe.copy()

    normalized = dataframe.copy()
    string_columns = [
        "trade_date",
        "code",
        "provider",
        "model",
        "prompt_version",
        "pred_direction",
        "pred_pattern_judgement",
        "pred_summary",
        "pred_drivers_json",
        "pred_risks_json",
        "pred_latest_day_analysis",
        "pred_market_relative_analysis",
        "pred_industry_relative_analysis",
        "raw_json",
    ]
    for column in string_columns:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype("string")
    if "pred_confidence" in normalized.columns:
        normalized["pred_confidence"] = pd.to_numeric(normalized["pred_confidence"], errors="coerce").astype("Float64")
    if "imported_at" in normalized.columns:
        normalized["imported_at"] = pd.to_datetime(normalized["imported_at"], errors="coerce", utc=True)
    return normalized


def _ensure_fin_dividend_schema(repository: DuckDBRepository, dataframe: pd.DataFrame) -> None:
    schema_info = repository.query(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'market_data' and table_name = 'fin_dividend'
        """
    )
    if schema_info.empty:
        return

    type_by_column = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_info.to_dict(orient="records")
    }
    schema_columns = list(type_by_column.keys())
    dataframe_columns = list(dataframe.columns)
    mixed_type_ok = all(
        type_by_column.get(column) == "VARCHAR"
        for column in FIN_DIVIDEND_MIXED_TYPE_COLUMNS
        if column in type_by_column
    )
    columns_match = schema_columns == dataframe_columns
    if mixed_type_ok and columns_match:
        return

    existing = repository.query("select * from market_data.fin_dividend")
    normalized = _normalize_fin_dividend_frame(existing)
    for column in dataframe_columns:
        if column not in normalized.columns:
            normalized[column] = pd.Series([""] * len(normalized), dtype="string")
    normalized = normalized[dataframe_columns]
    repository.replace_table("market_data.fin_dividend", normalized)


def _ensure_margin_alert_schema(repository: DuckDBRepository, dataframe: pd.DataFrame) -> None:
    schema_info = repository.query(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'market_data' and table_name = 'margin_alert'
        """
    )
    if schema_info.empty:
        return

    type_by_column = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_info.to_dict(orient="records")
    }
    schema_columns = list(type_by_column.keys())
    dataframe_columns = list(dataframe.columns)
    target_type_by_column = {
        column: _duckdb_type_for_series(dataframe[column])
        for column in dataframe_columns
    }
    mixed_type_ok = all(
        type_by_column.get(column) == "VARCHAR"
        for column in MARGIN_ALERT_MIXED_TYPE_COLUMNS
        if column in type_by_column
    )
    columns_match = schema_columns == dataframe_columns
    types_match = all(
        type_by_column.get(column) == target_type_by_column[column]
        for column in dataframe_columns
    )
    if mixed_type_ok and columns_match and types_match:
        return

    existing = repository.query("select * from market_data.margin_alert")
    normalized_source = _normalize_margin_alert_frame(existing)
    normalized = pd.DataFrame(index=normalized_source.index)
    for column in dataframe_columns:
        if column in normalized_source.columns:
            normalized[column] = _coerce_series_to_match(normalized_source[column], dataframe[column])
        else:
            normalized[column] = _empty_series_for_dtype(dataframe[column], len(normalized_source))
    repository.replace_table("market_data.margin_alert", normalized)


def _ensure_market_breakdown_schema(repository: DuckDBRepository, dataframe: pd.DataFrame) -> None:
    schema_info = repository.query(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'market_data' and table_name = 'market_breakdown'
        order by ordinal_position
        """
    )
    if schema_info.empty:
        return

    schema_columns = schema_info["column_name"].tolist()
    dataframe_columns = list(dataframe.columns)
    target_type_by_column = {
        column: _duckdb_type_for_series(dataframe[column])
        for column in dataframe_columns
    }
    current_type_by_column = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_info.to_dict(orient="records")
    }
    columns_match = schema_columns == dataframe_columns
    types_match = all(
        current_type_by_column.get(column) == target_type_by_column[column]
        for column in dataframe_columns
    )
    if columns_match and types_match:
        return

    existing = repository.query("select * from market_data.market_breakdown")
    normalized = pd.DataFrame(index=existing.index)
    for column in dataframe_columns:
        if column in existing.columns:
            normalized[column] = _coerce_series_to_match(existing[column], dataframe[column])
        else:
            normalized[column] = _empty_series_for_dtype(dataframe[column], len(existing))
    repository.replace_table("market_data.market_breakdown", normalized)


def _ensure_short_ratio_schema(repository: DuckDBRepository, dataframe: pd.DataFrame) -> None:
    schema_info = repository.query(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'market_data' and table_name = 'short_ratio'
        order by ordinal_position
        """
    )
    if schema_info.empty:
        return

    schema_columns = schema_info["column_name"].tolist()
    dataframe_columns = list(dataframe.columns)
    target_type_by_column = {
        column: _duckdb_type_for_series(dataframe[column])
        for column in dataframe_columns
    }
    current_type_by_column = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_info.to_dict(orient="records")
    }
    columns_match = schema_columns == dataframe_columns
    types_match = all(
        current_type_by_column.get(column) == target_type_by_column[column]
        for column in dataframe_columns
    )
    if columns_match and types_match:
        return

    existing = repository.query("select * from market_data.short_ratio")
    normalized = pd.DataFrame(index=existing.index)
    for column in dataframe_columns:
        if column in existing.columns:
            normalized[column] = _coerce_series_to_match(existing[column], dataframe[column])
        else:
            normalized[column] = _empty_series_for_dtype(dataframe[column], len(existing))
    repository.replace_table("market_data.short_ratio", normalized)


def _ensure_llm_predictions_schema(repository: DuckDBRepository, dataframe: pd.DataFrame) -> None:
    schema_info = repository.query(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'analytics' and table_name = 'llm_predictions'
        order by ordinal_position
        """
    )
    normalized_dataframe = _normalize_llm_prediction_frame(dataframe)
    if schema_info.empty:
        repository.replace_table("analytics.llm_predictions", normalized_dataframe)
        return

    schema_columns = schema_info["column_name"].tolist()
    dataframe_columns = list(normalized_dataframe.columns)
    target_type_by_column = {
        column: _duckdb_type_for_series(normalized_dataframe[column])
        for column in dataframe_columns
    }
    current_type_by_column = {
        row["column_name"]: row["data_type"].upper()
        for row in schema_info.to_dict(orient="records")
    }
    columns_match = schema_columns == dataframe_columns
    types_match = all(
        current_type_by_column.get(column) == target_type_by_column[column]
        for column in dataframe_columns
    )
    if columns_match and types_match:
        return

    existing = repository.query("select * from analytics.llm_predictions")
    normalized_source = _normalize_llm_prediction_frame(existing)
    normalized = pd.DataFrame(index=normalized_source.index)
    for column in dataframe_columns:
        if column in normalized_source.columns:
            normalized[column] = _coerce_series_to_match(normalized_source[column], normalized_dataframe[column])
        else:
            normalized[column] = _empty_series_for_dtype(normalized_dataframe[column], len(normalized_source))
    repository.replace_table("analytics.llm_predictions", normalized)


def _empty_series_for_dtype(source_series: pd.Series, length: int) -> pd.Series:
    dtype = source_series.dtype
    if ptypes.is_string_dtype(dtype) or ptypes.is_object_dtype(dtype):
        return pd.Series([pd.NA] * length, dtype="string")
    if ptypes.is_datetime64_any_dtype(dtype):
        return pd.Series([pd.NaT] * length, dtype=dtype)
    if ptypes.is_float_dtype(dtype):
        return pd.Series([pd.NA] * length, dtype="Float64")
    if ptypes.is_integer_dtype(dtype):
        return pd.Series([pd.NA] * length, dtype="Int64")
    if ptypes.is_bool_dtype(dtype):
        return pd.Series([pd.NA] * length, dtype="boolean")
    return pd.Series([pd.NA] * length, dtype="string")


def _coerce_series_to_match(existing_series: pd.Series, template_series: pd.Series) -> pd.Series:
    dtype = template_series.dtype
    if ptypes.is_string_dtype(dtype) or ptypes.is_object_dtype(dtype):
        return existing_series.astype("string")
    if isinstance(dtype, DatetimeTZDtype):
        return pd.to_datetime(existing_series, errors="coerce", utc=True)
    if ptypes.is_datetime64_any_dtype(dtype):
        return pd.to_datetime(existing_series, errors="coerce")
    if ptypes.is_float_dtype(dtype):
        return pd.to_numeric(existing_series, errors="coerce").astype("Float64")
    if ptypes.is_integer_dtype(dtype):
        return pd.to_numeric(existing_series, errors="coerce").astype("Int64")
    if ptypes.is_bool_dtype(dtype):
        return existing_series.astype("boolean")
    return existing_series.astype("string")


def _duckdb_type_for_series(series: pd.Series) -> str:
    dtype = series.dtype
    if ptypes.is_string_dtype(dtype) or ptypes.is_object_dtype(dtype):
        return "VARCHAR"
    if isinstance(dtype, DatetimeTZDtype):
        return "TIMESTAMP WITH TIME ZONE"
    if ptypes.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    if ptypes.is_float_dtype(dtype):
        return "DOUBLE"
    if ptypes.is_integer_dtype(dtype):
        return "BIGINT"
    if ptypes.is_bool_dtype(dtype):
        return "BOOLEAN"
    return "VARCHAR"
