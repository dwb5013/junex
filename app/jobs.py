from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from app.aggregator import merge_records
from app.clients import ExternalAPIClient, JQuantsClient
from app.config import Settings
from app.db import DuckDBRepository, save_summary
from app.logging import get_logger
from app.models import MetricRecord
from app.stats import summarize_by_category


logger = get_logger(__name__)


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


async def sync_market_calendar(settings: Settings, *, from_date: str, to_date: str) -> int:
    """Fetch /v2/markets/calendar and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("market_calendar_sync_started", from_date=from_date, to_date=to_date)
    dataframe = client.fetch_market_calendar(from_date=from_date, to_date=to_date)
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
    """Fetch /v2/indices/bars/daily and upsert it into DuckDB."""

    client = _build_jquants_client(settings)
    repository = _build_repository(settings)

    logger.info("index_daily_bar_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    dataframe = client.fetch_index_daily_bars(code=code, date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.index_daily_bar", dataframe, key_columns=["Date", "Code"])
    logger.info("index_daily_bar_sync_finished", row_count=upserted_count)
    return upserted_count


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
    dataframe = client.fetch_fins_dividend(code=code, date=date, from_date=from_date, to_date=to_date)
    upserted_count = repository.upsert_table("market_data.fin_dividend", dataframe, key_columns=["RefNo"])
    logger.info("fins_dividend_sync_finished", row_count=upserted_count)
    return upserted_count


async def backfill_index_daily_bars(
    settings: Settings,
    *,
    start_date: str = "2008-05-07",
    end_date: str | None = None,
) -> int:
    """Backfill /v2/indices/bars/daily for all indices day by day."""

    final_end_date = end_date or date.today().isoformat()
    client = _build_jquants_client(settings)
    repository = _build_repository(settings)
    trading_dates = await _fetch_trading_dates(client=client, start_date=start_date, end_date=final_end_date)

    total_upserted = 0
    for trade_date in trading_dates:
        dataframe = client.fetch_index_daily_bars(date=trade_date.isoformat())
        upserted = repository.upsert_table("market_data.index_daily_bar", dataframe, key_columns=["Date", "Code"])
        total_upserted += upserted
        logger.info(
            "index_daily_bar_backfill_progress",
            trade_date=trade_date.isoformat(),
            upserted=upserted,
            total_upserted=total_upserted,
        )

    logger.info(
        "index_daily_bar_backfill_finished",
        start_date=start_date,
        end_date=final_end_date,
        total_upserted=total_upserted,
    )
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
        dataframe = client.fetch_fins_dividend(date=current.isoformat())
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
