from __future__ import annotations

from datetime import date, timedelta

from app.aggregator import merge_records
from app.clients import ExternalAPIClient, JQuantsClient
from app.config import Settings
from app.db import (
    create_db_engine,
    save_summary,
    upsert_fins_dividend,
    upsert_fins_summary,
    upsert_earnings_calendar,
    upsert_equity_daily_bars,
    upsert_equity_master_snapshots,
    upsert_index_daily_bars,
    upsert_market_calendar,
    upsert_topix_daily_bars,
)
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
    engine = create_db_engine(settings.database_url)
    save_summary(engine, summary)
    logger.info("batch_job_finished", record_count=len(merged), category_count=len(summary))
    return summary


async def sync_equity_master(settings: Settings, *, code: str | None = None, date: str | None = None) -> int:
    """Fetch /v2/equities/master from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/equities/master")

    client = _build_jquants_client(settings)

    logger.info("equity_master_sync_started", code=code, date=date)
    records = await client.fetch_equities_master(code=code, date=date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_equity_master_snapshots(engine, records)
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
    """Fetch /v2/equities/bars/daily from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/equities/bars/daily")

    client = _build_jquants_client(settings)

    logger.info("equity_daily_bar_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_equity_daily_bars(
        code=code,
        date=date,
        from_date=from_date,
        to_date=to_date,
    )
    engine = create_db_engine(settings.database_url)
    upserted_count, revised_rows = upsert_equity_daily_bars(engine, records)
    if revised_rows:
        logger.warning(
            "equity_daily_bar_source_revision_detected",
            revision_count=len(revised_rows),
            revisions=revised_rows[:20],
        )
    logger.info("equity_daily_bar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_market_calendar(settings: Settings, *, from_date: str, to_date: str) -> int:
    """Fetch /v2/markets/calendar from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/markets/calendar")

    client = _build_jquants_client(settings)

    logger.info("market_calendar_sync_started", from_date=from_date, to_date=to_date)
    records = await client.fetch_market_calendar(from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_market_calendar(engine, records)
    logger.info("market_calendar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_topix_daily_bars(
    settings: Settings,
    *,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/indices/bars/daily/topix from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/indices/bars/daily/topix")

    client = _build_jquants_client(settings)

    logger.info("topix_daily_bar_sync_started", date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_topix_daily_bars(date=date, from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_topix_daily_bars(engine, records)
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
    """Fetch /v2/indices/bars/daily from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/indices/bars/daily")

    client = _build_jquants_client(settings)

    logger.info("index_daily_bar_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_index_daily_bars(code=code, date=date, from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_index_daily_bars(engine, records)
    logger.info("index_daily_bar_sync_finished", row_count=upserted_count)
    return upserted_count


async def sync_earnings_calendar(
    settings: Settings,
    *,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Fetch /v2/equities/earnings-calendar from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/equities/earnings-calendar")

    client = _build_jquants_client(settings)

    logger.info("earnings_calendar_sync_started", date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_earnings_calendar(date=date, from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_earnings_calendar(engine, records)
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
    """Fetch /v2/fins/summary from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/fins/summary")

    client = _build_jquants_client(settings)

    logger.info("fins_summary_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_fins_summary(code=code, date=date, from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_fins_summary(engine, records)
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
    """Fetch /v2/fins/dividend from J-Quants and upsert it into PostgreSQL."""

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required to fetch /v2/fins/dividend")

    client = JQuantsClient(
        api_key=settings.jquants_api_key,
        base_url=settings.jquants_api_base_url,
        timeout=settings.jquants_timeout_seconds,
    )

    logger.info("fins_dividend_sync_started", code=code, date=date, from_date=from_date, to_date=to_date)
    records = await client.fetch_fins_dividend(code=code, date=date, from_date=from_date, to_date=to_date)
    engine = create_db_engine(settings.database_url)
    upserted_count = upsert_fins_dividend(engine, records)
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
    engine = create_db_engine(settings.database_url)
    trading_dates = await _fetch_trading_dates(settings, client=client, start_date=start_date, end_date=final_end_date)

    total_upserted = 0
    for trade_date in trading_dates:
        records = await client.fetch_index_daily_bars(date=trade_date.isoformat())
        upserted = upsert_index_daily_bars(engine, records)
        total_upserted += upserted
        logger.info("index_daily_bar_backfill_progress", trade_date=trade_date.isoformat(), upserted=upserted, total_upserted=total_upserted)

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
    engine = create_db_engine(settings.database_url)
    total_upserted = 0

    current = date.fromisoformat(start_date)
    last = date.fromisoformat(final_end_date)
    while current <= last:
        records = await client.fetch_fins_dividend(date=current.isoformat())
        upserted = upsert_fins_dividend(engine, records)
        total_upserted += upserted
        logger.info("fins_dividend_backfill_progress", publication_date=current.isoformat(), upserted=upserted, total_upserted=total_upserted)
        current += timedelta(days=1)

    logger.info("fins_dividend_backfill_finished", start_date=start_date, end_date=final_end_date, total_upserted=total_upserted)
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
    return JQuantsClient(
        api_key=settings.jquants_api_key,
        base_url=settings.jquants_api_base_url,
        timeout=settings.jquants_timeout_seconds,
    )


async def _fetch_trading_dates(settings: Settings, *, client: JQuantsClient, start_date: str, end_date: str) -> list[date]:
    records = await client.fetch_market_calendar(from_date=start_date, to_date=end_date)
    return [record.trade_date for record in records if record.holiday_division != "0"]
