from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import duckdb

from app.clients import JQuantsClient
from app.config import Settings
from app.db import DuckDBRepository
from app.jobs import (
    sync_equity_daily_bars,
    sync_equity_master,
    sync_fins_dividend,
    sync_fins_summary,
    sync_margin_interest,
    sync_market_calendar,
    sync_topix_daily_bars,
)


WATCH_CODE = "4592"
INITIAL_START_DATE = "2021-01-01"


def _next_date(database_path: str, sql: str, fallback_start_date: str) -> str:
    try:
        with duckdb.connect(database_path) as connection:
            result = connection.execute(sql).fetchone()
    except duckdb.Error:
        return fallback_start_date

    if not result or result[0] is None:
        return fallback_start_date

    latest_value = result[0]
    if isinstance(latest_value, datetime):
        latest_date = latest_value.date()
    elif isinstance(latest_value, date):
        latest_date = latest_value
    else:
        latest_date = date.fromisoformat(str(latest_value))

    return (latest_date + timedelta(days=1)).isoformat()


async def main() -> None:
    settings = Settings()
    today = date.today().isoformat()
    repository = DuckDBRepository(settings.duckdb_path)
    client = JQuantsClient(
        api_key=settings.jquants_api_key,
        rate_limit_per_minute=settings.jquants_rate_limit_per_minute,
    )

    if not settings.jquants_api_key:
        raise ValueError("JQUANTS_API_KEY is required")

    next_equity_bar_date = _next_date(
        settings.duckdb_path,
        'select max("Date") from market_data.equity_daily_bar where "Code" = \'45920\'',
        INITIAL_START_DATE,
    )
    next_dividend_date = _next_date(
        settings.duckdb_path,
        'select max("PubDate") from market_data.fin_dividend where "Code" = \'45920\'',
        INITIAL_START_DATE,
    )
    next_margin_interest_date = _next_date(
        settings.duckdb_path,
        'select max("Date") from market_data.margin_interest where "Code" = \'45920\'',
        INITIAL_START_DATE,
    )
    next_market_calendar_date = _next_date(
        settings.duckdb_path,
        'select max("Date") from market_data.market_calendar',
        INITIAL_START_DATE,
    )
    next_topix_date = _next_date(
        settings.duckdb_path,
        'select max("Date") from market_data.topix_daily_bar',
        INITIAL_START_DATE,
    )

    print(f"Refreshing watchlist data for {WATCH_CODE} through {today}")

    upsert_counts: dict[str, int] = {}

    upsert_counts["equity_master"] = await sync_equity_master(settings, code=WATCH_CODE)

    if next_equity_bar_date <= today:
        upsert_counts["equity_daily_bar"] = await sync_equity_daily_bars(
            settings,
            code=WATCH_CODE,
            from_date=next_equity_bar_date,
            to_date=today,
        )
    else:
        upsert_counts["equity_daily_bar"] = 0

    if next_dividend_date <= today:
        upsert_counts["fin_dividend"] = await sync_fins_dividend(
            settings,
            code=WATCH_CODE,
            from_date=next_dividend_date,
            to_date=today,
        )
    else:
        upsert_counts["fin_dividend"] = 0

    if next_margin_interest_date <= today:
        upsert_counts["margin_interest"] = await sync_margin_interest(
            settings,
            code=WATCH_CODE,
            from_date=next_margin_interest_date,
            to_date=today,
        )
    else:
        upsert_counts["margin_interest"] = 0

    if next_market_calendar_date <= today:
        upsert_counts["market_calendar"] = await sync_market_calendar(
            settings,
            from_date=next_market_calendar_date,
            to_date=today,
        )
    else:
        upsert_counts["market_calendar"] = 0

    if next_topix_date <= today:
        upsert_counts["topix_daily_bar"] = await sync_topix_daily_bars(
            settings,
            from_date=next_topix_date,
            to_date=today,
        )
    else:
        upsert_counts["topix_daily_bar"] = 0

    upsert_counts["fin_summary"] = await sync_fins_summary(settings, code=WATCH_CODE)

    earnings_dataframe = client.fetch_earnings_calendar()
    filtered_earnings = earnings_dataframe.loc[
        earnings_dataframe["Code"] == f"{WATCH_CODE}0"
    ].reset_index(drop=True)
    upsert_counts["earnings_calendar"] = repository.replace_table(
        "market_data.earnings_calendar",
        filtered_earnings,
    )

    print("Upsert summary:")
    for table_name, row_count in upsert_counts.items():
        print(f"  {table_name}: {row_count}")


if __name__ == "__main__":
    asyncio.run(main())
