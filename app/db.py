from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, Date, DateTime, Float, Index, Integer, MetaData, Numeric, String, Table, Text, create_engine, select, text, tuple_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from app.models import (
    EarningsCalendarRecord,
    EquityDailyBarRecord,
    EquityMasterRecord,
    FinsDividendRecord,
    FinsSummaryRecord,
    IndexDailyBarRecord,
    MarketCalendarRecord,
    TopixDailyBarRecord,
)


metadata = MetaData(schema="market_data")
UPSERT_BATCH_SIZE = 500

equity_master_snapshot = Table(
    "equity_master_snapshot",
    metadata,
    Column("as_of_date", Date, primary_key=True, nullable=False),
    Column("code", String(5), primary_key=True, nullable=False),
    Column("company_name", Text, nullable=False),
    Column("company_name_en", Text, nullable=True),
    Column("sector17_code", String(2), nullable=False),
    Column("sector17_name", Text, nullable=False),
    Column("sector33_code", String(4), nullable=False),
    Column("sector33_name", Text, nullable=False),
    Column("scale_category", Text, nullable=False),
    Column("market_code", String(4), nullable=False),
    Column("market_name", Text, nullable=False),
    Column("margin_code", String(1), nullable=True),
    Column("margin_name", Text, nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/equities/master'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_equity_master_snapshot_code_as_of_date", "code", "as_of_date"),
    Index("ix_equity_master_snapshot_market_code_as_of_date", "market_code", "as_of_date"),
    Index("ix_equity_master_snapshot_sector33_code_as_of_date", "sector33_code", "as_of_date"),
)

equity_daily_bar = Table(
    "equity_daily_bar",
    metadata,
    Column("trade_date", Date, primary_key=True, nullable=False),
    Column("code", String(5), primary_key=True, nullable=False),
    Column("open_price", Float, nullable=True),
    Column("high_price", Float, nullable=True),
    Column("low_price", Float, nullable=True),
    Column("close_price", Float, nullable=True),
    Column("upper_limit_flag", String(1), nullable=True),
    Column("lower_limit_flag", String(1), nullable=True),
    Column("volume", Float, nullable=True),
    Column("turnover_value", Float, nullable=True),
    Column("adjustment_factor", Float, nullable=True),
    Column("adjusted_open_price", Float, nullable=True),
    Column("adjusted_high_price", Float, nullable=True),
    Column("adjusted_low_price", Float, nullable=True),
    Column("adjusted_close_price", Float, nullable=True),
    Column("adjusted_volume", Float, nullable=True),
    Column("morning_open_price", Float, nullable=True),
    Column("morning_high_price", Float, nullable=True),
    Column("morning_low_price", Float, nullable=True),
    Column("morning_close_price", Float, nullable=True),
    Column("morning_upper_limit_flag", String(1), nullable=True),
    Column("morning_lower_limit_flag", String(1), nullable=True),
    Column("morning_volume", Float, nullable=True),
    Column("morning_turnover_value", Float, nullable=True),
    Column("morning_adjusted_open_price", Float, nullable=True),
    Column("morning_adjusted_high_price", Float, nullable=True),
    Column("morning_adjusted_low_price", Float, nullable=True),
    Column("morning_adjusted_close_price", Float, nullable=True),
    Column("morning_adjusted_volume", Float, nullable=True),
    Column("afternoon_open_price", Float, nullable=True),
    Column("afternoon_high_price", Float, nullable=True),
    Column("afternoon_low_price", Float, nullable=True),
    Column("afternoon_close_price", Float, nullable=True),
    Column("afternoon_upper_limit_flag", String(1), nullable=True),
    Column("afternoon_lower_limit_flag", String(1), nullable=True),
    Column("afternoon_volume", Float, nullable=True),
    Column("afternoon_turnover_value", Float, nullable=True),
    Column("afternoon_adjusted_open_price", Float, nullable=True),
    Column("afternoon_adjusted_high_price", Float, nullable=True),
    Column("afternoon_adjusted_low_price", Float, nullable=True),
    Column("afternoon_adjusted_close_price", Float, nullable=True),
    Column("afternoon_adjusted_volume", Float, nullable=True),
    Column("has_source_revision", Boolean, nullable=False, server_default=text("false")),
    Column("source_revision_count", Integer, nullable=False, server_default=text("0")),
    Column("last_source_revision_at", DateTime(timezone=True), nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/equities/bars/daily'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_equity_daily_bar_code_trade_date", "code", "trade_date"),
    Index("ix_equity_daily_bar_trade_date", "trade_date"),
)

market_calendar = Table(
    "market_calendar",
    metadata,
    Column("trade_date", Date, primary_key=True, nullable=False),
    Column("holiday_division", String(1), nullable=False),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/markets/calendar'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_market_calendar_holiday_division_trade_date", "holiday_division", "trade_date"),
)

topix_daily_bar = Table(
    "topix_daily_bar",
    metadata,
    Column("trade_date", Date, primary_key=True, nullable=False),
    Column("open_price", Float, nullable=True),
    Column("high_price", Float, nullable=True),
    Column("low_price", Float, nullable=True),
    Column("close_price", Float, nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/indices/bars/daily/topix'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_topix_daily_bar_trade_date", "trade_date"),
)

index_daily_bar = Table(
    "index_daily_bar",
    metadata,
    Column("trade_date", Date, primary_key=True, nullable=False),
    Column("code", String(4), primary_key=True, nullable=False),
    Column("open_price", Float, nullable=True),
    Column("high_price", Float, nullable=True),
    Column("low_price", Float, nullable=True),
    Column("close_price", Float, nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/indices/bars/daily'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_index_daily_bar_code_trade_date", "code", "trade_date"),
    Index("ix_index_daily_bar_trade_date", "trade_date"),
)

earnings_calendar = Table(
    "earnings_calendar",
    metadata,
    Column("scheduled_date", Date, primary_key=True, nullable=False),
    Column("code", String(5), primary_key=True, nullable=False),
    Column("company_name", Text, nullable=False),
    Column("fiscal_year_end", Text, nullable=False),
    Column("sector_name", Text, nullable=False),
    Column("fiscal_quarter", Text, nullable=False),
    Column("section", Text, nullable=False),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/equities/earnings-calendar'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_earnings_calendar_code_scheduled_date", "code", "scheduled_date"),
    Index("ix_earnings_calendar_scheduled_date", "scheduled_date"),
)

fins_summary = Table(
    "fins_summary",
    metadata,
    Column("disclosure_number", Text, primary_key=True, nullable=False),
    Column("disclosure_date", Date, nullable=False),
    Column("disclosure_time", Text, nullable=True),
    Column("code", String(5), nullable=False),
    Column("document_type", Text, nullable=False),
    Column("current_period_type", Text, nullable=True),
    Column("current_period_start", Date, nullable=True),
    Column("current_period_end", Date, nullable=True),
    Column("current_fiscal_year_start", Date, nullable=True),
    Column("current_fiscal_year_end", Date, nullable=True),
    Column("next_fiscal_year_start", Date, nullable=True),
    Column("next_fiscal_year_end", Date, nullable=True),
    Column("sales", Numeric, nullable=True),
    Column("operating_profit", Numeric, nullable=True),
    Column("ordinary_profit", Numeric, nullable=True),
    Column("net_profit", Numeric, nullable=True),
    Column("eps", Numeric, nullable=True),
    Column("diluted_eps", Numeric, nullable=True),
    Column("total_assets", Numeric, nullable=True),
    Column("equity", Numeric, nullable=True),
    Column("equity_attributable_ratio", Numeric, nullable=True),
    Column("bps", Numeric, nullable=True),
    Column("cash_flow_from_operating", Numeric, nullable=True),
    Column("cash_flow_from_investing", Numeric, nullable=True),
    Column("cash_flow_from_financing", Numeric, nullable=True),
    Column("cash_and_equivalents", Numeric, nullable=True),
    Column("dividend_1q", Numeric, nullable=True),
    Column("dividend_2q", Numeric, nullable=True),
    Column("dividend_3q", Numeric, nullable=True),
    Column("dividend_fy", Numeric, nullable=True),
    Column("dividend_annual", Numeric, nullable=True),
    Column("dividend_unit", Text, nullable=True),
    Column("dividend_total_annual", Numeric, nullable=True),
    Column("payout_ratio_annual", Numeric, nullable=True),
    Column("forecast_dividend_1q", Numeric, nullable=True),
    Column("forecast_dividend_2q", Numeric, nullable=True),
    Column("forecast_dividend_3q", Numeric, nullable=True),
    Column("forecast_dividend_fy", Numeric, nullable=True),
    Column("forecast_dividend_annual", Numeric, nullable=True),
    Column("forecast_dividend_unit", Text, nullable=True),
    Column("forecast_dividend_total_annual", Numeric, nullable=True),
    Column("forecast_payout_ratio_annual", Numeric, nullable=True),
    Column("next_forecast_dividend_1q", Numeric, nullable=True),
    Column("next_forecast_dividend_2q", Numeric, nullable=True),
    Column("next_forecast_dividend_3q", Numeric, nullable=True),
    Column("next_forecast_dividend_fy", Numeric, nullable=True),
    Column("next_forecast_dividend_annual", Numeric, nullable=True),
    Column("next_forecast_dividend_unit", Text, nullable=True),
    Column("next_forecast_payout_ratio_annual", Numeric, nullable=True),
    Column("forecast_sales_2q", Numeric, nullable=True),
    Column("forecast_operating_profit_2q", Numeric, nullable=True),
    Column("forecast_ordinary_profit_2q", Numeric, nullable=True),
    Column("forecast_net_profit_2q", Numeric, nullable=True),
    Column("forecast_eps_2q", Numeric, nullable=True),
    Column("next_forecast_sales_2q", Numeric, nullable=True),
    Column("next_forecast_operating_profit_2q", Numeric, nullable=True),
    Column("next_forecast_ordinary_profit_2q", Numeric, nullable=True),
    Column("next_forecast_net_profit_2q", Numeric, nullable=True),
    Column("next_forecast_eps_2q", Numeric, nullable=True),
    Column("forecast_sales_fy", Numeric, nullable=True),
    Column("forecast_operating_profit_fy", Numeric, nullable=True),
    Column("forecast_ordinary_profit_fy", Numeric, nullable=True),
    Column("forecast_net_profit_fy", Numeric, nullable=True),
    Column("forecast_eps_fy", Numeric, nullable=True),
    Column("next_forecast_sales_fy", Numeric, nullable=True),
    Column("next_forecast_operating_profit_fy", Numeric, nullable=True),
    Column("next_forecast_ordinary_profit_fy", Numeric, nullable=True),
    Column("next_forecast_net_profit_fy", Numeric, nullable=True),
    Column("next_forecast_eps_fy", Numeric, nullable=True),
    Column("material_changes_in_subsidiaries", Boolean, nullable=True),
    Column("significant_change_in_scope", Boolean, nullable=True),
    Column("change_by_accounting_standard_revision", Boolean, nullable=True),
    Column("no_change_by_accounting_standard_revision", Boolean, nullable=True),
    Column("change_of_accounting_estimates", Boolean, nullable=True),
    Column("retrospective_restatement", Boolean, nullable=True),
    Column("shares_outstanding_fy", Numeric, nullable=True),
    Column("treasury_shares_fy", Numeric, nullable=True),
    Column("average_shares", Numeric, nullable=True),
    Column("nc_sales", Numeric, nullable=True),
    Column("nc_operating_profit", Numeric, nullable=True),
    Column("nc_ordinary_profit", Numeric, nullable=True),
    Column("nc_net_profit", Numeric, nullable=True),
    Column("nc_eps", Numeric, nullable=True),
    Column("nc_total_assets", Numeric, nullable=True),
    Column("nc_equity", Numeric, nullable=True),
    Column("nc_equity_ratio", Numeric, nullable=True),
    Column("nc_bps", Numeric, nullable=True),
    Column("forecast_nc_sales_2q", Numeric, nullable=True),
    Column("forecast_nc_operating_profit_2q", Numeric, nullable=True),
    Column("forecast_nc_ordinary_profit_2q", Numeric, nullable=True),
    Column("forecast_nc_net_profit_2q", Numeric, nullable=True),
    Column("forecast_nc_eps_2q", Numeric, nullable=True),
    Column("next_forecast_nc_sales_2q", Numeric, nullable=True),
    Column("next_forecast_nc_operating_profit_2q", Numeric, nullable=True),
    Column("next_forecast_nc_ordinary_profit_2q", Numeric, nullable=True),
    Column("next_forecast_nc_net_profit_2q", Numeric, nullable=True),
    Column("next_forecast_nc_eps_2q", Numeric, nullable=True),
    Column("forecast_nc_sales_fy", Numeric, nullable=True),
    Column("forecast_nc_operating_profit_fy", Numeric, nullable=True),
    Column("forecast_nc_ordinary_profit_fy", Numeric, nullable=True),
    Column("forecast_nc_net_profit_fy", Numeric, nullable=True),
    Column("forecast_nc_eps_fy", Numeric, nullable=True),
    Column("next_forecast_nc_sales_fy", Numeric, nullable=True),
    Column("next_forecast_nc_operating_profit_fy", Numeric, nullable=True),
    Column("next_forecast_nc_ordinary_profit_fy", Numeric, nullable=True),
    Column("next_forecast_nc_net_profit_fy", Numeric, nullable=True),
    Column("next_forecast_nc_eps_fy", Numeric, nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/fins/summary'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_fins_summary_code_disclosure_date", "code", "disclosure_date"),
    Index("ix_fins_summary_document_type_disclosure_date", "document_type", "disclosure_date"),
    Index("ix_fins_summary_current_fiscal_year_end_code", "current_fiscal_year_end", "code"),
)

fins_dividend = Table(
    "fins_dividend",
    metadata,
    Column("reference_number", Text, primary_key=True, nullable=False),
    Column("publication_date", Date, nullable=False),
    Column("publication_time", Text, nullable=False),
    Column("code", String(5), nullable=False),
    Column("status_code", String(1), nullable=False),
    Column("board_meeting_date", Date, nullable=False),
    Column("interim_final_code", String(1), nullable=False),
    Column("forecast_revision_code", String(1), nullable=False),
    Column("interim_final_term", Text, nullable=False),
    Column("dividend_rate", Text, nullable=True),
    Column("record_date", Date, nullable=False),
    Column("ex_rights_date", Date, nullable=False),
    Column("actual_record_date", Date, nullable=False),
    Column("payment_start_date", Text, nullable=True),
    Column("corporate_action_reference_number", Text, nullable=False),
    Column("distribution_amount", Text, nullable=True),
    Column("retained_earnings_amount", Text, nullable=True),
    Column("deemed_dividend_amount", Text, nullable=True),
    Column("deemed_capital_gains_amount", Text, nullable=True),
    Column("net_asset_decrease_ratio", Text, nullable=True),
    Column("commemorative_special_code", String(1), nullable=False),
    Column("commemorative_dividend_rate", Text, nullable=True),
    Column("special_dividend_rate", Text, nullable=True),
    Column("source_api", Text, nullable=False, server_default=text("'/v2/fins/dividend'")),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("raw_payload", JSONB, nullable=True),
    Index("ix_fins_dividend_code_publication_date", "code", "publication_date"),
    Index("ix_fins_dividend_record_date_code", "record_date", "code"),
    Index("ix_fins_dividend_ca_reference_number", "corporate_action_reference_number"),
)


def create_db_engine(database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/junex") -> Engine:
    return create_engine(database_url, future=True, pool_pre_ping=True)


def save_summary(engine: Engine, summary: dict[str, float]) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS category_summary (
                    category TEXT PRIMARY KEY,
                    total REAL NOT NULL
                )
                """
            )
        )

        for category, total in summary.items():
            connection.execute(
                text(
                    """
                    INSERT INTO category_summary (category, total)
                    VALUES (:category, :total)
                    ON CONFLICT(category) DO UPDATE SET total = excluded.total
                    """
                ),
                {"category": category, "total": total},
            )


def upsert_equity_master_snapshots(engine: Engine, records: Sequence[EquityMasterRecord]) -> int:
    if not records:
        return 0

    rows = [_equity_master_row(record) for record in records]
    insert_stmt = pg_insert(equity_master_snapshot).values(rows)

    update_columns = {
        "company_name": insert_stmt.excluded.company_name,
        "company_name_en": insert_stmt.excluded.company_name_en,
        "sector17_code": insert_stmt.excluded.sector17_code,
        "sector17_name": insert_stmt.excluded.sector17_name,
        "sector33_code": insert_stmt.excluded.sector33_code,
        "sector33_name": insert_stmt.excluded.sector33_name,
        "scale_category": insert_stmt.excluded.scale_category,
        "market_code": insert_stmt.excluded.market_code,
        "market_name": insert_stmt.excluded.market_name,
        "margin_code": insert_stmt.excluded.margin_code,
        "margin_name": insert_stmt.excluded.margin_name,
        "source_api": insert_stmt.excluded.source_api,
        "fetched_at": insert_stmt.excluded.fetched_at,
        "raw_payload": insert_stmt.excluded.raw_payload,
    }

    stmt = insert_stmt.on_conflict_do_update(
        index_elements=[equity_master_snapshot.c.as_of_date, equity_master_snapshot.c.code],
        set_=update_columns,
    )

    with engine.begin() as connection:
        connection.execute(stmt)

    return len(rows)


def upsert_equity_daily_bars(
    engine: Engine, records: Sequence[EquityDailyBarRecord]
) -> tuple[int, list[dict[str, object]]]:
    if not records:
        return 0, []

    all_rows = [_equity_daily_bar_row(record) for record in records]
    compared_column_names = [
        column.name
        for column in equity_daily_bar.columns
        if column.name
        not in {
            "trade_date",
            "code",
            "has_source_revision",
            "source_revision_count",
            "last_source_revision_at",
            "source_api",
            "fetched_at",
            "raw_payload",
        }
    ]

    total_rows = 0
    all_revised_rows: list[dict[str, object]] = []

    with engine.begin() as connection:
        for start in range(0, len(all_rows), UPSERT_BATCH_SIZE):
            rows = all_rows[start : start + UPSERT_BATCH_SIZE]
            existing_stmt = select(equity_daily_bar).where(
                tuple_(equity_daily_bar.c.trade_date, equity_daily_bar.c.code).in_(
                    [(row["trade_date"], row["code"]) for row in rows]
                )
            )
            existing_rows = {
                (row.trade_date, row.code): dict(row._mapping)
                for row in connection.execute(existing_stmt)
            }

            final_rows: list[dict[str, object]] = []
            for row in rows:
                key = (row["trade_date"], row["code"])
                existing_row = existing_rows.get(key)
                final_row = dict(row)

                if existing_row is not None:
                    changed = any(
                        existing_row.get(column_name) != row.get(column_name)
                        for column_name in compared_column_names
                    )
                    if changed:
                        previous_count = existing_row.get("source_revision_count", 0) or 0
                        final_row["has_source_revision"] = True
                        final_row["source_revision_count"] = previous_count + 1
                        final_row["last_source_revision_at"] = final_row["fetched_at"]
                        all_revised_rows.append(
                            {
                                "trade_date": row["trade_date"].isoformat(),
                                "code": row["code"],
                                "previous_source_revision_count": previous_count,
                            }
                        )
                    else:
                        final_row["has_source_revision"] = existing_row.get("has_source_revision", False)
                        final_row["source_revision_count"] = existing_row.get("source_revision_count", 0) or 0
                        final_row["last_source_revision_at"] = existing_row.get("last_source_revision_at")

                final_rows.append(final_row)

            insert_stmt = pg_insert(equity_daily_bar).values(final_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in equity_daily_bar.columns
                if column.name not in {"trade_date", "code"}
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[equity_daily_bar.c.trade_date, equity_daily_bar.c.code],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(final_rows)

    return total_rows, all_revised_rows


def upsert_market_calendar(engine: Engine, records: Sequence[MarketCalendarRecord]) -> int:
    if not records:
        return 0

    rows = [_market_calendar_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(market_calendar).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in market_calendar.columns
                if column.name != "trade_date"
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[market_calendar.c.trade_date],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def upsert_topix_daily_bars(engine: Engine, records: Sequence[TopixDailyBarRecord]) -> int:
    if not records:
        return 0

    rows = [_topix_daily_bar_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(topix_daily_bar).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in topix_daily_bar.columns
                if column.name != "trade_date"
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[topix_daily_bar.c.trade_date],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def upsert_index_daily_bars(engine: Engine, records: Sequence[IndexDailyBarRecord]) -> int:
    if not records:
        return 0

    rows = [_index_daily_bar_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(index_daily_bar).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in index_daily_bar.columns
                if column.name not in {"trade_date", "code"}
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[index_daily_bar.c.trade_date, index_daily_bar.c.code],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def upsert_earnings_calendar(engine: Engine, records: Sequence[EarningsCalendarRecord]) -> int:
    if not records:
        return 0

    rows = [_earnings_calendar_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(earnings_calendar).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in earnings_calendar.columns
                if column.name not in {"scheduled_date", "code"}
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[earnings_calendar.c.scheduled_date, earnings_calendar.c.code],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def upsert_fins_summary(engine: Engine, records: Sequence[FinsSummaryRecord]) -> int:
    if not records:
        return 0

    rows = [_fins_summary_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(fins_summary).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in fins_summary.columns
                if column.name != "disclosure_number"
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[fins_summary.c.disclosure_number],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def upsert_fins_dividend(engine: Engine, records: Sequence[FinsDividendRecord]) -> int:
    if not records:
        return 0

    rows = [_fins_dividend_row(record) for record in records]
    total_rows = 0

    with engine.begin() as connection:
        for start in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch_rows = rows[start : start + UPSERT_BATCH_SIZE]
            insert_stmt = pg_insert(fins_dividend).values(batch_rows)
            update_columns = {
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in fins_dividend.columns
                if column.name != "reference_number"
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[fins_dividend.c.reference_number],
                set_=update_columns,
            )
            connection.execute(stmt)
            total_rows += len(batch_rows)

    return total_rows


def _equity_master_row(record: EquityMasterRecord) -> dict[str, object]:
    return {
        "as_of_date": record.as_of_date,
        "code": record.code,
        "company_name": record.company_name,
        "company_name_en": record.company_name_en,
        "sector17_code": record.sector17_code,
        "sector17_name": record.sector17_name,
        "sector33_code": record.sector33_code,
        "sector33_name": record.sector33_name,
        "scale_category": record.scale_category,
        "market_code": record.market_code,
        "market_name": record.market_name,
        "margin_code": record.margin_code,
        "margin_name": record.margin_name,
        "source_api": "/v2/equities/master",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _equity_daily_bar_row(record: EquityDailyBarRecord) -> dict[str, object]:
    return {
        "trade_date": record.trade_date,
        "code": record.code,
        "open_price": record.open_price,
        "high_price": record.high_price,
        "low_price": record.low_price,
        "close_price": record.close_price,
        "upper_limit_flag": record.upper_limit_flag,
        "lower_limit_flag": record.lower_limit_flag,
        "volume": record.volume,
        "turnover_value": record.turnover_value,
        "adjustment_factor": record.adjustment_factor,
        "adjusted_open_price": record.adjusted_open_price,
        "adjusted_high_price": record.adjusted_high_price,
        "adjusted_low_price": record.adjusted_low_price,
        "adjusted_close_price": record.adjusted_close_price,
        "adjusted_volume": record.adjusted_volume,
        "morning_open_price": record.morning_open_price,
        "morning_high_price": record.morning_high_price,
        "morning_low_price": record.morning_low_price,
        "morning_close_price": record.morning_close_price,
        "morning_upper_limit_flag": record.morning_upper_limit_flag,
        "morning_lower_limit_flag": record.morning_lower_limit_flag,
        "morning_volume": record.morning_volume,
        "morning_turnover_value": record.morning_turnover_value,
        "morning_adjusted_open_price": record.morning_adjusted_open_price,
        "morning_adjusted_high_price": record.morning_adjusted_high_price,
        "morning_adjusted_low_price": record.morning_adjusted_low_price,
        "morning_adjusted_close_price": record.morning_adjusted_close_price,
        "morning_adjusted_volume": record.morning_adjusted_volume,
        "afternoon_open_price": record.afternoon_open_price,
        "afternoon_high_price": record.afternoon_high_price,
        "afternoon_low_price": record.afternoon_low_price,
        "afternoon_close_price": record.afternoon_close_price,
        "afternoon_upper_limit_flag": record.afternoon_upper_limit_flag,
        "afternoon_lower_limit_flag": record.afternoon_lower_limit_flag,
        "afternoon_volume": record.afternoon_volume,
        "afternoon_turnover_value": record.afternoon_turnover_value,
        "afternoon_adjusted_open_price": record.afternoon_adjusted_open_price,
        "afternoon_adjusted_high_price": record.afternoon_adjusted_high_price,
        "afternoon_adjusted_low_price": record.afternoon_adjusted_low_price,
        "afternoon_adjusted_close_price": record.afternoon_adjusted_close_price,
        "afternoon_adjusted_volume": record.afternoon_adjusted_volume,
        "has_source_revision": False,
        "source_revision_count": 0,
        "last_source_revision_at": None,
        "source_api": "/v2/equities/bars/daily",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _market_calendar_row(record: MarketCalendarRecord) -> dict[str, object]:
    return {
        "trade_date": record.trade_date,
        "holiday_division": record.holiday_division,
        "source_api": "/v2/markets/calendar",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _topix_daily_bar_row(record: TopixDailyBarRecord) -> dict[str, object]:
    return {
        "trade_date": record.trade_date,
        "open_price": record.open_price,
        "high_price": record.high_price,
        "low_price": record.low_price,
        "close_price": record.close_price,
        "source_api": "/v2/indices/bars/daily/topix",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _index_daily_bar_row(record: IndexDailyBarRecord) -> dict[str, object]:
    return {
        "trade_date": record.trade_date,
        "code": record.code,
        "open_price": record.open_price,
        "high_price": record.high_price,
        "low_price": record.low_price,
        "close_price": record.close_price,
        "source_api": "/v2/indices/bars/daily",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _earnings_calendar_row(record: EarningsCalendarRecord) -> dict[str, object]:
    return {
        "scheduled_date": record.scheduled_date,
        "code": record.code,
        "company_name": record.company_name,
        "fiscal_year_end": record.fiscal_year_end,
        "sector_name": record.sector_name,
        "fiscal_quarter": record.fiscal_quarter,
        "section": record.section,
        "source_api": "/v2/equities/earnings-calendar",
        "fetched_at": datetime.now(timezone.utc),
        "raw_payload": record.model_dump(mode="json", by_alias=True),
    }


def _fins_summary_row(record: FinsSummaryRecord) -> dict[str, object]:
    payload = record.model_dump(mode="json", by_alias=True)
    row = record.model_dump(by_alias=False)
    row["source_api"] = "/v2/fins/summary"
    row["fetched_at"] = datetime.now(timezone.utc)
    row["raw_payload"] = payload
    return row


def _fins_dividend_row(record: FinsDividendRecord) -> dict[str, object]:
    payload = record.model_dump(mode="json", by_alias=True)
    row = record.model_dump(by_alias=False)
    row["source_api"] = "/v2/fins/dividend"
    row["fetched_at"] = datetime.now(timezone.utc)
    row["raw_payload"] = payload
    return row
