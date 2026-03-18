"""create market calendar table

Revision ID: 20260318_0006
Revises: 20260318_0005
Create Date: 2026-03-18 00:50:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0006"
down_revision = "20260318_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_calendar",
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("holiday_division", sa.String(length=1), nullable=False),
        sa.Column(
            "source_api",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'/v2/markets/calendar'"),
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("trade_date", name="pk_market_calendar"),
        schema="market_data",
    )
    op.create_index(
        "ix_market_calendar_holiday_division_trade_date",
        "market_calendar",
        ["holiday_division", "trade_date"],
        unique=False,
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_market_calendar_holiday_division_trade_date",
        table_name="market_calendar",
        schema="market_data",
    )
    op.drop_table("market_calendar", schema="market_data")
