"""create equity daily bar table

Revision ID: 20260318_0004
Revises: 20260318_0003
Create Date: 2026-03-18 00:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0004"
down_revision = "20260318_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "equity_daily_bar",
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=5), nullable=False),
        sa.Column("open_price", sa.Float(), nullable=True),
        sa.Column("high_price", sa.Float(), nullable=True),
        sa.Column("low_price", sa.Float(), nullable=True),
        sa.Column("close_price", sa.Float(), nullable=True),
        sa.Column("upper_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("lower_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("volume", sa.Float(), nullable=True),
        sa.Column("turnover_value", sa.Float(), nullable=True),
        sa.Column("adjustment_factor", sa.Float(), nullable=True),
        sa.Column("adjusted_open_price", sa.Float(), nullable=True),
        sa.Column("adjusted_high_price", sa.Float(), nullable=True),
        sa.Column("adjusted_low_price", sa.Float(), nullable=True),
        sa.Column("adjusted_close_price", sa.Float(), nullable=True),
        sa.Column("adjusted_volume", sa.Float(), nullable=True),
        sa.Column("morning_open_price", sa.Float(), nullable=True),
        sa.Column("morning_high_price", sa.Float(), nullable=True),
        sa.Column("morning_low_price", sa.Float(), nullable=True),
        sa.Column("morning_close_price", sa.Float(), nullable=True),
        sa.Column("morning_upper_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("morning_lower_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("morning_volume", sa.Float(), nullable=True),
        sa.Column("morning_turnover_value", sa.Float(), nullable=True),
        sa.Column("morning_adjusted_open_price", sa.Float(), nullable=True),
        sa.Column("morning_adjusted_high_price", sa.Float(), nullable=True),
        sa.Column("morning_adjusted_low_price", sa.Float(), nullable=True),
        sa.Column("morning_adjusted_close_price", sa.Float(), nullable=True),
        sa.Column("morning_adjusted_volume", sa.Float(), nullable=True),
        sa.Column("afternoon_open_price", sa.Float(), nullable=True),
        sa.Column("afternoon_high_price", sa.Float(), nullable=True),
        sa.Column("afternoon_low_price", sa.Float(), nullable=True),
        sa.Column("afternoon_close_price", sa.Float(), nullable=True),
        sa.Column("afternoon_upper_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("afternoon_lower_limit_flag", sa.String(length=1), nullable=True),
        sa.Column("afternoon_volume", sa.Float(), nullable=True),
        sa.Column("afternoon_turnover_value", sa.Float(), nullable=True),
        sa.Column("afternoon_adjusted_open_price", sa.Float(), nullable=True),
        sa.Column("afternoon_adjusted_high_price", sa.Float(), nullable=True),
        sa.Column("afternoon_adjusted_low_price", sa.Float(), nullable=True),
        sa.Column("afternoon_adjusted_close_price", sa.Float(), nullable=True),
        sa.Column("afternoon_adjusted_volume", sa.Float(), nullable=True),
        sa.Column(
            "source_api",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'/v2/equities/bars/daily'"),
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("trade_date", "code", name="pk_equity_daily_bar"),
        schema="market_data",
    )
    op.create_index(
        "ix_equity_daily_bar_code_trade_date",
        "equity_daily_bar",
        ["code", "trade_date"],
        unique=False,
        schema="market_data",
    )
    op.create_index(
        "ix_equity_daily_bar_trade_date",
        "equity_daily_bar",
        ["trade_date"],
        unique=False,
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_index("ix_equity_daily_bar_trade_date", table_name="equity_daily_bar", schema="market_data")
    op.drop_index(
        "ix_equity_daily_bar_code_trade_date",
        table_name="equity_daily_bar",
        schema="market_data",
    )
    op.drop_table("equity_daily_bar", schema="market_data")
