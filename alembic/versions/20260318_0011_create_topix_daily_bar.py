"""create topix daily bar table

Revision ID: 20260318_0011
Revises: 20260318_0010
Create Date: 2026-03-18 02:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0011"
down_revision = "20260318_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "topix_daily_bar",
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open_price", sa.Float(), nullable=True),
        sa.Column("high_price", sa.Float(), nullable=True),
        sa.Column("low_price", sa.Float(), nullable=True),
        sa.Column("close_price", sa.Float(), nullable=True),
        sa.Column(
            "source_api",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'/v2/indices/bars/daily/topix'"),
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("trade_date", name="pk_topix_daily_bar"),
        schema="market_data",
    )
    op.create_index(
        "ix_topix_daily_bar_trade_date",
        "topix_daily_bar",
        ["trade_date"],
        unique=False,
        schema="market_data",
    )

    op.execute(
        """
        COMMENT ON TABLE market_data.topix_daily_bar IS
        'J-Quants TOPIX 日线四本值表。每行代表一个交易日的 TOPIX 指数 OHLC，主键为 trade_date。数据来源 /v2/indices/bars/daily/topix。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.trade_date IS '交易日期，对应 API 字段 Date。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.open_price IS '开盘价，对应 API 字段 O。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.high_price IS '最高价，对应 API 字段 H。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.low_price IS '最低价，对应 API 字段 L。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.close_price IS '收盘价，对应 API 字段 C。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.source_api IS '数据来源 API 路径。当前固定为 /v2/indices/bars/daily/topix。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.fetched_at IS '本地抓取并写入数据库的时间戳，不是交易所发布时间。';")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")


def downgrade() -> None:
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.raw_payload IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.fetched_at IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.source_api IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.close_price IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.low_price IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.high_price IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.open_price IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.topix_daily_bar.trade_date IS NULL;")
    op.execute("COMMENT ON TABLE market_data.topix_daily_bar IS NULL;")
    op.drop_index("ix_topix_daily_bar_trade_date", table_name="topix_daily_bar", schema="market_data")
    op.drop_table("topix_daily_bar", schema="market_data")
