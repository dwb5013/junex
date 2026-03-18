"""create earnings calendar table

Revision ID: 20260318_0009
Revises: 20260318_0008
Create Date: 2026-03-18 01:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0009"
down_revision = "20260318_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "earnings_calendar",
        sa.Column("scheduled_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=5), nullable=False),
        sa.Column("company_name", sa.Text(), nullable=False),
        sa.Column("fiscal_year_end", sa.Text(), nullable=False),
        sa.Column("sector_name", sa.Text(), nullable=False),
        sa.Column("fiscal_quarter", sa.Text(), nullable=False),
        sa.Column("section", sa.Text(), nullable=False),
        sa.Column(
            "source_api",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'/v2/equities/earnings-calendar'"),
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("scheduled_date", "code", name="pk_earnings_calendar"),
        schema="market_data",
    )
    op.create_index(
        "ix_earnings_calendar_code_scheduled_date",
        "earnings_calendar",
        ["code", "scheduled_date"],
        unique=False,
        schema="market_data",
    )
    op.create_index(
        "ix_earnings_calendar_scheduled_date",
        "earnings_calendar",
        ["scheduled_date"],
        unique=False,
        schema="market_data",
    )
    op.execute(
        """
        COMMENT ON TABLE market_data.earnings_calendar IS
        'J-Quants 決算発表予定日表。每行代表某个予定披露日的一家公司记录，主键为 (scheduled_date, code)。数据来源 /v2/equities/earnings-calendar。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.scheduled_date IS '预计决算披露日期，对应 API 字段 Date。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.code IS '证券代码，对应 API 字段 Code。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.company_name IS '公司名称，对应 API 字段 CoName。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.fiscal_year_end IS '决算期末日，对应 API 字段 FY，例如 3月31日、9月30日。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.sector_name IS '业种名称，对应 API 字段 SectorNm。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.fiscal_quarter IS '财报季度，对应 API 字段 FQ，例如 第１四半期、第３四半期、通期 等。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.section IS '市场板块名称，对应 API 字段 Section，例如 プライム、スタンダード、グロース。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.source_api IS '数据来源 API 路径。当前固定为 /v2/equities/earnings-calendar。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.fetched_at IS '本地抓取并写入数据库的时间戳，不是交易所公告时间。';")
    op.execute("COMMENT ON COLUMN market_data.earnings_calendar.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")


def downgrade() -> None:
    op.drop_index("ix_earnings_calendar_scheduled_date", table_name="earnings_calendar", schema="market_data")
    op.drop_index(
        "ix_earnings_calendar_code_scheduled_date",
        table_name="earnings_calendar",
        schema="market_data",
    )
    op.drop_table("earnings_calendar", schema="market_data")
