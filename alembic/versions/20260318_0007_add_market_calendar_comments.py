"""add market calendar comments

Revision ID: 20260318_0007
Revises: 20260318_0006
Create Date: 2026-03-18 01:00:00
"""
from __future__ import annotations

from alembic import op


revision = "20260318_0007"
down_revision = "20260318_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        COMMENT ON TABLE market_data.market_calendar IS
        'J-Quants 交易日历表。每行代表一个自然日，用于判断该日是否为交易所营业日。数据来自 /v2/markets/calendar。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.trade_date IS
        '自然日日期，格式为 YYYY-MM-DD。该表以 trade_date 为主键，一天只有一条记录。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.holiday_division IS
        'J-Quants 的 HolDiv 休日区分代码。取值含义为：0 = 非営業日（非营业日，交易所休市）；1 = 営業日（正常营业日）；2 = 東証半日立会日（东京证券交易所半日交易日）；3 = 非営業日(祝日取引あり)（非营业日，但存在祝日交易安排）。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.source_api IS
        '数据来源 API 路径。当前固定为 /v2/markets/calendar。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.fetched_at IS
        '本地抓取并写入数据库的时间戳，不是交易所公告时间。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.raw_payload IS
        'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';
        """
    )


def downgrade() -> None:
    op.execute("COMMENT ON COLUMN market_data.market_calendar.raw_payload IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.fetched_at IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.source_api IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.holiday_division IS NULL;")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.trade_date IS NULL;")
    op.execute("COMMENT ON TABLE market_data.market_calendar IS NULL;")
