"""create index daily bar and fins dividend tables

Revision ID: 20260318_0012
Revises: 20260318_0011
Create Date: 2026-03-18 06:45:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0012"
down_revision = "20260318_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "index_daily_bar",
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=4), nullable=False),
        sa.Column("open_price", sa.Float(), nullable=True),
        sa.Column("high_price", sa.Float(), nullable=True),
        sa.Column("low_price", sa.Float(), nullable=True),
        sa.Column("close_price", sa.Float(), nullable=True),
        sa.Column("source_api", sa.Text(), nullable=False, server_default=sa.text("'/v2/indices/bars/daily'")),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("trade_date", "code", name="pk_index_daily_bar"),
        schema="market_data",
    )
    op.create_index("ix_index_daily_bar_code_trade_date", "index_daily_bar", ["code", "trade_date"], unique=False, schema="market_data")
    op.create_index("ix_index_daily_bar_trade_date", "index_daily_bar", ["trade_date"], unique=False, schema="market_data")

    op.execute(
        """
        COMMENT ON TABLE market_data.index_daily_bar IS
        'J-Quants 指数日线四本值表。每行代表某个交易日的一只指数 OHLC，主键为 (trade_date, code)。数据来源 /v2/indices/bars/daily。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.trade_date IS '交易日期，对应 API 字段 Date。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.code IS '指数代码，对应 API 字段 Code，例如 0000、0028。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.open_price IS '开盘价，对应 API 字段 O。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.high_price IS '最高价，对应 API 字段 H。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.low_price IS '最低价，对应 API 字段 L。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.close_price IS '收盘价，对应 API 字段 C。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.source_api IS '数据来源 API 路径。当前固定为 /v2/indices/bars/daily。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.fetched_at IS '本地抓取并写入数据库的时间戳，不是指数发布时间。';")
    op.execute("COMMENT ON COLUMN market_data.index_daily_bar.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")

    op.create_table(
        "fins_dividend",
        sa.Column("reference_number", sa.Text(), nullable=False),
        sa.Column("publication_date", sa.Date(), nullable=False),
        sa.Column("publication_time", sa.Text(), nullable=False),
        sa.Column("code", sa.String(length=5), nullable=False),
        sa.Column("status_code", sa.String(length=1), nullable=False),
        sa.Column("board_meeting_date", sa.Date(), nullable=False),
        sa.Column("interim_final_code", sa.String(length=1), nullable=False),
        sa.Column("forecast_revision_code", sa.String(length=1), nullable=False),
        sa.Column("interim_final_term", sa.Text(), nullable=False),
        sa.Column("dividend_rate", sa.Text(), nullable=True),
        sa.Column("record_date", sa.Date(), nullable=False),
        sa.Column("ex_rights_date", sa.Date(), nullable=False),
        sa.Column("actual_record_date", sa.Date(), nullable=False),
        sa.Column("payment_start_date", sa.Text(), nullable=True),
        sa.Column("corporate_action_reference_number", sa.Text(), nullable=False),
        sa.Column("distribution_amount", sa.Text(), nullable=True),
        sa.Column("retained_earnings_amount", sa.Text(), nullable=True),
        sa.Column("deemed_dividend_amount", sa.Text(), nullable=True),
        sa.Column("deemed_capital_gains_amount", sa.Text(), nullable=True),
        sa.Column("net_asset_decrease_ratio", sa.Text(), nullable=True),
        sa.Column("commemorative_special_code", sa.String(length=1), nullable=False),
        sa.Column("commemorative_dividend_rate", sa.Text(), nullable=True),
        sa.Column("special_dividend_rate", sa.Text(), nullable=True),
        sa.Column("source_api", sa.Text(), nullable=False, server_default=sa.text("'/v2/fins/dividend'")),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("reference_number", name="pk_fins_dividend"),
        schema="market_data",
    )
    op.create_index("ix_fins_dividend_code_publication_date", "fins_dividend", ["code", "publication_date"], unique=False, schema="market_data")
    op.create_index("ix_fins_dividend_record_date_code", "fins_dividend", ["record_date", "code"], unique=False, schema="market_data")
    op.create_index("ix_fins_dividend_ca_reference_number", "fins_dividend", ["corporate_action_reference_number"], unique=False, schema="market_data")

    op.execute(
        """
        COMMENT ON TABLE market_data.fins_dividend IS
        'J-Quants 配当金信息表。每行代表一次配当通知事件，主键为 RefNo。数据来源 /v2/fins/dividend。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.reference_number IS '配当通知参考号，对应 API 字段 RefNo。作为唯一主键。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.publication_date IS '通知日期，对应 API 字段 PubDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.publication_time IS '通知时间，对应 API 字段 PubTime，格式通常为 HH:MM。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.code IS '证券代码，对应 API 字段 Code。4 位代码查询时 API 可能返回 5 位普通股代码。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.status_code IS '更新区分代码，对应 API 字段 StatCode。1=新规，2=订正，3=删除。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.board_meeting_date IS '董事会决议日，对应 API 字段 BoardDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.interim_final_code IS '配当种类代码，对应 API 字段 IFCode。1=中间配当，2=期末配当。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.forecast_revision_code IS '预测/决定代码，对应 API 字段 FRCode。1=决定，2=预测。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.interim_final_term IS '配当基准年月，对应 API 字段 IFTerm，例如 2014-03。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.dividend_rate IS '每股配当金原始值，对应 API 字段 DivRate。可能是数值文本、-（未定）或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.record_date IS '基准日，对应 API 字段 RecDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.ex_rights_date IS '权利落日，对应 API 字段 ExDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.actual_record_date IS '权益确定日，对应 API 字段 ActRecDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.payment_start_date IS '支付开始予定日原始值，对应 API 字段 PayDate。可能是日期文本、-（未定）或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.corporate_action_reference_number IS '公司行动参考号，对应 API 字段 CARefNo。订正/删除时指向原通知。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.distribution_amount IS '每股交付金钱等金额原始值，对应 API 字段 DistAmt。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.retained_earnings_amount IS '每股利益剰余金额原始值，对应 API 字段 RetEarn。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.deemed_dividend_amount IS '每股みなし配当额原始值，对应 API 字段 DeemDiv。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.deemed_capital_gains_amount IS '每股みなし譲渡収入额原始值，对应 API 字段 DeemCapGains。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.net_asset_decrease_ratio IS '纯资产减少比例原始值，对应 API 字段 NetAssetDecRatio。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.commemorative_special_code IS '纪念/特别配当代码，对应 API 字段 CommSpecCode。0=普通，1=纪念，2=特别，3=纪念且特别。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.commemorative_dividend_rate IS '每股纪念配当金额原始值，对应 API 字段 CommDivRate。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.special_dividend_rate IS '每股特别配当金额原始值，对应 API 字段 SpecDivRate。可能是数值文本、- 或空值。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.source_api IS '数据来源 API 路径。当前固定为 /v2/fins/dividend。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.fetched_at IS '本地抓取并写入数据库的时间戳。';")
    op.execute("COMMENT ON COLUMN market_data.fins_dividend.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")


def downgrade() -> None:
    op.drop_index("ix_fins_dividend_ca_reference_number", table_name="fins_dividend", schema="market_data")
    op.drop_index("ix_fins_dividend_record_date_code", table_name="fins_dividend", schema="market_data")
    op.drop_index("ix_fins_dividend_code_publication_date", table_name="fins_dividend", schema="market_data")
    op.drop_table("fins_dividend", schema="market_data")

    op.drop_index("ix_index_daily_bar_trade_date", table_name="index_daily_bar", schema="market_data")
    op.drop_index("ix_index_daily_bar_code_trade_date", table_name="index_daily_bar", schema="market_data")
    op.drop_table("index_daily_bar", schema="market_data")
