"""create fins summary table

Revision ID: 20260318_0010
Revises: 20260318_0009
Create Date: 2026-03-18 02:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0010"
down_revision = "20260318_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fins_summary",
        sa.Column("disclosure_number", sa.Text(), nullable=False),
        sa.Column("disclosure_date", sa.Date(), nullable=False),
        sa.Column("disclosure_time", sa.Text(), nullable=True),
        sa.Column("code", sa.String(length=5), nullable=False),
        sa.Column("document_type", sa.Text(), nullable=False),
        sa.Column("current_period_type", sa.Text(), nullable=True),
        sa.Column("current_period_start", sa.Date(), nullable=True),
        sa.Column("current_period_end", sa.Date(), nullable=True),
        sa.Column("current_fiscal_year_start", sa.Date(), nullable=True),
        sa.Column("current_fiscal_year_end", sa.Date(), nullable=True),
        sa.Column("next_fiscal_year_start", sa.Date(), nullable=True),
        sa.Column("next_fiscal_year_end", sa.Date(), nullable=True),
        sa.Column("sales", sa.Numeric(), nullable=True),
        sa.Column("operating_profit", sa.Numeric(), nullable=True),
        sa.Column("ordinary_profit", sa.Numeric(), nullable=True),
        sa.Column("net_profit", sa.Numeric(), nullable=True),
        sa.Column("eps", sa.Numeric(), nullable=True),
        sa.Column("diluted_eps", sa.Numeric(), nullable=True),
        sa.Column("total_assets", sa.Numeric(), nullable=True),
        sa.Column("equity", sa.Numeric(), nullable=True),
        sa.Column("equity_attributable_ratio", sa.Numeric(), nullable=True),
        sa.Column("bps", sa.Numeric(), nullable=True),
        sa.Column("cash_flow_from_operating", sa.Numeric(), nullable=True),
        sa.Column("cash_flow_from_investing", sa.Numeric(), nullable=True),
        sa.Column("cash_flow_from_financing", sa.Numeric(), nullable=True),
        sa.Column("cash_and_equivalents", sa.Numeric(), nullable=True),
        sa.Column("dividend_1q", sa.Numeric(), nullable=True),
        sa.Column("dividend_2q", sa.Numeric(), nullable=True),
        sa.Column("dividend_3q", sa.Numeric(), nullable=True),
        sa.Column("dividend_fy", sa.Numeric(), nullable=True),
        sa.Column("dividend_annual", sa.Numeric(), nullable=True),
        sa.Column("dividend_unit", sa.Text(), nullable=True),
        sa.Column("dividend_total_annual", sa.Numeric(), nullable=True),
        sa.Column("payout_ratio_annual", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_1q", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_3q", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_annual", sa.Numeric(), nullable=True),
        sa.Column("forecast_dividend_unit", sa.Text(), nullable=True),
        sa.Column("forecast_dividend_total_annual", sa.Numeric(), nullable=True),
        sa.Column("forecast_payout_ratio_annual", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_1q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_3q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_annual", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_dividend_unit", sa.Text(), nullable=True),
        sa.Column("next_forecast_payout_ratio_annual", sa.Numeric(), nullable=True),
        sa.Column("forecast_sales_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_operating_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_ordinary_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_net_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_eps_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_sales_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_operating_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_ordinary_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_net_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_eps_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_sales_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_operating_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_ordinary_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_net_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_eps_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_sales_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_operating_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_ordinary_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_net_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_eps_fy", sa.Numeric(), nullable=True),
        sa.Column("material_changes_in_subsidiaries", sa.Boolean(), nullable=True),
        sa.Column("significant_change_in_scope", sa.Boolean(), nullable=True),
        sa.Column("change_by_accounting_standard_revision", sa.Boolean(), nullable=True),
        sa.Column("no_change_by_accounting_standard_revision", sa.Boolean(), nullable=True),
        sa.Column("change_of_accounting_estimates", sa.Boolean(), nullable=True),
        sa.Column("retrospective_restatement", sa.Boolean(), nullable=True),
        sa.Column("shares_outstanding_fy", sa.Numeric(), nullable=True),
        sa.Column("treasury_shares_fy", sa.Numeric(), nullable=True),
        sa.Column("average_shares", sa.Numeric(), nullable=True),
        sa.Column("nc_sales", sa.Numeric(), nullable=True),
        sa.Column("nc_operating_profit", sa.Numeric(), nullable=True),
        sa.Column("nc_ordinary_profit", sa.Numeric(), nullable=True),
        sa.Column("nc_net_profit", sa.Numeric(), nullable=True),
        sa.Column("nc_eps", sa.Numeric(), nullable=True),
        sa.Column("nc_total_assets", sa.Numeric(), nullable=True),
        sa.Column("nc_equity", sa.Numeric(), nullable=True),
        sa.Column("nc_equity_ratio", sa.Numeric(), nullable=True),
        sa.Column("nc_bps", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_sales_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_operating_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_ordinary_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_net_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_eps_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_sales_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_operating_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_ordinary_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_net_profit_2q", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_eps_2q", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_sales_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_operating_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_ordinary_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_net_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("forecast_nc_eps_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_sales_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_operating_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_ordinary_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_net_profit_fy", sa.Numeric(), nullable=True),
        sa.Column("next_forecast_nc_eps_fy", sa.Numeric(), nullable=True),
        sa.Column("source_api", sa.Text(), nullable=False, server_default=sa.text("'/v2/fins/summary'")),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("disclosure_number", name="pk_fins_summary"),
        schema="market_data",
    )
    op.create_index("ix_fins_summary_code_disclosure_date", "fins_summary", ["code", "disclosure_date"], unique=False, schema="market_data")
    op.create_index("ix_fins_summary_document_type_disclosure_date", "fins_summary", ["document_type", "disclosure_date"], unique=False, schema="market_data")
    op.create_index("ix_fins_summary_current_fiscal_year_end_code", "fins_summary", ["current_fiscal_year_end", "code"], unique=False, schema="market_data")
    op.execute("COMMENT ON TABLE market_data.fins_summary IS 'J-Quants 財務情報サマリー。每行代表一次财务披露事件，主键为 DiscNo。数据来源 /v2/fins/summary。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.disclosure_number IS '披露编号，对应 API 字段 DiscNo。作为唯一主键。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.disclosure_date IS '披露日期，对应 API 字段 DiscDate。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.disclosure_time IS '披露时间，对应 API 字段 DiscTime。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.code IS '证券代码，对应 API 字段 Code。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.document_type IS '披露文档类型，对应 API 字段 DocType，例如 FYFinancialStatements_Consolidated_JP、EarnForecastRevision。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.current_period_type IS '当前披露对应期间类型，对应 API 字段 CurPerType，例如 1Q、2Q、3Q、FY。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.current_period_start IS '当前披露期间开始日，对应 API 字段 CurPerSt。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.current_period_end IS '当前披露期间结束日，对应 API 字段 CurPerEn。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.current_fiscal_year_start IS '当前会计年度开始日，对应 API 字段 CurFYSt。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.current_fiscal_year_end IS '当前会计年度结束日，对应 API 字段 CurFYEn。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.next_fiscal_year_start IS '下一会计年度开始日，对应 API 字段 NxtFYSt。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.next_fiscal_year_end IS '下一会计年度结束日，对应 API 字段 NxtFYEn。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.sales IS '营业收入，对应 API 字段 Sales。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.operating_profit IS '营业利润，对应 API 字段 OP。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.ordinary_profit IS '经常利润，对应 API 字段 OdP。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.net_profit IS '净利润，对应 API 字段 NP。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.eps IS '每股收益，对应 API 字段 EPS。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.diluted_eps IS '稀释后每股收益，对应 API 字段 DEPS。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.total_assets IS '总资产，对应 API 字段 TA。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.equity IS '净资产/权益，对应 API 字段 Eq。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.equity_attributable_ratio IS '权益比率，对应 API 字段 EqAR。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.bps IS '每股净资产，对应 API 字段 BPS。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.cash_flow_from_operating IS '经营活动现金流，对应 API 字段 CFO。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.cash_flow_from_investing IS '投资活动现金流，对应 API 字段 CFI。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.cash_flow_from_financing IS '融资活动现金流，对应 API 字段 CFF。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.cash_and_equivalents IS '现金及现金等价物，对应 API 字段 CashEq。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.material_changes_in_subsidiaries IS '是否发生重要子公司变动，对应 API 字段 MatChgSub。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.significant_change_in_scope IS '是否发生重要范围变更，对应 API 字段 SigChgInC。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.change_by_accounting_standard_revision IS '是否因会计准则修订而变更，对应 API 字段 ChgByASRev。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.no_change_by_accounting_standard_revision IS '会计准则修订是否无影响，对应 API 字段 ChgNoASRev。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.change_of_accounting_estimates IS '是否发生会计估计变更，对应 API 字段 ChgAcEst。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.retrospective_restatement IS '是否存在追溯重述，对应 API 字段 RetroRst。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.source_api IS '数据来源 API 路径。当前固定为 /v2/fins/summary。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.fetched_at IS '本地抓取并写入数据库的时间戳。';")
    op.execute("COMMENT ON COLUMN market_data.fins_summary.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")


def downgrade() -> None:
    op.drop_index("ix_fins_summary_current_fiscal_year_end_code", table_name="fins_summary", schema="market_data")
    op.drop_index("ix_fins_summary_document_type_disclosure_date", table_name="fins_summary", schema="market_data")
    op.drop_index("ix_fins_summary_code_disclosure_date", table_name="fins_summary", schema="market_data")
    op.drop_table("fins_summary", schema="market_data")
