"""drop equity master check constraints

Revision ID: 20260318_0003
Revises: 20260318_0002
Create Date: 2026-03-18 00:20:00
"""
from __future__ import annotations

from alembic import op


revision = "20260318_0003"
down_revision = "20260318_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "chk_equity_master_snapshot_code_digits",
        "equity_master_snapshot",
        schema="market_data",
        type_="check",
    )
    op.drop_constraint(
        "chk_equity_master_snapshot_sector17_digits",
        "equity_master_snapshot",
        schema="market_data",
        type_="check",
    )
    op.drop_constraint(
        "chk_equity_master_snapshot_sector33_digits",
        "equity_master_snapshot",
        schema="market_data",
        type_="check",
    )
    op.drop_constraint(
        "chk_equity_master_snapshot_market_code_digits",
        "equity_master_snapshot",
        schema="market_data",
        type_="check",
    )
    op.drop_constraint(
        "chk_equity_master_snapshot_margin_code",
        "equity_master_snapshot",
        schema="market_data",
        type_="check",
    )


def downgrade() -> None:
    op.create_check_constraint(
        "chk_equity_master_snapshot_code_digits",
        "equity_master_snapshot",
        "code ~ '^[0-9]{4,5}$'",
        schema="market_data",
    )
    op.create_check_constraint(
        "chk_equity_master_snapshot_sector17_digits",
        "equity_master_snapshot",
        "sector17_code ~ '^[0-9]{1,2}$'",
        schema="market_data",
    )
    op.create_check_constraint(
        "chk_equity_master_snapshot_sector33_digits",
        "equity_master_snapshot",
        "sector33_code ~ '^[0-9]{1,4}$'",
        schema="market_data",
    )
    op.create_check_constraint(
        "chk_equity_master_snapshot_market_code_digits",
        "equity_master_snapshot",
        "market_code ~ '^[0-9]{4}$'",
        schema="market_data",
    )
    op.create_check_constraint(
        "chk_equity_master_snapshot_margin_code",
        "equity_master_snapshot",
        "margin_code IS NULL OR margin_code IN ('1', '2', '3')",
        schema="market_data",
    )
