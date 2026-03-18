"""relax sector code constraints

Revision ID: 20260318_0002
Revises: 20260318_0001
Create Date: 2026-03-18 00:10:00
"""
from __future__ import annotations

from alembic import op


revision = "20260318_0002"
down_revision = "20260318_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
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
    op.create_check_constraint(
        "chk_equity_master_snapshot_sector17_digits",
        "equity_master_snapshot",
        "sector17_code ~ '^[0-9]{2}$'",
        schema="market_data",
    )
    op.create_check_constraint(
        "chk_equity_master_snapshot_sector33_digits",
        "equity_master_snapshot",
        "sector33_code ~ '^[0-9]{4}$'",
        schema="market_data",
    )
