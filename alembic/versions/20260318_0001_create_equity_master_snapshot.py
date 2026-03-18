"""create equity master snapshot table

Revision ID: 20260318_0001
Revises:
Create Date: 2026-03-18 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260318_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS market_data"))

    op.create_table(
        "equity_master_snapshot",
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("code", sa.String(length=5), nullable=False),
        sa.Column("company_name", sa.Text(), nullable=False),
        sa.Column("company_name_en", sa.Text(), nullable=True),
        sa.Column("sector17_code", sa.String(length=2), nullable=False),
        sa.Column("sector17_name", sa.Text(), nullable=False),
        sa.Column("sector33_code", sa.String(length=4), nullable=False),
        sa.Column("sector33_name", sa.Text(), nullable=False),
        sa.Column("scale_category", sa.Text(), nullable=False),
        sa.Column("market_code", sa.String(length=4), nullable=False),
        sa.Column("market_name", sa.Text(), nullable=False),
        sa.Column("margin_code", sa.String(length=1), nullable=True),
        sa.Column("margin_name", sa.Text(), nullable=True),
        sa.Column(
            "source_api",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'/v2/equities/master'"),
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("as_of_date", "code", name="pk_equity_master_snapshot"),
        sa.CheckConstraint("code ~ '^[0-9]{4,5}$'", name="chk_equity_master_snapshot_code_digits"),
        sa.CheckConstraint(
            "sector17_code ~ '^[0-9]{1,2}$'",
            name="chk_equity_master_snapshot_sector17_digits",
        ),
        sa.CheckConstraint(
            "sector33_code ~ '^[0-9]{1,4}$'",
            name="chk_equity_master_snapshot_sector33_digits",
        ),
        sa.CheckConstraint(
            "market_code ~ '^[0-9]{4}$'",
            name="chk_equity_master_snapshot_market_code_digits",
        ),
        sa.CheckConstraint(
            "margin_code IS NULL OR margin_code IN ('1', '2', '3')",
            name="chk_equity_master_snapshot_margin_code",
        ),
        schema="market_data",
    )

    op.create_index(
        "ix_equity_master_snapshot_code_as_of_date",
        "equity_master_snapshot",
        ["code", "as_of_date"],
        unique=False,
        schema="market_data",
    )
    op.create_index(
        "ix_equity_master_snapshot_market_code_as_of_date",
        "equity_master_snapshot",
        ["market_code", "as_of_date"],
        unique=False,
        schema="market_data",
    )
    op.create_index(
        "ix_equity_master_snapshot_sector33_code_as_of_date",
        "equity_master_snapshot",
        ["sector33_code", "as_of_date"],
        unique=False,
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_equity_master_snapshot_sector33_code_as_of_date",
        table_name="equity_master_snapshot",
        schema="market_data",
    )
    op.drop_index(
        "ix_equity_master_snapshot_market_code_as_of_date",
        table_name="equity_master_snapshot",
        schema="market_data",
    )
    op.drop_index(
        "ix_equity_master_snapshot_code_as_of_date",
        table_name="equity_master_snapshot",
        schema="market_data",
    )
    op.drop_table("equity_master_snapshot", schema="market_data")
