"""add daily bar revision tracking

Revision ID: 20260318_0005
Revises: 20260318_0004
Create Date: 2026-03-18 00:40:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260318_0005"
down_revision = "20260318_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "equity_daily_bar",
        sa.Column("has_source_revision", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="market_data",
    )
    op.add_column(
        "equity_daily_bar",
        sa.Column("source_revision_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        schema="market_data",
    )
    op.add_column(
        "equity_daily_bar",
        sa.Column("last_source_revision_at", sa.DateTime(timezone=True), nullable=True),
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_column("equity_daily_bar", "last_source_revision_at", schema="market_data")
    op.drop_column("equity_daily_bar", "source_revision_count", schema="market_data")
    op.drop_column("equity_daily_bar", "has_source_revision", schema="market_data")
