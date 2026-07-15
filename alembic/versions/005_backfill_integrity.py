"""Backfill work items, parse skips y columnas de verificación

Revision ID: 005_backfill_integrity
Revises: 004_webhook_changes
Create Date: 2026-07-15
"""

from __future__ import annotations

import os
import sys

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from alegra_etl.config import get_settings

revision = "005_backfill_integrity"
down_revision = "004_webhook_changes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    print(f"[migrate] 005: backfill_work_items en {schema}", flush=True)

    op.add_column(
        "sync_checkpoints",
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        schema=schema,
    )
    op.add_column(
        "sync_checkpoints",
        sa.Column("backfill_generation", sa.Integer(), server_default="1", nullable=False),
        schema=schema,
    )

    op.create_table(
        "backfill_work_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("resource_name", sa.String(length=100), nullable=False),
        sa.Column("work_key", sa.String(length=50), nullable=False),
        sa.Column("work_date", sa.Date(), nullable=True),
        sa.Column("start_offset", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=30), server_default="pending", nullable=False),
        sa.Column("records_extracted", sa.Integer(), server_default="0", nullable=False),
        sa.Column("records_source", sa.Integer(), server_default="0", nullable=False),
        sa.Column("records_typed", sa.Integer(), server_default="0", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("company_id", "resource_name", "work_key", name="uq_backfill_work_item"),
        schema=schema,
    )
    op.create_index(
        "ix_backfill_work_items_status",
        "backfill_work_items",
        ["company_id", "resource_name", "status"],
        schema=schema,
    )

    op.create_table(
        "etl_parse_skips",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("resource_name", sa.String(length=100), nullable=False),
        sa.Column("alegra_id", sa.String(length=50), nullable=True),
        sa.Column("reason", sa.String(length=200), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )

    # Reparación one-shot de checkpoints legacy corruptos (invoices, payments, credit-notes).
    op.execute(
        sa.text(
            f"""
            UPDATE "{schema}".sync_checkpoints
            SET
              status = 'pending',
              backfill_start_date = COALESCE(backfill_start_date, DATE '2022-01-01'),
              backfill_end_date = COALESCE(backfill_end_date, CURRENT_DATE),
              cursor_date = COALESCE(cursor_date, COALESCE(backfill_start_date, DATE '2022-01-01')),
              cursor_offset = 0,
              backfill_completed_at = NULL,
              verified_at = NULL,
              backfill_generation = COALESCE(backfill_generation, 1) + 1,
              metadata_json = COALESCE(metadata_json, '{{}}'::jsonb)
                || jsonb_build_object(
                  'repaired_at', NOW()::text,
                  'repair_reason', 'premature_completed_legacy'
                )
            WHERE status = 'completed'
              AND resource_name IN ('invoices', 'payments-income', 'credit-notes')
              AND (
                backfill_start_date IS NULL
                OR backfill_end_date IS NULL
                OR cursor_date IS NULL
                OR cursor_date <= backfill_end_date
              )
            """
        )
    )
    print("[migrate] 005: reparación checkpoints legacy OK", flush=True)


def downgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    op.drop_table("etl_parse_skips", schema=schema)
    op.drop_index("ix_backfill_work_items_status", table_name="backfill_work_items", schema=schema)
    op.drop_table("backfill_work_items", schema=schema)
    op.drop_column("sync_checkpoints", "backfill_generation", schema=schema)
    op.drop_column("sync_checkpoints", "verified_at", schema=schema)
