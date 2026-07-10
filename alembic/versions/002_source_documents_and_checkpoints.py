"""source_documents y columnas de checkpoint reanudable

Revision ID: 002_source_docs
Revises: 001_initial
Create Date: 2026-07-10
"""

from __future__ import annotations

import os
import sys

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from alegra_etl.config import get_settings

revision = "002_source_docs"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema

    op.create_table(
        "source_documents",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("resource_name", sa.String(length=100), nullable=False),
        sa.Column("alegra_id", sa.String(length=50), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=30), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("metadata_json", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("company_id", "resource_name", "alegra_id", name="uq_source_document"),
        schema=schema,
    )
    op.create_index("ix_source_documents_company_id", "source_documents", ["company_id"], schema=schema)
    op.create_index("ix_source_documents_resource_name", "source_documents", ["resource_name"], schema=schema)
    op.create_index("ix_source_documents_alegra_id", "source_documents", ["alegra_id"], schema=schema)
    op.create_index("ix_source_documents_document_date", "source_documents", ["document_date"], schema=schema)

    op.add_column("sync_checkpoints", sa.Column("status", sa.String(length=30), server_default="pending", nullable=False), schema=schema)
    op.add_column("sync_checkpoints", sa.Column("backfill_start_date", sa.Date(), nullable=True), schema=schema)
    op.add_column("sync_checkpoints", sa.Column("backfill_end_date", sa.Date(), nullable=True), schema=schema)
    op.add_column("sync_checkpoints", sa.Column("cursor_date", sa.Date(), nullable=True), schema=schema)
    op.add_column("sync_checkpoints", sa.Column("cursor_offset", sa.Integer(), server_default="0", nullable=False), schema=schema)
    op.add_column("sync_checkpoints", sa.Column("backfill_completed_at", sa.DateTime(timezone=True), nullable=True), schema=schema)


def downgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    op.drop_column("sync_checkpoints", "backfill_completed_at", schema=schema)
    op.drop_column("sync_checkpoints", "cursor_offset", schema=schema)
    op.drop_column("sync_checkpoints", "cursor_date", schema=schema)
    op.drop_column("sync_checkpoints", "backfill_end_date", schema=schema)
    op.drop_column("sync_checkpoints", "backfill_start_date", schema=schema)
    op.drop_column("sync_checkpoints", "status", schema=schema)
    op.drop_table("source_documents", schema=schema)
