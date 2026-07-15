"""Columna changes (diff) en webhook_events

Revision ID: 004_webhook_changes
Revises: 003_invoice_numbering
Create Date: 2026-07-15
"""

from __future__ import annotations

import os
import sys

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from alegra_etl.config import get_settings

revision = "004_webhook_changes"
down_revision = "003_invoice_numbering"
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    print(f"[migrate] 004: columna changes en {schema}.webhook_events", flush=True)
    op.add_column(
        "webhook_events",
        sa.Column("changes", JSONB(), nullable=True),
        schema=schema,
    )


def downgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    op.drop_column("webhook_events", "changes", schema=schema)
