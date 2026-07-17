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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from alegra_etl.config import get_settings
from helpers import add_column_if_missing, column_exists

revision = "004_webhook_changes"
down_revision = "003_invoice_numbering"
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    print(f"[migrate] 004: columna changes en {schema}.webhook_events", flush=True)
    add_column_if_missing(
        "webhook_events",
        sa.Column("changes", JSONB(), nullable=True),
        schema=schema,
    )


def downgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    if column_exists("webhook_events", "changes", schema):
        op.drop_column("webhook_events", "changes", schema=schema)
