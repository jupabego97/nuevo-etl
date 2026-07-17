"""Evidencia de reconciliación por unidad de backfill.

Revision ID: 006_backfill_evidence
Revises: 005_backfill_integrity
Create Date: 2026-07-15
"""

from __future__ import annotations

import os
import sys

import sqlalchemy as sa
from alembic import op

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from alegra_etl.config import get_settings
from helpers import add_column_if_missing, column_exists

revision = "006_backfill_evidence"
down_revision = "005_backfill_integrity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = get_settings().db_schema
    table = "backfill_work_items"
    columns = (
        ("api_records", sa.Integer(), True),
        ("api_distinct_ids", sa.Integer(), True),
        ("source_distinct_ids", sa.Integer(), True),
        ("typed_distinct_ids", sa.Integer(), True),
        ("confirmed_offset", sa.Integer(), False),
    )
    for name, column_type, nullable in columns:
        add_column_if_missing(
            table,
            sa.Column(
                name,
                column_type,
                nullable=nullable,
                server_default="0" if not nullable else None,
            ),
            schema=schema,
        )


def downgrade() -> None:
    schema = get_settings().db_schema
    for name in (
        "confirmed_offset",
        "typed_distinct_ids",
        "source_distinct_ids",
        "api_distinct_ids",
        "api_records",
    ):
        if column_exists("backfill_work_items", name, schema):
            op.drop_column("backfill_work_items", name, schema=schema)
