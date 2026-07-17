"""Columnas de numeración y factura electrónica en fact_sales_invoice

Revision ID: 003_invoice_numbering
Revises: 002_source_docs
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
from helpers import add_column_if_missing, column_exists, create_index_if_missing

revision = "003_invoice_numbering"
down_revision = "002_source_docs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    print(f"[migrate] 003: columnas numeración en {schema}.fact_sales_invoice", flush=True)

    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("number_template_id", sa.String(length=50), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("number_template_name", sa.String(length=200), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("number_prefix", sa.String(length=50), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("number_value", sa.String(length=50), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("is_electronic", sa.Boolean(), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        "fact_sales_invoice",
        sa.Column("cufe", sa.String(length=200), nullable=True),
        schema=schema,
    )
    create_index_if_missing(
        "ix_fact_sales_invoice_number_template_id",
        "fact_sales_invoice",
        ["number_template_id"],
        schema=schema,
    )
    create_index_if_missing(
        "ix_fact_sales_invoice_number_prefix",
        "fact_sales_invoice",
        ["number_prefix"],
        schema=schema,
    )
    create_index_if_missing(
        "ix_fact_sales_invoice_is_electronic",
        "fact_sales_invoice",
        ["is_electronic"],
        schema=schema,
    )

    # Backfill no destructivo desde raw_json ya persistido.
    op.execute(
        sa.text(
            f"""
            UPDATE "{schema}".fact_sales_invoice
            SET
              number_template_id = COALESCE(
                number_template_id,
                raw_json->'numberTemplate'->>'id'
              ),
              number_template_name = COALESCE(
                number_template_name,
                raw_json->'numberTemplate'->>'name',
                raw_json->'numberTemplate'->>'documentName'
              ),
              number_prefix = COALESCE(
                number_prefix,
                NULLIF(raw_json->'numberTemplate'->>'prefix', '')
              ),
              number_value = COALESCE(
                number_value,
                raw_json->'numberTemplate'->>'number'
              ),
              cufe = COALESCE(
                cufe,
                NULLIF(raw_json->>'cufe', ''),
                NULLIF(raw_json->>'CUFE', ''),
                NULLIF(raw_json->'stamp'->>'cufe', ''),
                NULLIF(raw_json->'stamp'->>'uuid', '')
              ),
              is_electronic = COALESCE(
                is_electronic,
                CASE
                  WHEN lower(COALESCE(raw_json->'numberTemplate'->>'isElectronic', ''))
                       IN ('true', '1', 'yes') THEN TRUE
                  WHEN lower(COALESCE(raw_json->'numberTemplate'->>'isElectronic', ''))
                       IN ('false', '0', 'no') THEN FALSE
                  WHEN lower(COALESCE(raw_json->>'isElectronic', ''))
                       IN ('true', '1', 'yes') THEN TRUE
                  WHEN lower(COALESCE(raw_json->>'isElectronic', ''))
                       IN ('false', '0', 'no') THEN FALSE
                  WHEN COALESCE(
                        NULLIF(raw_json->>'cufe', ''),
                        NULLIF(raw_json->'stamp'->>'cufe', '')
                      ) IS NOT NULL THEN TRUE
                  ELSE NULL
                END
              )
            """
        )
    )
    print("[migrate] 003: backfill desde raw_json OK", flush=True)


def downgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    for index_name in (
        "ix_fact_sales_invoice_is_electronic",
        "ix_fact_sales_invoice_number_prefix",
        "ix_fact_sales_invoice_number_template_id",
    ):
        try:
            op.drop_index(index_name, table_name="fact_sales_invoice", schema=schema)
        except Exception:
            pass
    for column in (
        "cufe",
        "is_electronic",
        "number_value",
        "number_prefix",
        "number_template_name",
        "number_template_id",
    ):
        if column_exists("fact_sales_invoice", column, schema):
            op.drop_column("fact_sales_invoice", column, schema=schema)
