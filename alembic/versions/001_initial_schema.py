"""Esquema inicial alegra_etl

Revision ID: 001_initial
Revises:
Create Date: 2026-07-10
"""

from __future__ import annotations

import os
import sys
import traceback

from alembic import op

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from alegra_etl.config import get_settings
from alegra_etl.db.models import Base
from alegra_etl.db.models.base import configure_schema

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    settings = get_settings()
    schema = settings.db_schema
    print(f"[migrate] Creando esquema {schema!r}...", flush=True)
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    configure_schema(schema)
    bind = op.get_bind()
    tables = list(Base.metadata.sorted_tables)
    print(f"[migrate] Creando {len(tables)} tablas...", flush=True)
    for index, table in enumerate(tables, start=1):
        qualified = f"{table.schema}.{table.name}" if table.schema else table.name
        print(f"[migrate] ({index}/{len(tables)}) {qualified}", flush=True)
        try:
            table.create(bind=bind, checkfirst=True)
        except Exception:
            print(f"[migrate] ERROR creando {qualified}", flush=True)
            traceback.print_exc()
            raise
    print("[migrate] Esquema inicial listo", flush=True)


def downgrade() -> None:
    settings = get_settings()
    configure_schema(settings.db_schema)
    bind = op.get_bind()
    for table in reversed(Base.metadata.sorted_tables):
        table.drop(bind=bind, checkfirst=True)
    op.execute(f'DROP SCHEMA IF EXISTS "{settings.db_schema}" CASCADE')
