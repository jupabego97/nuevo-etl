"""Esquema inicial alegra_etl

Revision ID: 001_initial
Revises:
Create Date: 2026-07-10
"""

from __future__ import annotations

import os
import sys

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
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{settings.db_schema}"')
    configure_schema(settings.db_schema)
    bind = op.get_bind()
    # Crea todas las tablas del modelo bajo el esquema aislado.
    Base.metadata.create_all(bind=bind, checkfirst=True)


def downgrade() -> None:
    settings = get_settings()
    configure_schema(settings.db_schema)
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind, checkfirst=True)
    op.execute(f'DROP SCHEMA IF EXISTS "{settings.db_schema}" CASCADE')
