"""Utilidades idempotentes para migraciones Alembic."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


def _inspector():
    return inspect(op.get_bind())


def table_exists(table: str, schema: str) -> bool:
    return table in _inspector().get_table_names(schema=schema)


def column_exists(table: str, column: str, schema: str) -> bool:
    if not table_exists(table, schema):
        return False
    return any(col["name"] == column for col in _inspector().get_columns(table, schema=schema))


def index_exists(table: str, index_name: str, schema: str) -> bool:
    if not table_exists(table, schema):
        return False
    return any(idx["name"] == index_name for idx in _inspector().get_indexes(table, schema=schema))


def add_column_if_missing(
    table: str,
    column: sa.Column,
    *,
    schema: str,
) -> None:
    if column_exists(table, column.name, schema):
        print(f"[migrate] skip column {schema}.{table}.{column.name}", flush=True)
        return
    op.add_column(table, column, schema=schema)


def create_index_if_missing(
    index_name: str,
    table: str,
    columns: list[str],
    *,
    schema: str,
) -> None:
    if index_exists(table, index_name, schema):
        print(f"[migrate] skip index {schema}.{index_name}", flush=True)
        return
    op.create_index(index_name, table, columns, schema=schema)


def create_table_if_missing(table_name: str, *args: Any, schema: str, **kwargs: Any) -> bool:
    """Crea la tabla solo si no existe. Devuelve True si la creó."""
    if table_exists(table_name, schema):
        print(f"[migrate] skip table {schema}.{table_name}", flush=True)
        return False
    op.create_table(table_name, *args, schema=schema, **kwargs)
    return True
