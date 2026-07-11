"""Carga idempotente mediante UPSERT."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _normalize_row_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Asegura el mismo conjunto de claves en todas las filas del lote."""
    if not rows:
        return rows
    all_keys: set[str] = set()
    for row in rows:
        all_keys.update(row.keys())
    return [{key: row.get(key) for key in all_keys} for row in rows]


def upsert_rows(
    session: Session,
    table: Any,
    rows: Iterable[dict[str, Any]],
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> int:
    rows_list = _normalize_row_keys(list(rows))
    if not rows_list:
        return 0

    count = 0
    chunk_size = 500
    for i in range(0, len(rows_list), chunk_size):
        chunk = rows_list[i : i + chunk_size]
        stmt = insert(table).values(chunk)
        excluded = stmt.excluded
        update_cols = update_columns or [c for c in chunk[0].keys() if c not in conflict_columns]
        set_map = {col: getattr(excluded, col) for col in update_cols if hasattr(table.c, col)}
        if set_map:
            stmt = stmt.on_conflict_do_update(index_elements=conflict_columns, set_=set_map)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
        result = session.execute(stmt)
        # psycopg puede devolver rowcount=-1; en Python -1 es truthy y rompe el contador.
        rowcount = result.rowcount
        count += len(chunk) if rowcount is None or rowcount < 0 else rowcount
    return count
