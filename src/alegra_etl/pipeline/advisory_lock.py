"""Bloqueo advisory PostgreSQL para evitar crons solapados."""

from __future__ import annotations

import hashlib

from sqlalchemy import text
from sqlalchemy.orm import Session


def _lock_key(resource_name: str) -> int:
    digest = hashlib.sha256(resource_name.encode()).hexdigest()
    return int(digest[:15], 16) % (2**31 - 1)


def try_acquire_backfill_lock(session: Session, company_id: int, resource_name: str) -> bool:
    key2 = _lock_key(resource_name)
    result = session.execute(
        text("SELECT pg_try_advisory_lock(:cid, :key)"),
        {"cid": company_id, "key": key2},
    ).scalar()
    return bool(result)


def release_backfill_lock(session: Session, company_id: int, resource_name: str) -> None:
    key2 = _lock_key(resource_name)
    session.execute(
        text("SELECT pg_advisory_unlock(:cid, :key)"),
        {"cid": company_id, "key": key2},
    )
