"""Gestión de unidades de trabajo de backfill."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy, get_backfill_resources
from alegra_etl.config import Settings
from alegra_etl.db.models.backfill import BackfillWorkItem
from alegra_etl.db.models import SyncCheckpoint

_BOGOTA = ZoneInfo("America/Bogota")
LEASE_MINUTES = 15


def _today_local() -> date:
    return datetime.now(_BOGOTA).date()


def work_key_for_date(target: date) -> str:
    return target.isoformat()


def work_key_for_offset(offset: int) -> str:
    return f"offset:{offset}"


def seed_work_items(
    settings: Settings,
    session: Session,
    resource: ResourceDefinition,
    checkpoint: SyncCheckpoint,
) -> int:
    """Genera work items pendientes para el rango del checkpoint."""
    start = checkpoint.backfill_start_date or date.fromisoformat(settings.backfill_start_date)
    end = checkpoint.backfill_end_date or _today_local()
    created = 0

    if resource.strategy == SyncStrategy.DATE_WINDOW:
        current = checkpoint.cursor_date or start
        if current < start:
            current = start
        while current <= end:
            key = work_key_for_date(current)
            stmt = (
                insert(BackfillWorkItem)
                .values(
                    company_id=settings.company_id,
                    resource_name=resource.name,
                    work_key=key,
                    work_date=current,
                    status="pending",
                )
                .on_conflict_do_nothing(
                    index_elements=["company_id", "resource_name", "work_key"]
                )
            )
            result = session.execute(stmt)
            if result.rowcount and result.rowcount > 0:
                created += 1
            current += timedelta(days=1)
    else:
        # FULL: una unidad por offset inicial (paginación continúa dentro del worker)
        key = work_key_for_offset(checkpoint.cursor_offset or 0)
        stmt = (
            insert(BackfillWorkItem)
            .values(
                company_id=settings.company_id,
                resource_name=resource.name,
                work_key=key,
                work_date=None,
                start_offset=checkpoint.cursor_offset or 0,
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["company_id", "resource_name", "work_key"])
        )
        result = session.execute(stmt)
        if result.rowcount and result.rowcount > 0:
            created += 1

    session.flush()
    return created


def seed_all_pending_work(settings: Settings, session: Session) -> dict[str, int]:
    """Genera work items para todos los recursos de backfill no completados."""
    counts: dict[str, int] = {}
    for resource in get_backfill_resources(settings):
        checkpoint = (
            session.query(SyncCheckpoint)
            .filter_by(company_id=settings.company_id, resource_name=resource.name)
            .one_or_none()
        )
        if checkpoint is None or checkpoint.status == "completed":
            continue
        if checkpoint.backfill_start_date is None:
            checkpoint.backfill_start_date = date.fromisoformat(settings.backfill_start_date)
        if checkpoint.backfill_end_date is None:
            checkpoint.backfill_end_date = _today_local()
        counts[resource.name] = seed_work_items(settings, session, resource, checkpoint)
    return counts


def release_stale_leases(session: Session, company_id: int) -> int:
    now = datetime.now(UTC)
    rows = (
        session.query(BackfillWorkItem)
        .filter(
            BackfillWorkItem.company_id == company_id,
            BackfillWorkItem.status == "running",
            BackfillWorkItem.leased_until < now,
        )
        .all()
    )
    for row in rows:
        row.status = "pending"
        row.lease_owner = None
        row.leased_until = None
    return len(rows)


def claim_work_items(
    session: Session,
    *,
    company_id: int,
    resource_name: str | None,
    limit: int,
    owner: str,
) -> list[BackfillWorkItem]:
    """Reclama work items con FOR UPDATE SKIP LOCKED."""
    now = datetime.now(UTC)
    lease_until = now + timedelta(minutes=LEASE_MINUTES)
    release_stale_leases(session, company_id)

    query = (
        session.query(BackfillWorkItem)
        .filter(
            BackfillWorkItem.company_id == company_id,
            BackfillWorkItem.status == "pending",
        )
        .order_by(BackfillWorkItem.work_date.asc().nullsfirst(), BackfillWorkItem.id.asc())
    )
    if resource_name:
        query = query.filter(BackfillWorkItem.resource_name == resource_name)

    # SKIP LOCKED requiere with_for_update
    pending = query.with_for_update(skip_locked=True).limit(limit).all()
    for item in pending:
        item.status = "running"
        item.lease_owner = owner
        item.leased_until = lease_until
        item.attempts += 1
    session.flush()
    return pending


def mark_work_verified(item: BackfillWorkItem, metrics: dict[str, Any], run_id: uuid.UUID) -> None:
    item.status = "verified"
    item.verified_at = datetime.now(UTC)
    item.records_extracted = int(metrics.get("extracted", 0))
    item.records_source = int(metrics.get("source_upserted", 0))
    item.records_typed = int(metrics.get("typed_upserted", 0))
    item.last_run_id = run_id
    item.lease_owner = None
    item.leased_until = None
    item.error_message = None


def mark_work_failed(item: BackfillWorkItem, error: str) -> None:
    item.status = "failed" if item.attempts >= 5 else "pending"
    item.error_message = error[:2000]
    item.lease_owner = None
    item.leased_until = None


def work_progress(session: Session, company_id: int, resource_name: str | None = None) -> dict[str, Any]:
    query = session.query(BackfillWorkItem).filter(BackfillWorkItem.company_id == company_id)
    if resource_name:
        query = query.filter(BackfillWorkItem.resource_name == resource_name)
    rows = query.all()
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row.status] = by_status.get(row.status, 0) + 1
    return {
        "total": len(rows),
        "by_status": by_status,
        "pending": by_status.get("pending", 0),
        "verified": by_status.get("verified", 0),
    }


def all_work_verified(session: Session, company_id: int, resource_name: str) -> bool:
    pending = (
        session.query(BackfillWorkItem)
        .filter(
            BackfillWorkItem.company_id == company_id,
            BackfillWorkItem.resource_name == resource_name,
            BackfillWorkItem.status.in_(("pending", "running", "failed")),
        )
        .count()
    )
    return pending == 0
