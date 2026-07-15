"""Auditoría y reparación de checkpoints de backfill."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import (
    ResourceDefinition,
    ResourcePriority,
    SyncStrategy,
    get_backfill_resources,
    resource_by_name,
)
from alegra_etl.config import Settings
from alegra_etl.db.models import SyncCheckpoint

_BOGOTA = ZoneInfo("America/Bogota")

CRITICAL_RESOURCES = frozenset(
    {"invoices", "bills", "payments-income", "credit-notes", "items", "contacts"}
)


def _today_local() -> date:
    return datetime.now(_BOGOTA).date()


def checkpoint_issues(
    checkpoint: SyncCheckpoint,
    resource: ResourceDefinition | None,
    *,
    today: date | None = None,
) -> list[str]:
    """Devuelve lista de invariantes violadas (vacía = consistente)."""
    issues: list[str] = []
    today = today or _today_local()

    if checkpoint.status == "skipped":
        return issues

    if checkpoint.status != "completed":
        return issues

    if resource is None:
        issues.append("unknown_resource")
        return issues

    if resource.strategy == SyncStrategy.DATE_WINDOW:
        if checkpoint.backfill_start_date is None:
            issues.append("missing_backfill_start_date")
        if checkpoint.backfill_end_date is None:
            issues.append("missing_backfill_end_date")
        if checkpoint.cursor_date is None:
            issues.append("missing_cursor_date")
        elif checkpoint.backfill_end_date and checkpoint.cursor_date <= checkpoint.backfill_end_date:
            issues.append("cursor_not_past_end")
        elif checkpoint.backfill_end_date is None and checkpoint.cursor_date is not None:
            issues.append("cursor_not_past_end")
        if checkpoint.backfill_completed_at is None:
            issues.append("missing_backfill_completed_at")
        if getattr(checkpoint, "verified_at", None) is None and resource.name in CRITICAL_RESOURCES:
            issues.append("not_verified")

    if resource.strategy == SyncStrategy.FULL:
        if checkpoint.backfill_completed_at is None and resource.priority in {
            ResourcePriority.CRITICAL,
            ResourcePriority.HIGH,
        }:
            issues.append("full_missing_completed_at")

    return issues


def is_truly_complete(
    checkpoint: SyncCheckpoint,
    resource: ResourceDefinition,
    *,
    today: date | None = None,
) -> bool:
    return checkpoint.status == "completed" and not checkpoint_issues(checkpoint, resource, today=today)


def audit_checkpoints(settings: Settings, session: Session) -> dict[str, Any]:
    """Diagnóstico de todos los checkpoints de backfill."""
    resources = {r.name: r for r in get_backfill_resources(settings)}
    rows = (
        session.query(SyncCheckpoint)
        .filter_by(company_id=settings.company_id)
        .order_by(SyncCheckpoint.resource_name)
        .all()
    )
    report: dict[str, Any] = {
        "company_id": settings.company_id,
        "today": _today_local().isoformat(),
        "resources": {},
        "invalid_completed": [],
        "pending": [],
        "truly_complete": [],
    }
    for row in rows:
        resource = resources.get(row.resource_name) or resource_by_name(row.resource_name)
        issues = checkpoint_issues(row, resource)
        entry = {
            "status": row.status,
            "cursor_date": row.cursor_date.isoformat() if row.cursor_date else None,
            "cursor_offset": row.cursor_offset,
            "backfill_start_date": row.backfill_start_date.isoformat() if row.backfill_start_date else None,
            "backfill_end_date": row.backfill_end_date.isoformat() if row.backfill_end_date else None,
            "backfill_completed_at": row.backfill_completed_at.isoformat() if row.backfill_completed_at else None,
            "verified_at": row.verified_at.isoformat() if row.verified_at else None,
            "issues": issues,
            "truly_complete": is_truly_complete(row, resource) if resource else False,
        }
        report["resources"][row.resource_name] = entry
        if row.status == "pending":
            report["pending"].append(row.resource_name)
        if issues:
            report["invalid_completed"].append(row.resource_name)
        if entry["truly_complete"]:
            report["truly_complete"].append(row.resource_name)

    from alegra_etl.pipeline.completion_gate import global_backfill_status

    aggregate = global_backfill_status(session, settings, resources, rows)
    report["false_completed"] = aggregate["false_completed"]
    report["safe_to_stop_backfill"] = aggregate["safe_to_stop_backfill"]
    report["all_backfill_complete"] = aggregate["all_backfill_complete"]
    report["blockers_by_resource"] = aggregate["blockers_by_resource"]
    return report


def repair_checkpoint(
    checkpoint: SyncCheckpoint,
    resource: ResourceDefinition,
    settings: Settings,
    *,
    reason: str = "manual_repair",
) -> bool:
    """Reabre un checkpoint inconsistente. Retorna True si se modificó."""
    issues = checkpoint_issues(checkpoint, resource)
    if not issues and checkpoint.status != "skipped":
        return False

    today = _today_local()
    backfill_start = date.fromisoformat(settings.backfill_start_date)
    meta = dict(checkpoint.metadata_json or {})
    meta["repaired_at"] = datetime.now(UTC).isoformat()
    meta["repair_reason"] = reason
    meta["previous_status"] = checkpoint.status
    meta["previous_cursor"] = checkpoint.cursor_date.isoformat() if checkpoint.cursor_date else None

    checkpoint.status = "pending"
    checkpoint.backfill_start_date = checkpoint.backfill_start_date or backfill_start
    checkpoint.backfill_end_date = today
    if resource.strategy == SyncStrategy.DATE_WINDOW:
        checkpoint.cursor_date = checkpoint.cursor_date or checkpoint.backfill_start_date or backfill_start
        if "cursor_not_past_end" in issues or "missing_cursor_date" in issues:
            checkpoint.cursor_date = checkpoint.backfill_start_date or backfill_start
    checkpoint.cursor_offset = 0
    checkpoint.backfill_completed_at = None
    checkpoint.verified_at = None
    checkpoint.backfill_generation = (checkpoint.backfill_generation or 1) + 1
    checkpoint.metadata_json = meta
    return True


def repair_all_invalid(settings: Settings, session: Session, *, apply: bool = False) -> dict[str, Any]:
    """Audita y opcionalmente repara checkpoints inválidos."""
    resources = {r.name: r for r in get_backfill_resources(settings)}
    rows = session.query(SyncCheckpoint).filter_by(company_id=settings.company_id).all()
    repaired: list[str] = []
    skipped: list[str] = []
    for row in rows:
        resource = resources.get(row.resource_name) or resource_by_name(row.resource_name)
        if not resource:
            skipped.append(row.resource_name)
            continue
        issues = checkpoint_issues(row, resource)
        if not issues:
            continue
        if apply:
            if repair_checkpoint(row, resource, settings, reason="bulk_repair"):
                repaired.append(row.resource_name)
        else:
            repaired.append(row.resource_name)

    result = {
        "apply": apply,
        "would_repair": repaired if not apply else repaired,
        "repaired": repaired if apply else [],
        "skipped_unknown": skipped,
    }
    if apply:
        session.flush()
    return result
