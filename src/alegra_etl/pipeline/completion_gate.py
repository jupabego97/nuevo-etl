"""Condiciones obligatorias antes de declarar un backfill como completo."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy
from alegra_etl.config import Settings
from alegra_etl.db.models import EtlParseSkip, SyncCheckpoint
from alegra_etl.db.models.backfill import BackfillWorkItem
from alegra_etl.pipeline.checkpoint_integrity import checkpoint_issues
from alegra_etl.pipeline.resource_coverage import count_source_ids, count_typed_ids


def backfill_completion_blockers(
    session: Session,
    settings: Settings,
    checkpoint: SyncCheckpoint,
    resource: ResourceDefinition,
    *,
    today: date | None = None,
) -> list[str]:
    """Lista de razones por las que NO se puede cerrar el histórico del recurso."""
    blockers = list(checkpoint_issues(checkpoint, resource, today=today))
    if checkpoint.status == "pending":
        blockers.append("checkpoint_pending")

    work_items = (
        session.query(BackfillWorkItem)
        .filter(
            BackfillWorkItem.company_id == settings.company_id,
            BackfillWorkItem.resource_name == resource.name,
        )
        .all()
    )
    if not work_items:
        blockers.append("work_items_missing")
    else:
        incomplete_work = [
            item
            for item in work_items
            if item.status != "verified"
            or item.verified_at is None
            or item.error_message is not None
        ]
        if incomplete_work:
            blockers.append(f"work_items_incomplete:{len(incomplete_work)}")

    metadata = checkpoint.metadata_json or {}
    if metadata.get("reconciliation_verified_generation") != checkpoint.backfill_generation:
        blockers.append("reconciliation_not_verified_for_generation")

    if resource.strategy == SyncStrategy.DATE_WINDOW:
        start = checkpoint.backfill_start_date
        end = checkpoint.backfill_end_date
        if start and end:
            source_count = count_source_ids(session, settings.company_id, resource.name, start, end)
            typed_count = count_typed_ids(session, settings.company_id, resource.name, start, end)
            if resource.has_typed_loader and source_count != typed_count:
                blockers.append(f"source_typed_mismatch:source={source_count},typed={typed_count}")

    if resource.has_typed_loader:
        skip_count = (
            session.query(EtlParseSkip)
            .filter_by(company_id=settings.company_id, resource_name=resource.name)
            .count()
        )
        if skip_count:
            blockers.append(f"parse_skips:{skip_count}")

    return blockers


def can_mark_backfill_completed(
    session: Session,
    settings: Settings,
    checkpoint: SyncCheckpoint,
    resource: ResourceDefinition,
    *,
    today: date | None = None,
) -> bool:
    # La completitud histórica es una garantía de integridad: no se puede
    # degradar a un cierre estructural mediante una variable de entorno.
    return (
        len(backfill_completion_blockers(session, settings, checkpoint, resource, today=today)) == 0
    )


def global_backfill_status(
    session: Session,
    settings: Settings,
    resources: dict[str, ResourceDefinition],
    checkpoints: list[SyncCheckpoint],
) -> dict[str, Any]:
    """Estado agregado para backfill-status / gate de Histórico completo."""
    by_name = {cp.resource_name: cp for cp in checkpoints}
    pending: list[str] = []
    unsupported: list[str] = []
    false_completed: list[str] = []
    truly_complete: list[str] = []
    blockers_by_resource: dict[str, list[str]] = {}

    for name, resource in resources.items():
        cp = by_name.get(name)
        if cp is None:
            pending.append(name)
            blockers_by_resource[name] = ["checkpoint_missing"]
            continue
        if cp.status == "unsupported":
            unsupported.append(name)
            blockers_by_resource[name] = ["resource_unsupported"]
            continue
        if cp.status == "skipped":
            truly_complete.append(name)
            blockers_by_resource[name] = []
            continue
        if cp.status in {"pending", "running", "failed"}:
            pending.append(name)
        blockers = backfill_completion_blockers(session, settings, cp, resource)
        blockers_by_resource[name] = blockers
        if cp.status == "completed" and blockers:
            false_completed.append(name)
        if cp.status == "completed" and can_mark_backfill_completed(
            session, settings, cp, resource
        ):
            truly_complete.append(name)

    return {
        "pending": pending,
        "unsupported": unsupported,
        "false_completed": false_completed,
        "truly_complete": truly_complete,
        "blockers_by_resource": blockers_by_resource,
        "safe_to_stop_backfill": (
            len(pending) == 0 and len(unsupported) == 0 and len(false_completed) == 0
        ),
        "all_backfill_complete": len(pending) == 0
        and len(unsupported) == 0
        and len(false_completed) == 0
        and all(
            by_name[name].status == "completed"
            and can_mark_backfill_completed(session, settings, by_name[name], resources[name])
            for name in resources
            if name in by_name
        ),
    }
