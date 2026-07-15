"""Replay de source_documents hacia tablas tipadas."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.config import Settings
from alegra_etl.db.models import SourceDocument
from alegra_etl.pipeline.typed_loader import transform_and_load_resilient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 200


def replay_source_documents(
    session: Session,
    settings: Settings,
    *,
    resource_name: str,
    alegra_id: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    resource = resource_by_name(resource_name)
    if not resource or not resource.has_typed_loader:
        raise ValueError(f"Recurso {resource_name} no tiene loader tipado")

    query = (
        session.query(SourceDocument)
        .filter_by(company_id=settings.company_id, resource_name=resource_name)
        .filter(SourceDocument.deleted_at.is_(None))
        .order_by(SourceDocument.id.asc())
    )
    if alegra_id:
        query = query.filter(SourceDocument.alegra_id == alegra_id)
    if limit:
        query = query.limit(limit)

    rows = query.all()
    if dry_run:
        return {"dry_run": True, "would_process": len(rows), "resource": resource_name}

    total_typed = 0
    total_skipped = 0
    chunks = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        records = [row.payload for row in chunk if isinstance(row.payload, dict)]
        if not records:
            continue
        loaded, skipped = transform_and_load_resilient(
            session, resource, records, settings.company_id
        )
        total_typed += loaded
        total_skipped += skipped
        chunks += 1
        session.commit()

    return {
        "resource": resource_name,
        "documents": len(rows),
        "chunks": chunks,
        "typed_upserted": total_typed,
        "skipped": total_skipped,
    }
