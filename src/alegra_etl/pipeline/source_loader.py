"""Carga canónica de documentos fuente."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from alegra_etl.alegra.client import hash_payload
from alegra_etl.alegra.parsers import resolve_tax_id
from alegra_etl.alegra.resources import ResourceDefinition
from alegra_etl.db.models.canonical import SourceDocument


def _extract_document_date(record: dict[str, Any]) -> date | None:
    for key in ("date", "invoiceDate", "billDate", "paymentDate", "createdAt"):
        value = record.get(key)
        if not value:
            continue
        if isinstance(value, datetime):
            return value.date()
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
    return None


def _extract_status(record: dict[str, Any]) -> str | None:
    status = record.get("status")
    return str(status) if status is not None else None


def _resolve_alegra_id(record: dict[str, Any], resource_name: str) -> str | None:
    """Obtiene un id estable; endpoints singleton (company) pueden no traer `id`."""
    if resource_name == "taxes":
        return resolve_tax_id(record)
    for key in ("id", "idCompany", "companyId", "identification"):
        value = record.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)
    # Documento único por recurso (p.ej. /company)
    if resource_name == "company":
        return "singleton"
    return None


def upsert_source_documents(
    session: Session,
    *,
    company_id: int,
    resource: ResourceDefinition,
    records: list[dict[str, Any]],
    run_id: uuid.UUID,
) -> int:
    if not records:
        return 0

    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for record in records:
        alegra_id = _resolve_alegra_id(record, resource.name)
        if alegra_id is None:
            continue
        rows.append(
            {
                "company_id": company_id,
                "resource_name": resource.name,
                "alegra_id": alegra_id,
                "payload": record,
                "payload_hash": hash_payload(record),
                "document_date": _extract_document_date(record),
                "status": _extract_status(record),
                "deleted_at": None,
                "last_run_id": run_id,
                "last_seen_at": now,
            }
        )

    if not rows:
        return 0

    count = 0
    chunk_size = 200
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        stmt = insert(SourceDocument.__table__).values(chunk)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=["company_id", "resource_name", "alegra_id"],
            set_={
                "payload": excluded.payload,
                "payload_hash": excluded.payload_hash,
                "document_date": excluded.document_date,
                "status": excluded.status,
                "deleted_at": None,
                "last_run_id": excluded.last_run_id,
                "last_seen_at": excluded.last_seen_at,
            },
        )
        result = session.execute(stmt)
        rowcount = result.rowcount
        count += len(chunk) if rowcount is None or rowcount < 0 else rowcount
    return count


def get_source_document_payload(
    session: Session,
    *,
    company_id: int,
    resource_name: str,
    alegra_id: str,
) -> dict[str, Any] | None:
    """Devuelve el payload canónico actual, o None si no existe."""
    doc = (
        session.query(SourceDocument)
        .filter_by(
            company_id=company_id,
            resource_name=resource_name,
            alegra_id=alegra_id,
        )
        .one_or_none()
    )
    if not doc or not isinstance(doc.payload, dict):
        return None
    return doc.payload


def soft_delete_source_document(
    session: Session,
    *,
    company_id: int,
    resource_name: str,
    alegra_id: str,
) -> None:
    doc = (
        session.query(SourceDocument)
        .filter_by(
            company_id=company_id,
            resource_name=resource_name,
            alegra_id=alegra_id,
        )
        .one_or_none()
    )
    if doc:
        doc.deleted_at = datetime.now(UTC)
        doc.status = "deleted"
