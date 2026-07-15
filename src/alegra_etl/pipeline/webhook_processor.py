"""Procesamiento durable de eventos webhook."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import resource_by_name, resource_for_webhook_event
from alegra_etl.config import Settings
from alegra_etl.db.models import DeadLetterEvent, WebhookEvent
from alegra_etl.pipeline.payload_diff import diff_payloads
from alegra_etl.pipeline.source_loader import (
    get_source_document_payload,
    soft_delete_source_document,
)
from alegra_etl.pipeline.typed_loader import soft_delete_typed_document

logger = logging.getLogger(__name__)

EVENT_TO_RESOURCE = {
    "new-invoice": "invoices",
    "edit-invoice": "invoices",
    "delete-invoice": "invoices",
    "new-bill": "bills",
    "edit-bill": "bills",
    "delete-bill": "bills",
    "new-client": "contacts",
    "edit-client": "contacts",
    "delete-client": "contacts",
    "new-item": "items",
    "edit-item": "items",
    "delete-item": "items",
}

# Claves anidadas que Alegra usa dentro de message.*
_MESSAGE_RESOURCE_KEYS = (
    "item",
    "invoice",
    "bill",
    "client",
    "contact",
    "company",
)


def parse_alegra_webhook_body(body: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Normaliza el body real de Alegra.

    Formato observado::

        {"subject": "edit-item", "message": {"item": {"id": "1899", ...}}}

    También tolera ``event``/``type`` y ``data`` por compatibilidad.
    """
    event_type = (
        body.get("subject")
        or body.get("event")
        or body.get("type")
        or "unknown"
    )
    event_type = str(event_type).strip() or "unknown"

    if isinstance(body.get("data"), dict):
        return event_type, body["data"]

    message = body.get("message")
    if isinstance(message, dict):
        for key in _MESSAGE_RESOURCE_KEYS:
            nested = message.get(key)
            if isinstance(nested, dict) and nested:
                return event_type, nested
        return event_type, message

    return event_type, body


def build_dedupe_key(event_type: str, payload: dict[str, Any]) -> str:
    normalized = json.dumps({"event_type": event_type, "payload": payload}, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode()).hexdigest()


def extract_resource_id(event_type: str, payload: dict[str, Any]) -> str | None:
    if not isinstance(payload, dict):
        return None

    for key in ("id", "resourceId", "invoiceId", "billId", "itemId", "clientId"):
        if payload.get(key) is not None:
            return str(payload[key])

    # Body completo con message.* (eventos antiguos mal parseados)
    message = payload.get("message")
    if isinstance(message, dict):
        for key in _MESSAGE_RESOURCE_KEYS:
            nested = message.get(key)
            if isinstance(nested, dict) and nested.get("id") is not None:
                return str(nested["id"])

    return None


def resolve_event_type(event: WebhookEvent) -> str:
    """Recupera subject/event desde payload si quedó guardado como unknown."""
    if event.event_type and event.event_type != "unknown":
        return event.event_type
    payload = event.payload if isinstance(event.payload, dict) else {}
    recovered = payload.get("subject") or payload.get("event") or payload.get("type")
    if recovered:
        return str(recovered).strip()
    return event.event_type or "unknown"


def resolve_resource_id(event: WebhookEvent, event_type: str) -> str | None:
    if event.resource_id:
        return event.resource_id
    payload = event.payload if isinstance(event.payload, dict) else {}
    return extract_resource_id(event_type, payload)


class WebhookProcessor:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
        from alegra_etl.pipeline.runner import PipelineRunner

        self.runner = PipelineRunner(settings, session)

    def enqueue_event(self, event_type: str, payload: dict[str, Any]) -> WebhookEvent:
        dedupe_key = build_dedupe_key(event_type, payload)
        existing = (
            self.session.query(WebhookEvent)
            .filter(WebhookEvent.dedupe_key == dedupe_key)
            .one_or_none()
        )
        if existing:
            return existing

        stmt = (
            insert(WebhookEvent)
            .values(
                dedupe_key=dedupe_key,
                event_type=event_type,
                resource_id=extract_resource_id(event_type, payload),
                payload=payload,
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["dedupe_key"])
            .returning(WebhookEvent)
        )
        inserted = self.session.execute(stmt).scalar_one_or_none()
        if inserted:
            self.session.flush()
            return inserted

        return (
            self.session.query(WebhookEvent)
            .filter(WebhookEvent.dedupe_key == dedupe_key)
            .one()
        )

    async def process_pending(self, limit: int = 100) -> dict[str, int]:
        pending = (
            self.session.query(WebhookEvent)
            .filter(WebhookEvent.status == "pending")
            .order_by(WebhookEvent.id.asc())
            .limit(limit)
            .all()
        )
        processed = failed = 0
        for event in pending:
            try:
                await self._process_event(event)
                event.status = "processed"
                event.processed_at = datetime.now(UTC)
                event.error_message = None
                processed += 1
            except Exception as exc:
                event.attempts += 1
                event.error_message = str(exc)
                if event.attempts >= 5:
                    event.status = "dead_letter"
                    self.session.add(
                        DeadLetterEvent(
                            source="webhook",
                            reference_id=str(event.id),
                            payload={"event_type": event.event_type, "payload": event.payload},
                            error_message=str(exc),
                            retry_count=event.attempts,
                        )
                    )
                failed += 1
        return {"processed": processed, "failed": failed, "pending": len(pending)}

    def _snapshot_source(
        self,
        *,
        resource_name: str | None,
        resource_id: str | None,
    ) -> dict[str, Any] | None:
        if not resource_name or not resource_id:
            return None
        return get_source_document_payload(
            self.session,
            company_id=self.settings.company_id,
            resource_name=resource_name,
            alegra_id=resource_id,
        )

    async def _process_event(self, event: WebhookEvent) -> None:
        event_type = resolve_event_type(event)
        resource_id = resolve_resource_id(event, event_type)

        # Corrige filas antiguas guardadas como unknown.
        if event.event_type == "unknown" and event_type != "unknown":
            event.event_type = event_type
        if not event.resource_id and resource_id:
            event.resource_id = resource_id

        if event_type.startswith("delete-"):
            lookup_key = event_type.replace("delete-", "edit-", 1)
            resource_name = EVENT_TO_RESOURCE.get(lookup_key)
            if not resource_name:
                resource = resource_for_webhook_event(lookup_key)
                resource_name = resource.name if resource else None
            before = self._snapshot_source(resource_name=resource_name, resource_id=resource_id)
            if resource_name and resource_id:
                soft_delete_source_document(
                    self.session,
                    company_id=self.settings.company_id,
                    resource_name=resource_name,
                    alegra_id=resource_id,
                )
                resource_def = resource_by_name(resource_name)
                if resource_def:
                    soft_delete_typed_document(
                        self.session,
                        resource_def,
                        resource_id,
                        self.settings.company_id,
                    )
            event.changes = diff_payloads(before, None)
            logger.info("Soft-delete aplicado para %s (%s)", event_type, resource_id)
            return

        resource_name = EVENT_TO_RESOURCE.get(event_type)
        resource = resource_by_name(resource_name) if resource_name else resource_for_webhook_event(event_type)
        if not resource:
            raise ValueError(f"No hay recurso mapeado para evento {event_type}")
        if not resource_id:
            raise ValueError(f"Evento {event_type} sin resource_id")

        before = self._snapshot_source(resource_name=resource.name, resource_id=resource_id)
        await self.runner.run_single_resource(resource, resource_id=resource_id)
        # Evita leer estado sucio de identidad SQLAlchemy tras el UPSERT.
        self.session.expire_all()
        after = self._snapshot_source(resource_name=resource.name, resource_id=resource_id)
        event.changes = diff_payloads(before, after)
        if event.changes.get("changed_fields"):
            logger.info(
                "Webhook %s id=%s cambios=%s",
                event_type,
                resource_id,
                event.changes["changed_fields"],
            )
