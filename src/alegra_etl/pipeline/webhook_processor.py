"""Procesamiento durable de eventos webhook."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import resource_by_name, resource_for_webhook_event
from alegra_etl.config import Settings
from alegra_etl.db.models import DeadLetterEvent, WebhookEvent
from alegra_etl.pipeline.runner import PipelineRunner

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


def build_dedupe_key(event_type: str, payload: dict[str, Any]) -> str:
    normalized = json.dumps({"event_type": event_type, "payload": payload}, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode()).hexdigest()


def extract_resource_id(event_type: str, payload: dict[str, Any]) -> str | None:
    for key in ("id", "resourceId", "invoiceId", "billId", "itemId", "clientId"):
        if payload.get(key) is not None:
            return str(payload[key])
    match = re.search(r"(\d+)", json.dumps(payload))
    return match.group(1) if match else None


class WebhookProcessor:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
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

    async def _process_event(self, event: WebhookEvent) -> None:
        if event.event_type.endswith("-delete"):
            logger.info("Evento delete registrado para %s (%s)", event.event_type, event.resource_id)
            return

        resource_name = EVENT_TO_RESOURCE.get(event.event_type)
        resource = resource_by_name(resource_name) if resource_name else resource_for_webhook_event(event.event_type)
        if not resource:
            raise ValueError(f"No hay recurso mapeado para evento {event.event_type}")
        if not event.resource_id:
            raise ValueError(f"Evento {event.event_type} sin resource_id")
        await self.runner.run_single_resource(resource, resource_id=event.resource_id)
