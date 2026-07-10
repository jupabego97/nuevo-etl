"""Servicio FastAPI para webhooks Alegra."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from alegra_etl.config import get_settings
from alegra_etl.db.session import session_scope
from alegra_etl.logging import setup_logging
from alegra_etl.pipeline.webhook_processor import WebhookProcessor

logger = logging.getLogger(__name__)


class WebhookPayload(BaseModel):
    event: str | None = None
    id: str | None = None
    data: dict[str, Any] | None = None


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_json)
    app = FastAPI(title="Alegra ETL Webhooks", version="1.0.0")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks/alegra")
    async def receive_webhook(
        request: Request,
        x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    ) -> dict[str, str]:
        if x_webhook_secret != settings.webhook_secret.get_secret_value():
            raise HTTPException(status_code=401, detail="Webhook no autorizado")

        body = await request.json()
        event_type = body.get("event") or body.get("type") or "unknown"
        payload = body.get("data") if isinstance(body.get("data"), dict) else body

        with session_scope(settings) as session:
            processor = WebhookProcessor(settings, session)
            event = processor.enqueue_event(event_type, payload)
            session.commit()
            event_id = str(event.id)

        asyncio.create_task(_process_event_async(event_id))
        return {"status": "accepted", "event_id": event_id}

    return app


async def _process_event_async(event_id: str) -> None:
    settings = get_settings()
    with session_scope(settings) as session:
        processor = WebhookProcessor(settings, session)
        await processor.process_pending(limit=10)


app = create_app()
