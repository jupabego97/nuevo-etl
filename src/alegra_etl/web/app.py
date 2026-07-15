"""Servicio FastAPI para webhooks Alegra."""

from __future__ import annotations

import logging
import secrets
import traceback
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from alegra_etl.config import Settings, get_settings
from alegra_etl.db.session import session_scope
from alegra_etl.logging import setup_logging
from alegra_etl.pipeline.webhook_processor import WebhookProcessor, parse_alegra_webhook_body

logger = logging.getLogger(__name__)


class WebhookPayload(BaseModel):
    event: str | None = None
    id: str | None = None
    data: dict[str, Any] | None = None


def extract_presented_secret(
    request: Request,
    *,
    path_token: str | None = None,
    x_webhook_secret: str | None = None,
) -> str | None:
    """Obtiene el secreto presentado por Alegra o por un cliente de prueba.

    Orden de prioridad:
    1. Header ``X-Webhook-Secret`` (curl / clientes que sí soportan headers)
    2. Query ``token`` o ``secret`` (Alegra solo permite URL sin https://)
    3. Segmento de path ``/webhooks/alegra/{token}``
    """
    if x_webhook_secret and x_webhook_secret.strip():
        return x_webhook_secret.strip()

    for key in ("token", "secret"):
        value = request.query_params.get(key)
        if value and value.strip():
            return value.strip()

    if path_token and path_token.strip():
        return path_token.strip()
    return None


def is_webhook_authorized(presented: str | None, settings: Settings) -> bool:
    expected = settings.webhook_secret.get_secret_value()
    if not expected or presented is None:
        return False
    return secrets.compare_digest(presented, expected)


async def _process_pending_webhooks() -> None:
    """Procesa cola pending; se ejecuta vía BackgroundTasks (fiable en uvicorn)."""
    settings = get_settings()
    try:
        with session_scope(settings) as session:
            processor = WebhookProcessor(settings, session)
            result = await processor.process_pending(limit=20)
            session.commit()
            print(f"[webhook] process_pending result={result}", flush=True)
    except Exception:
        print("[webhook] FALLO process_pending:", flush=True)
        traceback.print_exc()


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_json)
    app = FastAPI(title="Alegra ETL Webhooks", version="1.0.0")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def _handle_webhook(
        request: Request,
        background_tasks: BackgroundTasks,
        *,
        path_token: str | None = None,
        x_webhook_secret: str | None = None,
    ) -> dict[str, str]:
        presented = extract_presented_secret(
            request,
            path_token=path_token,
            x_webhook_secret=x_webhook_secret,
        )
        if not is_webhook_authorized(presented, settings):
            raise HTTPException(status_code=401, detail="Webhook no autorizado")

        body = await request.json()
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Body JSON inválido")
        event_type, payload = parse_alegra_webhook_body(body)

        with session_scope(settings) as session:
            processor = WebhookProcessor(settings, session)
            event = processor.enqueue_event(event_type, payload)
            session.commit()
            event_id = str(event.id)

        # BackgroundTasks de FastAPI sobrevive al response; create_task a veces no.
        background_tasks.add_task(_process_pending_webhooks)
        return {"status": "accepted", "event_id": event_id, "event": event_type}

    @app.post("/webhooks/alegra")
    async def receive_webhook(
        request: Request,
        background_tasks: BackgroundTasks,
        x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    ) -> dict[str, str]:
        return await _handle_webhook(
            request,
            background_tasks,
            x_webhook_secret=x_webhook_secret,
        )

    @app.post("/webhooks/alegra/{path_token}")
    async def receive_webhook_with_path_token(
        path_token: str,
        request: Request,
        background_tasks: BackgroundTasks,
        x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    ) -> dict[str, str]:
        return await _handle_webhook(
            request,
            background_tasks,
            path_token=path_token,
            x_webhook_secret=x_webhook_secret,
        )

    @app.post("/webhooks/process-pending")
    async def process_pending_endpoint(
        request: Request,
        x_webhook_secret: str | None = Header(default=None, alias="X-Webhook-Secret"),
    ) -> dict[str, Any]:
        """Endpoint de mantenimiento para drenar la cola pending."""
        presented = extract_presented_secret(request, x_webhook_secret=x_webhook_secret)
        if not is_webhook_authorized(presented, settings):
            raise HTTPException(status_code=401, detail="Webhook no autorizado")
        with session_scope(settings) as session:
            processor = WebhookProcessor(settings, session)
            result = await processor.process_pending(limit=100)
            session.commit()
        return {"status": "ok", **result}

    return app
