"""CLI principal del ETL."""

from __future__ import annotations

import asyncio
import sys

import typer
import uvicorn

from alegra_etl.config import get_settings
from alegra_etl.db.session import create_db_engine, ensure_schema, session_scope
from alegra_etl.logging import setup_logging
from alegra_etl.marts.builder import MartBuilder
from alegra_etl.pipeline.reconciler import Reconciler
from alegra_etl.pipeline.runner import PipelineRunner
from alegra_etl.pipeline.webhook_processor import WebhookProcessor
from alegra_etl.web.app import create_app

app = typer.Typer(help="ETL productivo Alegra → PostgreSQL")


def _setup() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_json)


@app.command("bootstrap")
def bootstrap() -> None:
    """Crea esquema, aplica migraciones y valida conexión."""
    _setup()
    settings = get_settings()
    ensure_schema(settings)
    engine = create_db_engine(settings)
    with engine.connect() as conn:
        conn.execute(__import__("sqlalchemy").text("SELECT 1"))
    typer.echo(f"Bootstrap OK. Esquema objetivo: {settings.db_schema}")


@app.command("migrate")
def migrate() -> None:
    """Ejecuta migraciones Alembic."""
    _setup()
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")
    typer.echo("Migraciones aplicadas")


@app.command("backfill")
def backfill() -> None:
    """Carga histórica completa de recursos habilitados."""
    _setup()
    settings = get_settings()

    async def _run() -> None:
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            run_id = await runner.run_backfill()
            typer.echo(f"Backfill completado. run_id={run_id}")

    asyncio.run(_run())


@app.command("daily-sync")
def daily_sync() -> None:
    """Sincronización incremental diaria con ventana solapada."""
    _setup()
    settings = get_settings()

    async def _run() -> None:
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            run_id = await runner.run_daily_sync()
            builder = MartBuilder(settings, session)
            marts = builder.build_all()
            typer.echo(f"Daily sync completado. run_id={run_id} marts={marts}")

    asyncio.run(_run())


@app.command("reconcile")
def reconcile(
    resource: str = typer.Option("invoices", help="Nombre del recurso"),
    days: int = typer.Option(30, help="Días a reconciliar"),
) -> None:
    """Reconcilia conteos API vs BD y reprocesa inconsistencias."""
    _setup()
    settings = get_settings()

    async def _run() -> None:
        with session_scope(settings) as session:
            reconciler = Reconciler(settings, session)
            result = await reconciler.reconcile_resource(resource, days=days)
            typer.echo(str(result))

    asyncio.run(_run())


@app.command("process-webhooks")
def process_webhooks(limit: int = typer.Option(100, help="Máximo de eventos")) -> None:
    """Procesa eventos webhook pendientes."""
    _setup()
    settings = get_settings()

    async def _run() -> None:
        with session_scope(settings) as session:
            processor = WebhookProcessor(settings, session)
            result = await processor.process_pending(limit=limit)
            typer.echo(str(result))

    asyncio.run(_run())


@app.command("build-marts")
def build_marts() -> None:
    """Regenera tablas gold analíticas."""
    _setup()
    settings = get_settings()
    with session_scope(settings) as session:
        builder = MartBuilder(settings, session)
        result = builder.build_all()
        typer.echo(str(result))


@app.command("serve-webhooks")
def serve_webhooks() -> None:
    """Levanta el servicio FastAPI de webhooks."""
    _setup()
    settings = get_settings()
    uvicorn.run(
        "alegra_etl.web.app:create_app",
        factory=True,
        host=settings.webhook_host,
        port=settings.webhook_port,
    )


def main() -> None:
    try:
        app()
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
