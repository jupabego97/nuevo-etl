"""CLI principal del ETL."""

from __future__ import annotations

import asyncio
import sys
import traceback

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

app = typer.Typer(
    help="ETL productivo Alegra → PostgreSQL",
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
)


def _setup() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_json)


def _run_migrations() -> None:
    from alembic import command
    from alembic.config import Config

    settings = get_settings()
    print(f"[cli] Migrando esquema {settings.db_schema!r}...", flush=True)
    ensure_schema(settings)
    alembic_cfg = Config("alembic.ini")
    try:
        command.upgrade(alembic_cfg, "head")
    except Exception:
        print("[cli] FALLO en migraciones:", flush=True)
        traceback.print_exc()
        raise
    print("[cli] Migraciones OK", flush=True)


@app.command("bootstrap")
def bootstrap() -> None:
    """Crea esquema, aplica migraciones y valida conexión."""
    _setup()
    settings = get_settings()
    _run_migrations()
    engine = create_db_engine(settings)
    with engine.connect() as conn:
        conn.execute(__import__("sqlalchemy").text("SELECT 1"))
    typer.echo(f"Bootstrap OK. Esquema objetivo: {settings.db_schema}")


@app.command("migrate")
def migrate() -> None:
    """Ejecuta migraciones Alembic."""
    _setup()
    _run_migrations()
    typer.echo("Migraciones aplicadas")


@app.command("backfill")
def backfill() -> None:
    """Carga histórica completa de recursos habilitados."""
    _setup()
    settings = get_settings()
    _run_migrations()

    async def _run() -> None:
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            run_id = await runner.run_backfill()
            typer.echo(f"Backfill completado. run_id={run_id}")

    asyncio.run(_run())


@app.command("daily-sync")
def daily_sync() -> None:
    """Sincronización incremental diaria con ventana solapada."""
    import signal

    def _on_signal(signum: int, _frame: object) -> None:
        print(f"[cli] Señal {signum} recibida (Railway puede estar deteniendo el job)", flush=True)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    _setup()
    settings = get_settings()
    print("[cli] Iniciando daily-sync...", flush=True)
    _run_migrations()
    print("[cli] Preparando sincronización Alegra...", flush=True)

    async def _run() -> None:
        print("[cli] Abriendo sesión PostgreSQL...", flush=True)
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            run_id = await runner.run_daily_sync()
            print("[cli] Construyendo marts...", flush=True)
            builder = MartBuilder(settings, session)
            marts = builder.build_all()
            print(f"[cli] Daily sync completado. run_id={run_id} marts={marts}", flush=True)

    try:
        asyncio.run(_run())
    except Exception:
        print("[cli] FALLO en daily-sync:", flush=True)
        traceback.print_exc()
        raise
    print("[cli] Exit OK", flush=True)
    sys.exit(0)


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
    except Exception:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
