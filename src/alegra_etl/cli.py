"""CLI principal del ETL."""

from __future__ import annotations

import asyncio
import sys
import traceback
from typing import Any

import typer
import uvicorn

from alegra_etl.config import get_settings
from alegra_etl.db.session import create_db_engine, ensure_schema, session_scope
from alegra_etl.logging import setup_logging
from alegra_etl.marts.builder import MartBuilder
from alegra_etl.pipeline.reconciler import Reconciler
from alegra_etl.pipeline.runner import PipelineRunner
from alegra_etl.pipeline.webhook_processor import WebhookProcessor

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


@app.command("backfill-audit")
def backfill_audit() -> None:
    """Diagnóstico de checkpoints y cobertura de backfill."""
    import json

    _setup()
    settings = get_settings()
    with session_scope(settings) as session:
        from alegra_etl.pipeline.checkpoint_integrity import audit_checkpoints

        report = audit_checkpoints(settings, session)
        typer.echo(json.dumps(report, indent=2, default=str))


@app.command("backfill-repair")
def backfill_repair(
    apply: bool = typer.Option(False, "--apply", help="Aplicar reparación idempotente"),
) -> None:
    """Reabre checkpoints inválidos sin borrar datos."""
    import json

    _setup()
    settings = get_settings()
    with session_scope(settings) as session:
        from alegra_etl.pipeline.checkpoint_integrity import repair_all_invalid

        result = repair_all_invalid(settings, session, apply=apply)
        if apply:
            session.commit()
        typer.echo(json.dumps(result, indent=2, default=str))


@app.command("backfill-status")
def backfill_status() -> None:
    """Progreso de work items y gate de completitud."""
    import json

    _setup()
    settings = get_settings()
    with session_scope(settings) as session:
        from alegra_etl.alegra.resources import get_backfill_resources
        from alegra_etl.db.models import SyncCheckpoint
        from alegra_etl.pipeline.backfill_work import work_progress
        from alegra_etl.pipeline.completion_gate import global_backfill_status

        resources = {r.name: r for r in get_backfill_resources(settings)}
        checkpoints = session.query(SyncCheckpoint).filter_by(company_id=settings.company_id).all()
        status = global_backfill_status(session, settings, resources, checkpoints)
        progress = work_progress(session, settings.company_id)
        typer.echo(
            json.dumps(
                {"gate": status, "work_items": progress},
                indent=2,
                default=str,
            )
        )


@app.command("backfill-workers")
def backfill_workers(
    resource: str = typer.Option(None, help="Limitar a un recurso"),
) -> None:
    """Workers concurrentes con leases y sesiones aisladas."""
    _setup()
    settings = get_settings()
    _run_migrations()

    async def _run() -> None:
        from alegra_etl.pipeline.backfill_worker import BackfillWorkerRunner

        with session_scope(settings) as session:
            runner = BackfillWorkerRunner(settings, session)
            # Bucle durable: no salir con error ante bloqueos temporales
            # (Railway reiniciaría el contenedor en vacío).
            result = await runner.run_until_idle(
                resource_name=resource,
                idle_sleep_seconds=60,
            )
            typer.echo(str(result))
            if result.get("status") == "complete":
                return
            # blocked/batch_limit: salir 0 para no provocar restart storm
            # si restartPolicy=ON_FAILURE; el log ya explica el estado.
            print(
                f"[backfill-workers] Finalizó sin complete: {result.get('status')} "
                f"reason={result.get('reason')}",
                flush=True,
            )

    asyncio.run(_run())


@app.command("replay-source")
def replay_source(
    resource: str = typer.Option(..., help="Nombre del recurso"),
    limit: int = typer.Option(None, help="Máximo de documentos"),
    dry_run: bool = typer.Option(False, help="Solo contar"),
) -> None:
    """Reconstruye tablas tipadas desde source_documents."""
    import json

    _setup()
    settings = get_settings()
    with session_scope(settings) as session:
        from alegra_etl.pipeline.replay_source import replay_source_documents

        result = replay_source_documents(
            session,
            settings,
            resource_name=resource,
            limit=limit,
            dry_run=dry_run,
        )
        typer.echo(json.dumps(result, indent=2, default=str))


@app.command("backfill-recover")
def backfill_recover() -> None:
    """Runbook de recuperación: audit → repair → workers → reconcile → marts."""
    import json

    _setup()
    settings = get_settings()
    _run_migrations()

    async def _run() -> None:
        from alegra_etl.alegra.resources import get_backfill_resources
        from alegra_etl.pipeline.backfill_worker import BackfillWorkerRunner
        from alegra_etl.pipeline.checkpoint_integrity import audit_checkpoints, repair_all_invalid
        from alegra_etl.pipeline.reconciler import Reconciler
        from alegra_etl.quality.checks import backfill_coverage_manifest

        steps: dict[str, Any] = {}
        with session_scope(settings) as session:
            steps["audit_before"] = audit_checkpoints(settings, session)
            steps["repair"] = repair_all_invalid(settings, session, apply=True)
            session.commit()

        with session_scope(settings) as session:
            worker = BackfillWorkerRunner(settings, session)
            steps["workers"] = await worker.run_until_idle()
            if steps["workers"].get("status") != "complete":
                raise RuntimeError(f"Backfill bloqueado: {steps['workers']}")

        with session_scope(settings) as session:
            reconciler = Reconciler(settings, session)
            for resource in get_backfill_resources(settings):
                resource_name = resource.name
                steps[f"reconcile_{resource_name}"] = await reconciler.reconcile_checkpoint(
                    resource_name
                )
            final_audit = audit_checkpoints(settings, session)
            if not final_audit["all_backfill_complete"]:
                raise RuntimeError(
                    f"Reconciliación incompleta; marts no regenerados: "
                    f"{final_audit.get('blockers_by_resource', {})}"
                )
            session.commit()
            builder = MartBuilder(settings, session)
            steps["marts"] = builder.build_all()
            steps["manifest"] = backfill_coverage_manifest(session, settings.company_id, settings)
            steps["audit_after"] = audit_checkpoints(settings, session)

        typer.echo(json.dumps(steps, indent=2, default=str))

    asyncio.run(_run())


@app.command("backfill-step")
def backfill_step() -> None:
    """Procesa un lote reanudable del histórico (para cron temporal en Railway)."""
    _setup()
    settings = get_settings()
    _run_migrations()

    async def _run() -> None:
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            result = await runner.run_backfill_step()
            typer.echo(str(result))

    asyncio.run(_run())
    sys.exit(0)


@app.command("weekly-refresh")
def weekly_refresh() -> None:
    """Refresca maestros completos (items, contactos, etc.)."""
    _setup()
    settings = get_settings()
    _run_migrations()

    async def _run() -> None:
        with session_scope(settings) as session:
            runner = PipelineRunner(settings, session)
            run_id = await runner.run_weekly_refresh()
            typer.echo(f"Weekly refresh completado. run_id={run_id}")

    asyncio.run(_run())


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
