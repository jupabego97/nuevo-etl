"""Worker de backfill concurrente por unidades de trabajo."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from alegra_etl.alegra.client import AlegraClient, AlegraClientError
from alegra_etl.alegra.resources import SyncStrategy, get_backfill_resources, resource_by_name
from alegra_etl.config import Settings
from alegra_etl.db.models import BackfillWorkItem, EtlRun, SyncCheckpoint
from alegra_etl.pipeline.advisory_lock import release_backfill_lock, try_acquire_backfill_lock
from alegra_etl.pipeline.backfill_work import (
    all_work_verified,
    claim_work_items,
    mark_work_failed,
    mark_work_verified,
    requeue_failed_work,
    seed_all_pending_work,
    work_progress,
)
from alegra_etl.pipeline.checkpoint import CheckpointManager
from alegra_etl.pipeline.completion_gate import global_backfill_status
from alegra_etl.pipeline.extractor import ResourceExtractor
from alegra_etl.pipeline.reconciler import Reconciler

logger = logging.getLogger(__name__)


class BackfillWorkerRunner:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session

    async def run_until_idle(
        self,
        *,
        resource_name: str | None = None,
        max_batches: int | None = None,
        idle_sleep_seconds: int = 60,
        max_idle_cycles: int | None = None,
    ) -> dict[str, Any]:
        """Consume lotes hasta completar; ante bloqueo espera y reintenta."""
        batches: list[dict[str, Any]] = []
        idle_cycles = 0
        last_result: dict[str, Any] | None = None

        while max_batches is None or len(batches) < max_batches:
            result = await self.run_batch(resource_name=resource_name)
            last_result = result
            batches.append(result)
            progress = result.get("progress", {})
            by_status = progress.get("by_status", {})

            if result.get("status") != "no_work":
                idle_cycles = 0
                continue

            # Sin claimable work: reabrir failed y evaluar gate.
            requeued = requeue_failed_work(
                self.session,
                self.settings.company_id,
                resource_name=resource_name,
            )
            if requeued:
                self.session.commit()
                print(
                    f"[backfill-workers] Reencolados {requeued} work items failed",
                    flush=True,
                )
                idle_cycles = 0
                continue

            if by_status.get("pending", 0) or by_status.get("running", 0):
                # Hay trabajo leased/pendiente no claimable aún: esperar leases.
                idle_cycles += 1
                print(
                    f"[backfill-workers] Esperando leases/pendientes "
                    f"(ciclo={idle_cycles}) progress={by_status}",
                    flush=True,
                )
                if max_idle_cycles is not None and idle_cycles >= max_idle_cycles:
                    return {
                        "status": "blocked",
                        "reason": "work_items_no_verificados",
                        "batches": len(batches),
                        "last": result,
                    }
                await asyncio.sleep(idle_sleep_seconds)
                continue

            resources = {r.name: r for r in get_backfill_resources(self.settings)}
            checkpoints = (
                self.session.query(SyncCheckpoint)
                .filter_by(company_id=self.settings.company_id)
                .all()
            )
            gate = global_backfill_status(self.session, self.settings, resources, checkpoints)
            if gate["all_backfill_complete"]:
                return {
                    "status": "complete",
                    "batches": len(batches),
                    "last": result,
                    "gate": gate,
                }

            idle_cycles += 1
            print(
                f"[backfill-workers] Gate incompleto; reintento en {idle_sleep_seconds}s "
                f"(ciclo={idle_cycles}) pending={gate.get('pending', [])[:8]}",
                flush=True,
            )
            if max_idle_cycles is not None and idle_cycles >= max_idle_cycles:
                return {
                    "status": "blocked",
                    "reason": "gate_de_completitud",
                    "batches": len(batches),
                    "gate": gate,
                    "last": result,
                }
            # Reseed por si faltan work items de recursos reabiertos.
            seed_all_pending_work(self.settings, self.session)
            self.session.commit()
            await asyncio.sleep(idle_sleep_seconds)

        return {
            "status": "batch_limit",
            "batches": len(batches),
            "last": last_result,
        }

    async def run_batch(self, *, resource_name: str | None = None) -> dict[str, Any]:
        run = EtlRun(run_type="backfill_workers", status="running", metrics={})
        self.session.add(run)
        self.session.flush()
        owner = f"worker-{run.id}"

        checkpoint_manager = CheckpointManager(self.settings, self.session)
        for resource in get_backfill_resources(self.settings):
            checkpoint_manager.get_or_create(resource)
        seed_counts = seed_all_pending_work(self.settings, self.session)
        self.session.commit()

        items = claim_work_items(
            self.session,
            company_id=self.settings.company_id,
            resource_name=resource_name,
            limit=self.settings.backfill_work_batch_size,
            owner=owner,
        )
        self.session.commit()

        if not items:
            progress = work_progress(self.session, self.settings.company_id, resource_name)
            run.status = "success"
            run.finished_at = datetime.now(UTC)
            run.metrics = {"status": "no_work", "progress": progress}
            self.session.commit()
            return {
                "status": "no_work",
                "seeded": seed_counts,
                "progress": progress,
                "run_id": str(run.id),
            }

        processed = 0
        failed = 0
        metrics: dict[str, Any] = {"items": {}}

        async with AlegraClient(self.settings) as client:
            # Agrupar por recurso para advisory lock
            by_resource: dict[str, list[int]] = {}
            for item in items:
                by_resource.setdefault(item.resource_name, []).append(item.id)

            factory = sessionmaker(
                bind=self.session.get_bind(),
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )

            async def _process_resource_group(res_name: str, item_ids: list[int]) -> None:
                nonlocal processed, failed
                resource = resource_by_name(res_name)
                with factory() as worker_session:
                    group_items = (
                        worker_session.query(BackfillWorkItem)
                        .filter(BackfillWorkItem.id.in_(item_ids))
                        .order_by(BackfillWorkItem.id.asc())
                        .all()
                    )
                    if not resource:
                        for item in group_items:
                            mark_work_failed(item, f"recurso desconocido: {res_name}")
                        worker_session.commit()
                        failed += len(group_items)
                        return

                    if not try_acquire_backfill_lock(
                        worker_session, self.settings.company_id, res_name
                    ):
                        for item in group_items:
                            item.status = "pending"
                            item.lease_owner = None
                            item.leased_until = None
                        worker_session.commit()
                        return

                    try:
                        extractor = ResourceExtractor(
                            self.settings, client, worker_session, run.id, strict_backfill=True
                        )
                        for item in group_items:
                            result: dict[str, Any]
                            try:
                                if item.work_date and resource.strategy == SyncStrategy.DATE_WINDOW:
                                    result = await extractor.extract_resource(
                                        resource,
                                        start_date=item.work_date,
                                        end_date=item.work_date,
                                        start_offset=item.start_offset,
                                        max_pages=self.settings.backfill_max_pages_per_day,
                                    )
                                else:
                                    result = await extractor.extract_resource(
                                        resource,
                                        start_offset=item.start_offset,
                                        max_pages=self.settings.backfill_pages_per_step,
                                    )
                                mark_work_verified(item, result, run.id)
                                metrics["items"][f"{res_name}:{item.work_key}"] = result
                                processed += 1
                                worker_session.commit()
                            except AlegraClientError as exc:
                                if exc.status_code in {400, 403, 404} and resource.optional:
                                    checkpoint = (
                                        worker_session.query(SyncCheckpoint)
                                        .filter_by(
                                            company_id=self.settings.company_id,
                                            resource_name=res_name,
                                        )
                                        .one_or_none()
                                    )
                                    item.status = "unsupported"
                                    item.lease_owner = None
                                    item.leased_until = None
                                    if checkpoint:
                                        CheckpointManager(
                                            self.settings, worker_session
                                        ).mark_unsupported(checkpoint, str(exc))
                                    worker_session.commit()
                                    continue
                                mark_work_failed(item, str(exc))
                                failed += 1
                                worker_session.commit()
                                logger.exception(
                                    "Fallo Alegra work item %s/%s", res_name, item.work_key
                                )
                            except Exception as exc:
                                mark_work_failed(item, str(exc))
                                failed += 1
                                worker_session.commit()
                                logger.exception("Fallo work item %s/%s", res_name, item.work_key)

                        # La API se consulta solo cuando todas las unidades locales terminaron.
                        if all_work_verified(worker_session, self.settings.company_id, res_name):
                            checkpoint = (
                                worker_session.query(SyncCheckpoint)
                                .filter_by(
                                    company_id=self.settings.company_id,
                                    resource_name=res_name,
                                )
                                .one_or_none()
                            )
                            if checkpoint:
                                checkpoint.status = "running"
                                reconciler = Reconciler(self.settings, worker_session)
                                report = await reconciler.reconcile_resource(
                                    res_name,
                                    start_date=(
                                        checkpoint.backfill_start_date
                                        if resource.strategy == SyncStrategy.DATE_WINDOW
                                        else None
                                    ),
                                    end_date=(
                                        checkpoint.backfill_end_date
                                        if resource.strategy == SyncStrategy.DATE_WINDOW
                                        else None
                                    ),
                                    reprocess=False,
                                )
                                mismatches = report.get("mismatches", {})
                                if mismatches:
                                    for key in mismatches:
                                        work_item = (
                                            worker_session.query(BackfillWorkItem)
                                            .filter_by(
                                                company_id=self.settings.company_id,
                                                resource_name=res_name,
                                                work_key=key,
                                            )
                                            .one_or_none()
                                        )
                                        if work_item:
                                            work_item.status = "pending"
                                            work_item.start_offset = 0
                                            work_item.verified_at = None
                                            work_item.error_message = "reconciliation_mismatch"
                                    if resource.strategy == SyncStrategy.FULL:
                                        for work_item in (
                                            worker_session.query(BackfillWorkItem)
                                            .filter_by(
                                                company_id=self.settings.company_id,
                                                resource_name=res_name,
                                            )
                                            .all()
                                        ):
                                            work_item.status = "pending"
                                            work_item.start_offset = 0
                                            work_item.verified_at = None
                                            work_item.error_message = "reconciliation_mismatch"
                                    checkpoint.status = "pending"
                                    checkpoint.verified_at = None
                                    checkpoint.backfill_completed_at = None
                                else:
                                    coverage = report.get("coverage", {})
                                    for work_item in (
                                        worker_session.query(BackfillWorkItem)
                                        .filter_by(
                                            company_id=self.settings.company_id,
                                            resource_name=res_name,
                                        )
                                        .all()
                                    ):
                                        evidence = coverage.get(work_item.work_key) or coverage.get(
                                            "full"
                                        )
                                        if evidence:
                                            work_item.api_records = evidence["api_records"]
                                            work_item.api_distinct_ids = evidence[
                                                "api_distinct_ids"
                                            ]
                                            work_item.source_distinct_ids = evidence[
                                                "source_distinct_ids"
                                            ]
                                            work_item.typed_distinct_ids = evidence[
                                                "typed_distinct_ids"
                                            ]
                                    checkpoint.metadata_json = {
                                        **(checkpoint.metadata_json or {}),
                                        "reconciliation_verified_at": datetime.now(UTC).isoformat(),
                                        "reconciliation_verified_generation": checkpoint.backfill_generation,
                                        "reconciliation_resource": res_name,
                                    }
                                    if resource.strategy == SyncStrategy.DATE_WINDOW:
                                        from datetime import timedelta

                                        end = checkpoint.backfill_end_date or date.today()
                                        checkpoint.cursor_date = end + timedelta(days=1)
                                    checkpoint.cursor_offset = 0
                                    manager = CheckpointManager(self.settings, worker_session)
                                    manager.try_mark_backfill_completed(checkpoint, resource)
                                worker_session.commit()
                    except Exception as exc:
                        # Una falla de reconciliación invalida la verificación local:
                        # se reencola el recurso completo y nunca se deja un checkpoint
                        # aparentemente listo.
                        for work_item in (
                            worker_session.query(BackfillWorkItem)
                            .filter_by(
                                company_id=self.settings.company_id,
                                resource_name=res_name,
                            )
                            .all()
                        ):
                            work_item.status = "pending"
                            work_item.start_offset = 0
                            work_item.verified_at = None
                            work_item.error_message = f"fallo_grupo:{exc}"[:2000]
                        checkpoint = (
                            worker_session.query(SyncCheckpoint)
                            .filter_by(
                                company_id=self.settings.company_id,
                                resource_name=res_name,
                            )
                            .one_or_none()
                        )
                        if checkpoint:
                            checkpoint.status = "pending"
                            checkpoint.backfill_completed_at = None
                            checkpoint.verified_at = None
                            metadata = dict(checkpoint.metadata_json or {})
                            metadata.pop("reconciliation_verified_at", None)
                            metadata.pop("reconciliation_verified_generation", None)
                            metadata.pop("reconciliation_resource", None)
                            checkpoint.metadata_json = metadata
                        worker_session.commit()
                        failed += 1
                        logger.exception("Fallo de grupo de backfill %s", res_name)
                    finally:
                        release_backfill_lock(worker_session, self.settings.company_id, res_name)

            # Limitar recursos en paralelo para evitar OOM / reinicios en Railway.
            max_parallel_resources = min(2, max(1, self.settings.sync_max_concurrent // 4))
            resource_semaphore = asyncio.Semaphore(max_parallel_resources)

            async def _bounded_resource_group(name: str, group: list[int]) -> None:
                async with resource_semaphore:
                    await _process_resource_group(name, group)

            tasks = [
                _bounded_resource_group(name, group) for name, group in by_resource.items()
            ]
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            for task_result in task_results:
                if isinstance(task_result, Exception):
                    failed += 1
                    logger.error(
                        "Fallo no controlado en worker de backfill",
                        exc_info=task_result,
                    )

        run.status = "success"
        run.metrics = {"processed": processed, "failed": failed, **metrics}
        run.finished_at = datetime.now(UTC)
        self.session.commit()

        return {
            "status": "ok",
            "processed": processed,
            "failed": failed,
            "seeded": seed_counts,
            "progress": work_progress(self.session, self.settings.company_id, resource_name),
            "run_id": str(run.id),
        }
