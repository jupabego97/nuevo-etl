"""Orquestación de runs ETL."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient, AlegraClientError
from alegra_etl.alegra.resources import (
    ResourceDefinition,
    SyncStrategy,
    get_backfill_resources,
    get_daily_sync_resources,
    get_weekly_refresh_resources,
)
from alegra_etl.config import Settings
from alegra_etl.db.models import EtlRun, EtlStageRun, SyncCheckpoint
from alegra_etl.pipeline.checkpoint import CheckpointManager
from alegra_etl.pipeline.extractor import ResourceExtractor
from alegra_etl.quality.checks import run_quality_checks

logger = logging.getLogger(__name__)

_BOGOTA = ZoneInfo("America/Bogota")


def _today_local() -> date:
    return datetime.now(_BOGOTA).date()


class PipelineRunner:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
        self.checkpoints = CheckpointManager(settings, session)

    def _create_run(self, run_type: str) -> EtlRun:
        run = EtlRun(run_type=run_type, status="running", started_at=datetime.now(UTC), metrics={})
        self.session.add(run)
        self.session.flush()
        return run

    def _finish_run(self, run: EtlRun, status: str, metrics: dict[str, Any], error: str | None = None) -> None:
        run.status = status
        run.finished_at = datetime.now(UTC)
        run.metrics = metrics
        run.error_message = error

    def _should_skip_error(self, resource: ResourceDefinition, exc: AlegraClientError) -> bool:
        return exc.status_code in {400, 403, 404} and resource.optional

    async def run_backfill(self) -> uuid.UUID:
        run = self._create_run("backfill")
        metrics: dict[str, Any] = {"resources": {}}
        resources = get_backfill_resources(self.settings)

        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            for resource in resources:
                stage = self._start_stage(run, resource.name)
                checkpoint = self.checkpoints.get_or_create(resource)
                self.checkpoints.mark_running(checkpoint, run.id)
                try:
                    start, end = self.checkpoints.backfill_window(resource, checkpoint)
                    result = await self._extract_with_strategy(
                        extractor,
                        resource,
                        start_date=start if resource.strategy == SyncStrategy.DATE_WINDOW else None,
                        end_date=end if resource.strategy == SyncStrategy.DATE_WINDOW else None,
                        start_offset=checkpoint.cursor_offset,
                        max_pages=None,
                    )
                    self.checkpoints.update_after_batch(checkpoint, resource, result, run.id)
                    self._finish_stage(stage, "success", result)
                    self.session.commit()
                    metrics["resources"][resource.name] = result
                except AlegraClientError as exc:
                    if self._should_skip_error(resource, exc):
                        self._finish_stage(stage, "skipped", {"reason": str(exc)})
                        checkpoint.status = "completed"
                        metrics["resources"][resource.name] = {"status": "skipped"}
                        self.session.commit()
                    else:
                        self.checkpoints.mark_failed(checkpoint, str(exc))
                        self._finish_stage(stage, "failed", {}, str(exc))
                        self._finish_run(run, "failed", metrics, str(exc))
                        self.session.commit()
                        raise
                except Exception as exc:
                    self.checkpoints.mark_failed(checkpoint, str(exc))
                    self._finish_stage(stage, "failed", {}, str(exc))
                    self._finish_run(run, "failed", metrics, str(exc))
                    self.session.commit()
                    raise

        quality = run_quality_checks(self.session, run.id, self.settings.company_id)
        metrics["quality"] = quality
        self._finish_run(run, "success", metrics)
        self.session.commit()
        return run.id

    async def run_backfill_step(self) -> dict[str, Any]:
        """Procesa un lote reanudable y termina con código 0."""
        run = self._create_run("backfill_step")
        self.session.commit()
        resources = get_backfill_resources(self.settings)
        target: ResourceDefinition | None = None
        checkpoint: SyncCheckpoint | None = None

        for resource in resources:
            cp = self.checkpoints.get_or_create(resource)
            if cp.status != "completed":
                target = resource
                checkpoint = cp
                break

        if not target or not checkpoint:
            self._finish_run(run, "success", {"status": "all_completed"})
            self.session.commit()
            print("[backfill-step] Histórico completo", flush=True)
            return {"status": "all_completed", "run_id": str(run.id)}

        print(f"[backfill-step] Recurso={target.name} status={checkpoint.status}", flush=True)
        stage = self._start_stage(run, target.name)
        self.checkpoints.mark_running(checkpoint, run.id)
        self.session.commit()

        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            try:
                start, end = self.checkpoints.backfill_window(target, checkpoint)
                result = await self._extract_with_strategy(
                    extractor,
                    target,
                    start_date=start if target.strategy == SyncStrategy.DATE_WINDOW else None,
                    end_date=end if target.strategy == SyncStrategy.DATE_WINDOW else None,
                    start_offset=checkpoint.cursor_offset,
                    max_pages=self.settings.backfill_pages_per_step,
                )
                self.checkpoints.update_after_batch(checkpoint, target, result, run.id)
                self._finish_stage(stage, "success", result)
                self._finish_run(run, "success", {"resource": target.name, **result})
                self.session.commit()
                print(f"[backfill-step] OK {target.name} {result}", flush=True)
                return {"resource": target.name, "run_id": str(run.id), **result}
            except AlegraClientError as exc:
                if self._should_skip_error(target, exc):
                    self._finish_stage(stage, "skipped", {"reason": str(exc)})
                    checkpoint.status = "completed"
                    self._finish_run(run, "success", {"resource": target.name, "status": "skipped"})
                    self.session.commit()
                    return {"resource": target.name, "status": "skipped", "reason": str(exc)}
                self.checkpoints.mark_failed(checkpoint, str(exc))
                self._finish_stage(stage, "failed", {}, str(exc))
                self._finish_run(run, "failed", {"resource": target.name}, str(exc))
                self.session.commit()
                raise

    async def run_daily_sync(self) -> uuid.UUID:
        print("[sync] Creando registro etl_run...", flush=True)
        run = self._create_run("daily_sync")
        self.session.commit()
        print(f"[sync] run_id={run.id}", flush=True)
        metrics: dict[str, Any] = {"resources": {}}
        end_date = _today_local()
        start_date = end_date - timedelta(days=self.settings.sync_overlap_days)
        resources = get_daily_sync_resources(self.settings)
        print(
            f"[sync] Recursos daily: {len(resources)} "
            f"ventana={start_date.isoformat()}..{end_date.isoformat()} (America/Bogota)",
            flush=True,
        )

        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            for index, resource in enumerate(resources, start=1):
                print(
                    f"[sync] ({index}/{len(resources)}) {resource.name} "
                    f"({resource.strategy.value})...",
                    flush=True,
                )
                stage = self._start_stage(run, resource.name)
                try:
                    result = await self._extract_with_strategy(
                        extractor,
                        resource,
                        start_date=start_date if resource.strategy == SyncStrategy.DATE_WINDOW else None,
                        end_date=end_date if resource.strategy == SyncStrategy.DATE_WINDOW else None,
                    )
                    self._finish_stage(stage, "success", result)
                    self.checkpoints.mark_daily_sync(resource.name, run.id)
                    self.session.commit()
                    metrics["resources"][resource.name] = result
                    print(
                        f"[sync] {resource.name} OK extracted={result.get('extracted', 0)} "
                        f"source={result.get('source_upserted', 0)} "
                        f"typed={result.get('typed_upserted', 0)}",
                        flush=True,
                    )
                except AlegraClientError as exc:
                    if self._should_skip_error(resource, exc):
                        self._finish_stage(stage, "skipped", {"reason": str(exc), "status": exc.status_code})
                        self.session.commit()
                        metrics["resources"][resource.name] = {"status": "skipped", "reason": str(exc)}
                        print(f"[sync] {resource.name} skipped ({exc.status_code})", flush=True)
                    else:
                        self._finish_stage(stage, "failed", {}, str(exc))
                        self._finish_run(run, "failed", metrics, str(exc))
                        self.session.commit()
                        raise
                except Exception as exc:
                    self._finish_stage(stage, "failed", {}, str(exc))
                    self._finish_run(run, "failed", metrics, str(exc))
                    self.session.commit()
                    print(f"[sync] {resource.name} FAILED: {type(exc).__name__}: {exc}", flush=True)
                    raise

        quality = run_quality_checks(self.session, run.id, self.settings.company_id)
        metrics["quality"] = quality
        self._finish_run(run, "success", metrics)
        self.session.commit()
        print("[sync] daily-sync terminado OK", flush=True)
        return run.id

    async def run_weekly_refresh(self) -> uuid.UUID:
        run = self._create_run("weekly_refresh")
        self.session.commit()
        resources = get_weekly_refresh_resources(self.settings)
        metrics: dict[str, Any] = {"resources": {}}
        print(f"[weekly] Recursos: {len(resources)}", flush=True)

        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            for resource in resources:
                stage = self._start_stage(run, resource.name)
                try:
                    result = await self._extract_with_strategy(extractor, resource)
                    self._finish_stage(stage, "success", result)
                    self.checkpoints.mark_daily_sync(resource.name, run.id)
                    self.session.commit()
                    metrics["resources"][resource.name] = result
                except AlegraClientError as exc:
                    if self._should_skip_error(resource, exc):
                        self._finish_stage(stage, "skipped", {"reason": str(exc)})
                        self.session.commit()
                        metrics["resources"][resource.name] = {"status": "skipped"}
                    else:
                        raise

        self._finish_run(run, "success", metrics)
        self.session.commit()
        return run.id

    async def run_single_resource(
        self,
        resource: ResourceDefinition,
        *,
        resource_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        run = self._create_run("single_resource")
        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            stage = self._start_stage(run, resource.name)
            if resource_id:
                result = await extractor.extract_resource_by_id(resource, resource_id)
            else:
                result = await self._extract_with_strategy(
                    extractor, resource, start_date=start_date, end_date=end_date
                )
            self._finish_stage(stage, "success", result)
            self.checkpoints.mark_daily_sync(resource.name, run.id)
            self._finish_run(run, "success", {"resource": resource.name, **result})
            self.session.commit()
            return result

    async def _extract_with_strategy(
        self,
        extractor: ResourceExtractor,
        resource: ResourceDefinition,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        start_offset: int = 0,
        max_pages: int | None = None,
    ) -> dict[str, int]:
        return await extractor.extract_resource(
            resource,
            start_date=start_date,
            end_date=end_date,
            start_offset=start_offset,
            max_pages=max_pages,
        )

    def _start_stage(self, run: EtlRun, resource_name: str) -> EtlStageRun:
        stage = EtlStageRun(run_id=run.id, stage_name="extract_load", resource_name=resource_name, status="running")
        self.session.add(stage)
        self.session.flush()
        return stage

    def _finish_stage(self, stage: EtlStageRun, status: str, metrics: dict[str, Any], error: str | None = None) -> None:
        stage.status = status
        stage.records_extracted = int(metrics.get("extracted", 0))
        stage.records_loaded = int(metrics.get("typed_upserted", metrics.get("loaded", 0)))
        stage.metrics = metrics
        stage.error_message = error
