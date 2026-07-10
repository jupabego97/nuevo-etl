"""Orquestación de runs ETL."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient, AlegraClientError
from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy, get_enabled_resources
from alegra_etl.config import Settings
from alegra_etl.db.models import EtlRun, EtlStageRun, SyncCheckpoint
from alegra_etl.pipeline.extractor import ResourceExtractor
from alegra_etl.quality.checks import run_quality_checks

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session

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

    async def run_backfill(self) -> uuid.UUID:
        run = self._create_run("backfill")
        metrics: dict[str, Any] = {"resources": {}}
        start_date = date(2022, 1, 1)
        end_date = date.today()

        async with AlegraClient(self.settings) as client:
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            for resource in get_enabled_resources(self.settings):
                stage = self._start_stage(run, resource.name)
                try:
                    result = await self._extract_with_strategy(
                        extractor, resource, start_date=start_date, end_date=end_date
                    )
                    self._finish_stage(stage, "success", result)
                    self._update_checkpoint(resource.name, run.id)
                    metrics["resources"][resource.name] = result
                except AlegraClientError as exc:
                    if exc.status_code in {400, 403, 404}:
                        self._finish_stage(stage, "skipped", {"reason": str(exc)})
                        metrics["resources"][resource.name] = {"status": "skipped"}
                    else:
                        self._finish_stage(stage, "failed", {}, str(exc))
                        self._finish_run(run, "failed", metrics, str(exc))
                        raise
                except Exception as exc:
                    self._finish_stage(stage, "failed", {}, str(exc))
                    self._finish_run(run, "failed", metrics, str(exc))
                    raise

        quality = run_quality_checks(self.session, run.id, self.settings.company_id)
        metrics["quality"] = quality
        self._finish_run(run, "success", metrics)
        return run.id

    async def run_daily_sync(self) -> uuid.UUID:
        print("[sync] Creando registro etl_run...", flush=True)
        run = self._create_run("daily_sync")
        self.session.commit()
        print(f"[sync] run_id={run.id}", flush=True)
        metrics: dict[str, Any] = {"resources": {}}
        end_date = date.today()
        start_date = end_date - timedelta(days=self.settings.sync_overlap_days)
        resources = get_enabled_resources(self.settings)
        print(f"[sync] Recursos habilitados: {len(resources)}", flush=True)

        print("[sync] Abriendo cliente Alegra...", flush=True)
        async with AlegraClient(self.settings) as client:
            print("[sync] Cliente Alegra listo", flush=True)
            extractor = ResourceExtractor(self.settings, client, self.session, run.id)
            for index, resource in enumerate(resources, start=1):
                print(
                    f"[sync] ({index}/{len(resources)}) Extrayendo {resource.name} "
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
                    self._update_checkpoint(resource.name, run.id)
                    self.session.commit()
                    metrics["resources"][resource.name] = result
                    print(
                        f"[sync] {resource.name} OK extracted={result.get('extracted', 0)} "
                        f"loaded={result.get('loaded', 0)}",
                        flush=True,
                    )
                except AlegraClientError as exc:
                    # 403/400/404: recurso no disponible o params no soportados → skip, no tumbar el run
                    if exc.status_code in {400, 403, 404}:
                        self._finish_stage(stage, "skipped", {"reason": str(exc), "status": exc.status_code})
                        self.session.commit()
                        metrics["resources"][resource.name] = {
                            "status": "skipped",
                            "reason": str(exc),
                        }
                        print(
                            f"[sync] {resource.name} skipped ({exc.status_code})",
                            flush=True,
                        )
                    else:
                        self._finish_stage(stage, "failed", {}, str(exc))
                        self._finish_run(run, "failed", metrics, str(exc))
                        self.session.commit()
                        print(f"[sync] {resource.name} FAILED: {exc}", flush=True)
                        raise
                except Exception as exc:
                    self._finish_stage(stage, "failed", {}, str(exc))
                    self._finish_run(run, "failed", metrics, str(exc))
                    self.session.commit()
                    print(f"[sync] {resource.name} FAILED: {exc}", flush=True)
                    raise

        print("[sync] Controles de calidad...", flush=True)
        quality = run_quality_checks(self.session, run.id, self.settings.company_id)
        metrics["quality"] = quality
        self._finish_run(run, "success", metrics)
        self.session.commit()
        print("[sync] daily-sync terminado OK", flush=True)
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
            self._update_checkpoint(resource.name, run.id)
            self._finish_run(run, "success", {"resource": resource.name, **result})
            return result

    async def _extract_with_strategy(
        self,
        extractor: ResourceExtractor,
        resource: ResourceDefinition,
        *,
        start_date: date | None,
        end_date: date | None,
    ) -> dict[str, int]:
        return await extractor.extract_resource(resource, start_date=start_date, end_date=end_date)

    def _start_stage(self, run: EtlRun, resource_name: str) -> EtlStageRun:
        stage = EtlStageRun(run_id=run.id, stage_name="extract_load", resource_name=resource_name, status="running")
        self.session.add(stage)
        self.session.flush()
        return stage

    def _finish_stage(self, stage: EtlStageRun, status: str, metrics: dict[str, Any], error: str | None = None) -> None:
        stage.status = status
        stage.records_extracted = metrics.get("extracted", 0)
        stage.records_loaded = metrics.get("loaded", 0)
        stage.metrics = metrics
        stage.error_message = error

    def _update_checkpoint(self, resource_name: str, run_id: uuid.UUID) -> None:
        checkpoint = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.settings.company_id, resource_name=resource_name)
            .one_or_none()
        )
        now = datetime.now(UTC)
        if checkpoint is None:
            checkpoint = SyncCheckpoint(
                company_id=self.settings.company_id,
                resource_name=resource_name,
                last_successful_run_id=run_id,
                last_synced_at=now,
            )
            self.session.add(checkpoint)
        else:
            checkpoint.last_successful_run_id = run_id
            checkpoint.last_synced_at = now
