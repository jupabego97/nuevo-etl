"""Worker de backfill concurrente por unidades de trabajo."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient
from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy, get_backfill_resources, resource_by_name
from alegra_etl.config import Settings
from alegra_etl.db.models import EtlRun, SyncCheckpoint
from alegra_etl.pipeline.advisory_lock import release_backfill_lock, try_acquire_backfill_lock
from alegra_etl.pipeline.backfill_work import (
    all_work_verified,
    claim_work_items,
    mark_work_failed,
    mark_work_verified,
    seed_all_pending_work,
    work_progress,
)
from alegra_etl.pipeline.checkpoint import CheckpointManager
from alegra_etl.pipeline.extractor import ResourceExtractor

logger = logging.getLogger(__name__)


class BackfillWorkerRunner:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session

    async def run_batch(self, *, resource_name: str | None = None) -> dict[str, Any]:
        run = EtlRun(run_type="backfill_workers", status="running", metrics={})
        self.session.add(run)
        self.session.flush()
        owner = f"worker-{run.id}"

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
            return {"status": "no_work", "seeded": seed_counts, "progress": progress, "run_id": str(run.id)}

        processed = 0
        failed = 0
        metrics: dict[str, Any] = {"items": {}}

        async with AlegraClient(self.settings) as client:
            # Agrupar por recurso para advisory lock
            by_resource: dict[str, list] = {}
            for item in items:
                by_resource.setdefault(item.resource_name, []).append(item)

            async def _process_resource_group(res_name: str, group_items: list) -> None:
                nonlocal processed, failed
                resource = resource_by_name(res_name)
                if not resource:
                    for item in group_items:
                        mark_work_failed(item, f"recurso desconocido: {res_name}")
                    failed += len(group_items)
                    return

                if not try_acquire_backfill_lock(self.session, self.settings.company_id, res_name):
                    for item in group_items:
                        item.status = "pending"
                        item.lease_owner = None
                    return

                try:
                    extractor = ResourceExtractor(
                        self.settings, client, self.session, run.id, strict_backfill=True
                    )
                    for item in group_items:
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
                            metrics["items"][item.work_key] = result
                            processed += 1
                            self.session.commit()
                        except Exception as exc:
                            mark_work_failed(item, str(exc))
                            failed += 1
                            self.session.commit()
                            logger.exception("Fallo work item %s/%s", res_name, item.work_key)

                    # Actualizar checkpoint del recurso si todo verificado
                    if all_work_verified(self.session, self.settings.company_id, res_name):
                        cp = (
                            self.session.query(SyncCheckpoint)
                            .filter_by(
                                company_id=self.settings.company_id,
                                resource_name=res_name,
                            )
                            .one_or_none()
                        )
                        if cp and resource.strategy == SyncStrategy.DATE_WINDOW:
                            from datetime import timedelta

                            end = cp.backfill_end_date or date.today()
                            cp.cursor_date = end + timedelta(days=1)
                            cp.cursor_offset = 0
                            manager = CheckpointManager(self.settings, self.session)
                            if not manager.try_mark_backfill_completed(cp, resource):
                                cp.status = "pending"
                                cp.backfill_completed_at = None
                                cp.verified_at = None
                            self.session.commit()
                finally:
                    release_backfill_lock(self.session, self.settings.company_id, res_name)

            tasks = [
                _process_resource_group(name, group)
                for name, group in by_resource.items()
            ]
            await asyncio.gather(*tasks)

        run.status = "success"
        run.metrics = {"processed": processed, "failed": failed, **metrics}
        from datetime import UTC, datetime

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
