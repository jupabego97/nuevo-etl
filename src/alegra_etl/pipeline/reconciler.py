"""Reconciliación API → source → tipado."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient
from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy, resource_by_name
from alegra_etl.config import Settings
from alegra_etl.db.models import EtlParseSkip, SourceDocument
from alegra_etl.db.models.backfill import BackfillWorkItem
from alegra_etl.pipeline.backfill_work import seed_work_items
from alegra_etl.pipeline.replay_source import replay_source_documents
from alegra_etl.pipeline.resource_coverage import (
    RESOURCE_TYPED_MAP,
    count_source_ids,
    count_typed_ids,
)

logger = logging.getLogger(__name__)


class Reconciler:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session

    @property
    def runner(self):
        from alegra_etl.pipeline.runner import PipelineRunner

        return PipelineRunner(self.settings, self.session)

    async def reconcile_resource(
        self,
        resource_name: str,
        *,
        days: int = 30,
        start_date: date | None = None,
        end_date: date | None = None,
        reprocess: bool = True,
    ) -> dict[str, Any]:
        resource = resource_by_name(resource_name)
        if not resource:
            raise ValueError(f"Recurso desconocido: {resource_name}")

        if resource.strategy != SyncStrategy.DATE_WINDOW:
            result = await self.runner.run_single_resource(resource)
            db_report = self._db_coverage_report(resource_name)
            return {
                "resource": resource_name,
                "action": "full_refresh",
                "db_coverage": db_report,
                **result,
            }

        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=days))
        api_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        typed_counts: dict[str, int] = {}

        async with AlegraClient(self.settings) as client:
            current = start_date
            while current <= end_date:
                day_key = current.isoformat()
                records = await client.get_by_date(
                    resource.endpoint,
                    day_key,
                    extra_params=resource.extra_params,
                )
                api_counts[day_key] = len(records)
                source_counts[day_key] = self._source_count_for_date(resource_name, current)
                typed_counts[day_key] = self._typed_count_for_date(resource_name, current)
                current += timedelta(days=1)

        mismatches = {
            day: {
                "api": api_counts[day],
                "source": source_counts[day],
                "typed": typed_counts[day],
            }
            for day in api_counts
            if not (
                api_counts[day] == source_counts[day] == typed_counts[day]
                or (api_counts[day] == source_counts[day] and not resource.has_typed_loader)
            )
        }

        actions: list[str] = []
        if mismatches and reprocess:
            logger.warning("Inconsistencias en %s: %s", resource_name, mismatches)
            await self.runner.run_single_resource(
                resource, start_date=start_date, end_date=end_date
            )
            actions.append("reextracted")
            replay = replay_source_documents(
                self.session, self.settings, resource_name=resource_name
            )
            actions.append(f"replay:{replay.get('typed_upserted', 0)}")

        return {
            "resource": resource_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "mismatches": mismatches,
            "reprocessed": bool(mismatches and reprocess),
            "actions": actions,
            "db_coverage": self._db_coverage_report(resource_name, start_date, end_date),
        }

    async def reconcile_checkpoint(self, resource_name: str) -> dict[str, Any]:
        """Reconciliación completa del rango del checkpoint y reencola unidades discrepantes."""
        from alegra_etl.db.models import SyncCheckpoint

        resource = resource_by_name(resource_name)
        if not resource:
            raise ValueError(f"Recurso desconocido: {resource_name}")

        checkpoint = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.settings.company_id, resource_name=resource_name)
            .one_or_none()
        )
        if not checkpoint:
            return {"resource": resource_name, "status": "no_checkpoint"}

        start = checkpoint.backfill_start_date
        end = checkpoint.backfill_end_date or date.today()
        report = await self.reconcile_resource(
            resource_name,
            start_date=start,
            end_date=end,
            reprocess=True,
        )

        requeued = 0
        if resource.strategy == SyncStrategy.DATE_WINDOW and report.get("mismatches"):
            for day in report["mismatches"]:
                work_date = date.fromisoformat(day)
                item = (
                    self.session.query(BackfillWorkItem)
                    .filter_by(
                        company_id=self.settings.company_id,
                        resource_name=resource_name,
                        work_key=work_date.isoformat(),
                    )
                    .one_or_none()
                )
                if item:
                    item.status = "pending"
                    item.verified_at = None
                    item.error_message = "reconciliation_mismatch"
                    requeued += 1
                else:
                    seed_work_items(self.settings, self.session, resource, checkpoint)
                    requeued += 1
            checkpoint.status = "pending"
            checkpoint.backfill_completed_at = None
            checkpoint.verified_at = None

        can_complete = False
        if checkpoint:
            from alegra_etl.pipeline.completion_gate import can_mark_backfill_completed

            can_complete = can_mark_backfill_completed(
                self.session, self.settings, checkpoint, resource
            )
        self.session.flush()
        return {
            **report,
            "requeued_work_items": requeued,
            "can_mark_completed": can_complete,
        }

    def _source_count_for_date(self, resource_name: str, target_date: date) -> int:
        return (
            self.session.scalar(
                select(func.count(func.distinct(SourceDocument.alegra_id))).where(
                    SourceDocument.company_id == self.settings.company_id,
                    SourceDocument.resource_name == resource_name,
                    SourceDocument.document_date == target_date,
                    SourceDocument.deleted_at.is_(None),
                )
            )
            or 0
        )

    def _typed_count_for_date(self, resource_name: str, target_date: date) -> int:
        mapping = RESOURCE_TYPED_MAP.get(resource_name)
        if not mapping or not mapping.date_column:
            return 0
        model = mapping.model
        date_col = getattr(model, mapping.date_column)
        return (
            self.session.scalar(
                select(func.count(func.distinct(getattr(model, mapping.id_column)))).where(
                    model.company_id == self.settings.company_id,
                    date_col == target_date,
                    model.deleted_at.is_(None),
                )
            )
            or 0
        )

    def _db_coverage_report(
        self,
        resource_name: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        resource = resource_by_name(resource_name)
        source = count_source_ids(
            self.session, self.settings.company_id, resource_name, start_date, end_date
        )
        typed = count_typed_ids(
            self.session, self.settings.company_id, resource_name, start_date, end_date
        )
        skips = (
            self.session.query(EtlParseSkip)
            .filter_by(company_id=self.settings.company_id, resource_name=resource_name)
            .count()
        )
        return {
            "source_ids": source,
            "typed_ids": typed,
            "parse_skips": skips,
            "aligned": source == typed if resource and resource.has_typed_loader else True,
        }
