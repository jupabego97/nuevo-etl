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
from alegra_etl.db.models import (
    EtlParseSkip,
    FactCreditNoteLine,
    FactPurchaseBillLine,
    FactSalesInvoiceLine,
    SourceDocument,
)
from alegra_etl.db.models.backfill import BackfillWorkItem
from alegra_etl.pipeline.backfill_work import seed_work_items
from alegra_etl.pipeline.concurrent_fetch import fetch_page_batch
from alegra_etl.pipeline.replay_source import replay_source_documents
from alegra_etl.pipeline.resource_coverage import (
    RESOURCE_TYPED_MAP,
    count_source_ids,
    count_typed_ids,
    source_ids,
    typed_ids,
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
            if reprocess:
                result = await self.runner.run_single_resource(resource)
            else:
                result = {}
            report = await self._reconcile_full_resource(resource)
            if report["mismatches"] and reprocess:
                result = {
                    **result,
                    "verification_after_reprocess": await self._reconcile_full_resource(resource),
                }
            return {
                "resource": resource_name,
                "action": "full_refresh" if reprocess else "verify_only",
                "db_coverage": self._db_coverage_report(resource_name),
                **report,
                **result,
            }

        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=days))
        api_counts: dict[str, Any] = {}
        source_counts: dict[str, int] = {}
        typed_counts: dict[str, int] = {}
        coverage: dict[str, dict[str, int]] = {}

        async with AlegraClient(self.settings) as client:
            current = start_date
            while current <= end_date:
                day_key = current.isoformat()
                records = await client.get_by_date(
                    resource.endpoint,
                    day_key,
                    extra_params=resource.extra_params,
                    fallback_remove_params=resource.fallback_remove_params,
                )
                api_counts[day_key] = len(records)
                api_ids = {
                    str(record.get("id")) for record in records if record.get("id") is not None
                }
                source_day_ids = self._source_ids_for_date(resource_name, current)
                typed_day_ids = self._typed_ids_for_date(resource_name, current)
                source_counts[day_key] = len(source_day_ids)
                typed_counts[day_key] = len(typed_day_ids)
                api_counts[f"{day_key}:distinct"] = len(api_ids)
                api_counts[f"{day_key}:missing_id"] = len(records) - len(api_ids)
                api_counts[f"{day_key}:api_ids"] = api_ids
                api_counts[f"{day_key}:source_ids"] = source_day_ids
                api_counts[f"{day_key}:typed_ids"] = typed_day_ids
                expected_children = self._expected_child_count(resource_name, records)
                typed_children = self._typed_child_count(resource_name, api_ids)
                api_counts[f"{day_key}:expected_children"] = expected_children
                api_counts[f"{day_key}:typed_children"] = typed_children
                coverage[day_key] = {
                    "api_records": len(records),
                    "api_distinct_ids": len(api_ids),
                    "source_distinct_ids": len(source_day_ids),
                    "typed_distinct_ids": len(typed_day_ids),
                }
                current += timedelta(days=1)

        mismatches = {
            day: {
                "api": api_counts[day],
                "api_distinct": api_counts[f"{day}:distinct"],
                "source": source_counts[day],
                "typed": typed_counts[day],
                "missing_in_source": sorted(
                    api_counts[f"{day}:api_ids"] - api_counts[f"{day}:source_ids"]
                )[:50],
                "extra_in_source": sorted(
                    api_counts[f"{day}:source_ids"] - api_counts[f"{day}:api_ids"]
                )[:50],
                "missing_in_typed": sorted(
                    api_counts[f"{day}:api_ids"] - api_counts[f"{day}:typed_ids"]
                )[:50],
                "extra_in_typed": sorted(
                    api_counts[f"{day}:typed_ids"] - api_counts[f"{day}:api_ids"]
                )[:50],
                "api_child_rows": api_counts[f"{day}:expected_children"],
                "typed_child_rows": api_counts[f"{day}:typed_children"],
            }
            for day in api_counts
            if ":" not in day
            if not (
                api_counts[f"{day}:missing_id"] == 0
                and api_counts[day] == api_counts[f"{day}:distinct"]
                and api_counts[f"{day}:api_ids"] == api_counts[f"{day}:source_ids"]
                and (
                    api_counts[f"{day}:api_ids"] == api_counts[f"{day}:typed_ids"]
                    if resource.has_typed_loader
                    else True
                )
                and api_counts[f"{day}:expected_children"] == api_counts[f"{day}:typed_children"]
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
            "coverage": coverage,
            "db_coverage": self._db_coverage_report(resource_name, start_date, end_date),
        }

    async def _reconcile_full_resource(self, resource: ResourceDefinition) -> dict[str, Any]:
        """Compara IDs completos sin acumular los payloads de la API en memoria."""
        api_ids: set[str] = set()
        api_records = 0
        missing_api_ids = 0
        expected_children = 0
        offset = 0

        async def collect(
            _offset: int,
            records: list[dict[str, Any]],
            _meta: dict[str, Any] | None,
        ) -> None:
            nonlocal api_records, missing_api_ids, expected_children
            api_records += len(records)
            expected_children += self._expected_child_count(resource.name, records)
            for record in records:
                value = record.get("id")
                if value is None:
                    missing_api_ids += 1
                else:
                    api_ids.add(str(value))

        async with AlegraClient(self.settings) as client:
            while True:
                result = await fetch_page_batch(
                    client,
                    resource.endpoint,
                    extra_params=resource.extra_params,
                    order_field=resource.order_field,
                    order_direction=resource.order_direction,
                    start_offset=offset,
                    max_pages=8,
                    on_page=collect,
                    allow_parallel=False,
                    supports_metadata=resource.supports_metadata,
                )
                if result.pages_fetched == 0:
                    break
                if result.completed:
                    break
                if result.next_offset <= offset:
                    return {
                        "api_records": api_records,
                        "api_distinct_ids": len(api_ids),
                        "mismatches": {"pagination": {"offset": offset}},
                    }
                offset = result.next_offset

        source_set = source_ids(self.session, self.settings.company_id, resource.name)
        typed_set = typed_ids(self.session, self.settings.company_id, resource.name)
        typed_children = self._typed_child_count(resource.name, api_ids)
        coverage = {
            "full": {
                "api_records": api_records,
                "api_distinct_ids": len(api_ids),
                "source_distinct_ids": len(source_set),
                "typed_distinct_ids": len(typed_set),
            }
        }
        mismatch = {
            "api_records": api_records,
            "api_distinct_ids": len(api_ids),
            "source_distinct_ids": len(source_set),
            "typed_distinct_ids": len(typed_set),
            "api_missing_id": missing_api_ids,
            "missing_in_source": sorted(api_ids - source_set)[:50],
            "extra_in_source": sorted(source_set - api_ids)[:50],
            "missing_in_typed": sorted(api_ids - typed_set)[:50],
            "extra_in_typed": sorted(typed_set - api_ids)[:50],
            "api_child_rows": expected_children,
            "typed_child_rows": typed_children,
        }
        has_mismatch = (
            missing_api_ids > 0
            or api_records != len(api_ids)
            or api_ids != source_set
            or (resource.has_typed_loader and api_ids != typed_set)
            or expected_children != typed_children
        )
        return {
            "api_records": api_records,
            "api_distinct_ids": len(api_ids),
            "mismatches": {"full": mismatch} if has_mismatch else {},
            "coverage": coverage,
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

    def _source_ids_for_date(self, resource_name: str, target_date: date) -> set[str]:
        return source_ids(
            self.session,
            self.settings.company_id,
            resource_name,
            target_date,
            target_date,
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

    def _typed_ids_for_date(self, resource_name: str, target_date: date) -> set[str]:
        return typed_ids(
            self.session,
            self.settings.company_id,
            resource_name,
            target_date,
            target_date,
        )

    @staticmethod
    def _expected_child_count(resource_name: str, records: list[dict[str, Any]]) -> int:
        if resource_name in {"invoices", "credit-notes"}:
            return sum(len(record.get("items") or []) for record in records)
        if resource_name == "bills":
            return sum(
                len((record.get("purchases") or {}).get("items") or [])
                + len((record.get("purchases") or {}).get("categories") or [])
                for record in records
            )
        return 0

    def _typed_child_count(self, resource_name: str, parent_ids: set[str]) -> int:
        if not parent_ids:
            return 0
        model: Any
        column: Any
        if resource_name == "invoices":
            model = FactSalesInvoiceLine
            column = FactSalesInvoiceLine.invoice_alegra_id
        elif resource_name == "bills":
            model = FactPurchaseBillLine
            column = FactPurchaseBillLine.bill_alegra_id
        elif resource_name == "credit-notes":
            model = FactCreditNoteLine
            column = FactCreditNoteLine.credit_note_alegra_id
        else:
            return 0
        return (
            self.session.scalar(
                select(func.count())
                .select_from(model)
                .where(
                    model.company_id == self.settings.company_id,
                    column.in_(parent_ids),
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
