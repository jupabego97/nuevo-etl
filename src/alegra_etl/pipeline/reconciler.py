"""Reconciliación contra Alegra."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient
from alegra_etl.alegra.resources import SyncStrategy, resource_by_name
from alegra_etl.config import Settings
from alegra_etl.db.models import FactPurchaseBill, FactSalesInvoice
from alegra_etl.pipeline.runner import PipelineRunner

logger = logging.getLogger(__name__)


class Reconciler:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
        self.runner = PipelineRunner(settings, session)

    async def reconcile_resource(self, resource_name: str, days: int = 30) -> dict:
        resource = resource_by_name(resource_name)
        if not resource:
            raise ValueError(f"Recurso desconocido: {resource_name}")
        if resource.strategy != SyncStrategy.DATE_WINDOW:
            result = await self.runner.run_single_resource(resource)
            return {"resource": resource_name, "action": "full_refresh", **result}

        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        api_counts: dict[str, int] = {}
        db_counts: dict[str, int] = {}

        async with AlegraClient(self.settings) as client:
            current = start_date
            while current <= end_date:
                records = await client.get_by_date(
                    resource.endpoint,
                    current.isoformat(),
                    extra_params=resource.extra_params,
                )
                api_counts[current.isoformat()] = len(records)
                db_counts[current.isoformat()] = self._db_count_for_date(resource.name, current)
                current += timedelta(days=1)

        mismatches = {
            day: {"api": api_counts[day], "db": db_counts[day]}
            for day in api_counts
            if api_counts[day] != db_counts[day]
        }
        if mismatches:
            logger.warning("Inconsistencias detectadas en %s: %s", resource_name, mismatches)
            await self.runner.run_single_resource(resource, start_date=start_date, end_date=end_date)
        return {"resource": resource_name, "mismatches": mismatches, "reprocessed": bool(mismatches)}

    def _db_count_for_date(self, resource_name: str, target_date: date) -> int:
        if resource_name == "invoices":
            return self.session.scalar(
                select(func.count()).select_from(FactSalesInvoice).where(
                    FactSalesInvoice.company_id == self.settings.company_id,
                    FactSalesInvoice.invoice_date == target_date,
                )
            ) or 0
        if resource_name == "bills":
            return self.session.scalar(
                select(func.count()).select_from(FactPurchaseBill).where(
                    FactPurchaseBill.company_id == self.settings.company_id,
                    FactPurchaseBill.bill_date == target_date,
                )
            ) or 0
        return 0
