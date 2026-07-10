"""Extracción paginada y persistencia raw."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient, hash_payload, hash_request
from alegra_etl.alegra.parsers import (
    parse_contacts,
    parse_credit_notes,
    parse_items,
    parse_purchase_bills,
    parse_sales_invoices,
    parse_simple_dimension,
)
from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy
from alegra_etl.config import Settings
from alegra_etl.db.models import (
    DimContact,
    DimItem,
    DimItemInventory,
    DimItemPrice,
    DimSeller,
    DimTax,
    DimWarehouse,
    FactCreditNote,
    FactCreditNoteLine,
    FactIncomePayment,
    FactIncomePaymentApplication,
    FactPurchaseBill,
    FactPurchaseBillLine,
    FactSalesInvoice,
    FactSalesInvoiceLine,
    RawDocument,
)
from alegra_etl.pipeline.loader import upsert_rows

logger = logging.getLogger(__name__)


class ResourceExtractor:
    def __init__(self, settings: Settings, client: AlegraClient, session: Session, run_id: uuid.UUID):
        self.settings = settings
        self.client = client
        self.session = session
        self.run_id = run_id
        self.company_id = settings.company_id

    async def extract_resource(
        self,
        resource: ResourceDefinition,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, int]:
        if resource.strategy == SyncStrategy.FULL:
            records = await self.client.fetch_all_pages(
                resource.endpoint,
                extra_params=resource.extra_params,
                order_field=resource.order_field,
                order_direction=resource.order_direction,
            )
            await self._store_raw(resource, 0, resource.extra_params, records)
            loaded = self._transform_and_load(resource.name, records)
            return {"extracted": len(records), "loaded": loaded}

        if resource.strategy == SyncStrategy.DATE_WINDOW:
            if not start_date or not end_date:
                end_date = date.today()
                start_date = end_date - timedelta(days=self.settings.sync_overlap_days)
            all_records: list[dict[str, Any]] = []
            current = start_date
            while current <= end_date:
                day_records = await self.client.get_by_date(
                    resource.endpoint,
                    current.isoformat(),
                    extra_params=resource.extra_params,
                )
                params = {"date": current.isoformat(), **resource.extra_params}
                await self._store_raw(resource, 0, params, day_records)
                all_records.extend(day_records)
                current += timedelta(days=1)
            loaded = self._transform_and_load(resource.name, all_records)
            return {"extracted": len(all_records), "loaded": loaded}

        records = await self.client.fetch_all_pages(
            resource.endpoint,
            extra_params=resource.extra_params,
            order_field=resource.order_field,
            order_direction=resource.order_direction,
        )
        await self._store_raw(resource, 0, resource.extra_params, records)
        loaded = self._transform_and_load(resource.name, records)
        return {"extracted": len(records), "loaded": loaded}

    async def extract_resource_by_id(self, resource: ResourceDefinition, resource_id: str) -> dict[str, int]:
        if not resource.detail_endpoint_template:
            raise ValueError(f"Recurso {resource.name} no soporta extracción por ID")
        record = await self.client.get_by_id(resource.detail_endpoint_template, resource_id)
        await self._store_raw(resource, 0, {"id": resource_id}, [record])
        loaded = self._transform_and_load(resource.name, [record])
        return {"extracted": 1, "loaded": loaded}

    async def _store_raw(
        self,
        resource: ResourceDefinition,
        page_start: int,
        params: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> None:
        payload = {"records": records, "count": len(records)}
        raw = RawDocument(
            run_id=self.run_id,
            resource_name=resource.name,
            endpoint=resource.endpoint,
            request_params=params,
            request_hash=hash_request(params),
            page_start=page_start,
            http_status=200,
            payload=payload,
            payload_hash=hash_payload(payload),
            extracted_at=datetime.now(UTC),
        )
        self.session.merge(raw)

    def _transform_and_load(self, resource_name: str, records: list[dict[str, Any]]) -> int:
        if resource_name == "items":
            items, prices, inventories = parse_items(records, self.company_id)
            loaded = 0
            if items:
                loaded = upsert_rows(
                    self.session,
                    DimItem.__table__,
                    items,
                    ["company_id", "alegra_id"],
                    update_columns=[c for c in items[0].keys()],
                )
            upsert_rows(self.session, DimItemPrice.__table__, prices, ["company_id", "item_alegra_id", "price_list_id"])
            upsert_rows(
                self.session,
                DimItemInventory.__table__,
                inventories,
                ["company_id", "item_alegra_id", "warehouse_alegra_id"],
            )
            return loaded

        if resource_name == "contacts":
            rows = parse_contacts(records, self.company_id)
            return upsert_rows(self.session, DimContact.__table__, rows, ["company_id", "alegra_id"])

        if resource_name == "sellers":
            rows = parse_simple_dimension(records, self.company_id)
            return upsert_rows(self.session, DimSeller.__table__, rows, ["company_id", "alegra_id"])

        if resource_name == "warehouses":
            rows = parse_simple_dimension(records, self.company_id)
            return upsert_rows(self.session, DimWarehouse.__table__, rows, ["company_id", "alegra_id"])

        if resource_name == "taxes":
            rows = []
            for record in records:
                alegra_id = str(record.get("id"))
                rows.append(
                    {
                        "company_id": self.company_id,
                        "alegra_id": alegra_id,
                        "name": record.get("name") or alegra_id,
                        "percentage": record.get("percentage"),
                        "tax_type": record.get("type"),
                        "status": record.get("status"),
                    }
                )
            return upsert_rows(self.session, DimTax.__table__, rows, ["company_id", "alegra_id"])

        if resource_name == "invoices":
            headers, lines, payments, applications = parse_sales_invoices(records, self.company_id)
            loaded = upsert_rows(self.session, FactSalesInvoice.__table__, headers, ["company_id", "alegra_id"])
            upsert_rows(
                self.session,
                FactSalesInvoiceLine.__table__,
                lines,
                ["company_id", "invoice_alegra_id", "line_number"],
            )
            upsert_rows(self.session, FactIncomePayment.__table__, payments, ["company_id", "alegra_id"])
            upsert_rows(
                self.session,
                FactIncomePaymentApplication.__table__,
                applications,
                ["company_id", "payment_alegra_id", "invoice_alegra_id"],
            )
            return loaded

        if resource_name == "bills":
            headers, lines = parse_purchase_bills(records, self.company_id)
            loaded = upsert_rows(self.session, FactPurchaseBill.__table__, headers, ["company_id", "alegra_id"])
            upsert_rows(
                self.session,
                FactPurchaseBillLine.__table__,
                lines,
                ["company_id", "bill_alegra_id", "line_number"],
            )
            return loaded

        if resource_name == "credit-notes":
            headers, lines = parse_credit_notes(records, self.company_id)
            loaded = upsert_rows(self.session, FactCreditNote.__table__, headers, ["company_id", "alegra_id"])
            upsert_rows(
                self.session,
                FactCreditNoteLine.__table__,
                lines,
                ["company_id", "credit_note_alegra_id", "line_number"],
            )
            return loaded

        logger.info("Recurso %s extraído en raw (%s registros) sin transformador dedicado", resource_name, len(records))
        return len(records)
