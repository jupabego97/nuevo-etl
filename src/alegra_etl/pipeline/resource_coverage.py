"""Mapeo recurso → tabla tipada y conteos de cobertura."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from alegra_etl.db.models import (
    DimContact,
    DimItem,
    DimSeller,
    DimTax,
    DimWarehouse,
    FactBankAccount,
    FactCreditNote,
    FactIncomePayment,
    FactInventoryAdjustment,
    FactPurchaseBill,
    FactPurchaseOrder,
    FactSalesInvoice,
    FactWarehouseTransfer,
    SourceDocument,
)


class TypedResourceMapping:
    def __init__(
        self,
        model: Any,
        *,
        date_column: str | None = None,
        id_column: str = "alegra_id",
    ):
        self.model = model
        self.date_column = date_column
        self.id_column = id_column


RESOURCE_TYPED_MAP: dict[str, TypedResourceMapping] = {
    "invoices": TypedResourceMapping(FactSalesInvoice, date_column="invoice_date"),
    "bills": TypedResourceMapping(FactPurchaseBill, date_column="bill_date"),
    "credit-notes": TypedResourceMapping(FactCreditNote, date_column="note_date"),
    "payments-income": TypedResourceMapping(FactIncomePayment, date_column="payment_date"),
    "purchase-orders": TypedResourceMapping(FactPurchaseOrder, date_column="order_date"),
    "inventory-adjustments": TypedResourceMapping(
        FactInventoryAdjustment, date_column="adjustment_date"
    ),
    "warehouse-transfers": TypedResourceMapping(
        FactWarehouseTransfer, date_column="transfer_date"
    ),
    "bank-accounts": TypedResourceMapping(FactBankAccount),
    "items": TypedResourceMapping(DimItem),
    "contacts": TypedResourceMapping(DimContact),
    "sellers": TypedResourceMapping(DimSeller),
    "warehouses": TypedResourceMapping(DimWarehouse),
    "taxes": TypedResourceMapping(DimTax),
}


def count_source_ids(
    session: Session,
    company_id: int,
    resource_name: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    query = select(func.count(func.distinct(SourceDocument.alegra_id))).where(
        SourceDocument.company_id == company_id,
        SourceDocument.resource_name == resource_name,
        SourceDocument.deleted_at.is_(None),
    )
    if start_date and end_date:
        query = query.where(
            SourceDocument.document_date >= start_date,
            SourceDocument.document_date <= end_date,
        )
    return session.scalar(query) or 0


def count_typed_ids(
    session: Session,
    company_id: int,
    resource_name: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
    mapping = RESOURCE_TYPED_MAP.get(resource_name)
    if not mapping:
        return 0
    model = mapping.model
    query = select(func.count(func.distinct(getattr(model, mapping.id_column)))).where(
        model.company_id == company_id,
        model.deleted_at.is_(None),
    )
    if start_date and end_date and mapping.date_column:
        date_col = getattr(model, mapping.date_column)
        query = query.where(date_col >= start_date, date_col <= end_date)
    return session.scalar(query) or 0
