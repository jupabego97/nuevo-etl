"""Transformación tipada con reconciliación de líneas."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, update
from sqlalchemy.orm import Session

from alegra_etl.alegra.parsers import (
    parse_contacts,
    parse_credit_notes,
    parse_items,
    parse_purchase_bills,
    parse_sales_invoices,
    parse_simple_dimension,
)
from alegra_etl.alegra.resources import ResourceDefinition
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
)
from alegra_etl.pipeline.loader import upsert_rows

logger = logging.getLogger(__name__)


def _reconcile_child_lines(
    session: Session,
    table: Any,
    *,
    company_id: int,
    parent_column: str,
    parent_id: str,
    line_column: str,
    current_line_numbers: set[int],
) -> int:
    from sqlalchemy import select

    stmt = select(getattr(table.c, line_column)).where(
        table.c.company_id == company_id,
        getattr(table.c, parent_column) == parent_id,
    )
    existing = set(session.execute(stmt).scalars().all())
    to_delete = existing - current_line_numbers
    if not to_delete:
        return 0
    del_stmt = delete(table).where(
        table.c.company_id == company_id,
        getattr(table.c, parent_column) == parent_id,
        getattr(table.c, line_column).in_(to_delete),
    )
    result = session.execute(del_stmt)
    return result.rowcount or len(to_delete)


def transform_and_load(
    session: Session,
    resource: ResourceDefinition,
    records: list[dict[str, Any]],
    company_id: int,
) -> int:
    if not resource.has_typed_loader or not records:
        return 0

    parser = resource.parser
    if parser == "items":
        items, prices, inventories = parse_items(records, company_id)
        loaded = 0
        if items:
            loaded = upsert_rows(
                session,
                DimItem.__table__,
                items,
                ["company_id", "alegra_id"],
                update_columns=[c for c in items[0].keys()],
            )
        upsert_rows(
            session,
            DimItemPrice.__table__,
            prices,
            ["company_id", "item_alegra_id", "price_list_id"],
        )
        upsert_rows(
            session,
            DimItemInventory.__table__,
            inventories,
            ["company_id", "item_alegra_id", "warehouse_alegra_id"],
        )
        return loaded

    if parser == "contacts":
        rows = parse_contacts(records, company_id)
        return upsert_rows(session, DimContact.__table__, rows, ["company_id", "alegra_id"])

    if parser == "sellers":
        rows = parse_simple_dimension(records, company_id)
        return upsert_rows(session, DimSeller.__table__, rows, ["company_id", "alegra_id"])

    if parser == "warehouses":
        rows = parse_simple_dimension(records, company_id)
        return upsert_rows(session, DimWarehouse.__table__, rows, ["company_id", "alegra_id"])

    if parser == "taxes":
        rows = []
        for record in records:
            alegra_id = str(record.get("id"))
            rows.append(
                {
                    "company_id": company_id,
                    "alegra_id": alegra_id,
                    "name": record.get("name") or alegra_id,
                    "percentage": record.get("percentage"),
                    "tax_type": record.get("type"),
                    "status": record.get("status"),
                }
            )
        return upsert_rows(session, DimTax.__table__, rows, ["company_id", "alegra_id"])

    if parser == "invoices":
        return _load_invoices(session, records, company_id)

    if parser == "bills":
        return _load_bills(session, records, company_id)

    if parser == "credit_notes":
        return _load_credit_notes(session, records, company_id)

    if parser == "payments_income":
        return _load_payments_income(session, records, company_id)

    logger.warning("Parser %s no implementado para %s", parser, resource.name)
    return 0


def _load_invoices(session: Session, records: list[dict[str, Any]], company_id: int) -> int:
    headers, lines, payments, applications = parse_sales_invoices(records, company_id)
    loaded = upsert_rows(session, FactSalesInvoice.__table__, headers, ["company_id", "alegra_id"])
    upsert_rows(
        session,
        FactSalesInvoiceLine.__table__,
        lines,
        ["company_id", "invoice_alegra_id", "line_number"],
    )
    upsert_rows(session, FactIncomePayment.__table__, payments, ["company_id", "alegra_id"])
    upsert_rows(
        session,
        FactIncomePaymentApplication.__table__,
        applications,
        ["company_id", "payment_alegra_id", "invoice_alegra_id"],
    )
    for invoice_id in {h["alegra_id"] for h in headers}:
        current_lines = {
            ln["line_number"]
            for ln in lines
            if ln["invoice_alegra_id"] == invoice_id
        }
        _reconcile_child_lines(
            session,
            FactSalesInvoiceLine.__table__,
            company_id=company_id,
            parent_column="invoice_alegra_id",
            parent_id=invoice_id,
            line_column="line_number",
            current_line_numbers=current_lines,
        )
    return loaded


def _load_bills(session: Session, records: list[dict[str, Any]], company_id: int) -> int:
    headers, lines = parse_purchase_bills(records, company_id)
    loaded = upsert_rows(session, FactPurchaseBill.__table__, headers, ["company_id", "alegra_id"])
    upsert_rows(
        session,
        FactPurchaseBillLine.__table__,
        lines,
        ["company_id", "bill_alegra_id", "line_number"],
    )
    for bill_id in {h["alegra_id"] for h in headers}:
        current_lines = {
            ln["line_number"]
            for ln in lines
            if ln["bill_alegra_id"] == bill_id
        }
        _reconcile_child_lines(
            session,
            FactPurchaseBillLine.__table__,
            company_id=company_id,
            parent_column="bill_alegra_id",
            parent_id=bill_id,
            line_column="line_number",
            current_line_numbers=current_lines,
        )
    return loaded


def _load_credit_notes(session: Session, records: list[dict[str, Any]], company_id: int) -> int:
    headers, lines = parse_credit_notes(records, company_id)
    loaded = upsert_rows(session, FactCreditNote.__table__, headers, ["company_id", "alegra_id"])
    upsert_rows(
        session,
        FactCreditNoteLine.__table__,
        lines,
        ["company_id", "credit_note_alegra_id", "line_number"],
    )
    for note_id in {h["alegra_id"] for h in headers}:
        current_lines = {
            ln["line_number"]
            for ln in lines
            if ln["credit_note_alegra_id"] == note_id
        }
        _reconcile_child_lines(
            session,
            FactCreditNoteLine.__table__,
            company_id=company_id,
            parent_column="credit_note_alegra_id",
            parent_id=note_id,
            line_column="line_number",
            current_line_numbers=current_lines,
        )
    return loaded


def _load_payments_income(session: Session, records: list[dict[str, Any]], company_id: int) -> int:
    rows = []
    for record in records:
        alegra_id = record.get("id")
        if alegra_id is None:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": str(alegra_id),
                "payment_date": record.get("date"),
                "amount": record.get("amount"),
                "payment_method": record.get("paymentMethod"),
                "status": record.get("status"),
                "client_alegra_id": str(record["client"]["id"])
                if isinstance(record.get("client"), dict)
                else None,
                "bank_account_alegra_id": str(record["bankAccount"]["id"])
                if isinstance(record.get("bankAccount"), dict)
                else None,
                "currency_code": record.get("currency"),
                "exchange_rate": record.get("exchangeRate"),
                "raw_json": record,
            }
        )
    return upsert_rows(session, FactIncomePayment.__table__, rows, ["company_id", "alegra_id"])


def soft_delete_typed_document(
    session: Session,
    resource: ResourceDefinition,
    alegra_id: str,
    company_id: int,
) -> None:
    now = datetime.now(UTC)
    table_map = {
        "invoices": FactSalesInvoice,
        "bills": FactPurchaseBill,
        "credit-notes": FactCreditNote,
        "items": DimItem,
        "contacts": DimContact,
    }
    model = table_map.get(resource.name)
    if not model:
        return
    session.execute(
        update(model)
        .where(
            model.company_id == company_id,
            model.alegra_id == alegra_id,
        )
        .values(deleted_at=now)
    )
