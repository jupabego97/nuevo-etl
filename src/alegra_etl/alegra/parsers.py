"""Transformadores de payloads Alegra a filas normalizadas."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from alegra_etl.alegra.client import hash_payload


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _str_id(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _parse_date(value: Any):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _name(obj: Any) -> str | None:
    if isinstance(obj, dict):
        return obj.get("name")
    if isinstance(obj, str):
        return obj
    return None


def _id(obj: Any) -> str | None:
    if isinstance(obj, dict):
        return _str_id(obj.get("id"))
    return None


def parse_items(records: list[dict[str, Any]], company_id: int) -> tuple[list[dict], list[dict], list[dict]]:
    items: list[dict] = []
    prices: list[dict] = []
    inventories: list[dict] = []
    now = datetime.now(UTC)

    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        custom = record.get("customFields") or []
        barcode = family = None
        for field in custom:
            name = (field.get("name") or "").lower()
            if "barras" in name or "barcode" in name:
                barcode = field.get("value")
            if "familia" in name or "family" in name:
                family = field.get("value")

        inventory = record.get("inventory") or {}
        items.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "name": record.get("name") or "",
                "reference": record.get("reference"),
                "description": record.get("description"),
                "item_type": record.get("type"),
                "status": record.get("status"),
                "is_inventoriable": bool(inventory),
                "unit": inventory.get("unit") if isinstance(inventory, dict) else None,
                "unit_cost": _dec(inventory.get("unitCost")) if isinstance(inventory, dict) else None,
                "category_id": _id(record.get("category")),
                "category_name": _name(record.get("category")),
                "barcode": barcode,
                "family": family,
                "brand": record.get("brand"),
                "model": record.get("model"),
                "raw_json": record,
                "payload_hash": hash_payload(record),
                "first_seen_at": now,
                "last_seen_at": now,
            }
        )
        for price in record.get("price") or []:
            prices.append(
                {
                    "company_id": company_id,
                    "item_alegra_id": alegra_id,
                    "price_list_id": _str_id(price.get("idPriceList")) or "default",
                    "price_list_name": price.get("name"),
                    "price": _dec(price.get("price")),
                }
            )
        warehouses = inventory.get("warehouses") if isinstance(inventory, dict) else None
        if warehouses:
            for wh in warehouses:
                inventories.append(
                    {
                        "company_id": company_id,
                        "item_alegra_id": alegra_id,
                        "warehouse_alegra_id": _str_id(wh.get("id")) or "default",
                        "warehouse_name": wh.get("name"),
                        "available_quantity": _dec(wh.get("availableQuantity")),
                        "min_quantity": _dec(wh.get("minQuantity")),
                        "max_quantity": _dec(wh.get("maxQuantity")),
                        "snapshot_date": now.date(),
                    }
                )
        elif isinstance(inventory, dict) and inventory.get("availableQuantity") is not None:
            inventories.append(
                {
                    "company_id": company_id,
                    "item_alegra_id": alegra_id,
                    "warehouse_alegra_id": "default",
                    "warehouse_name": "Principal",
                    "available_quantity": _dec(inventory.get("availableQuantity")),
                    "min_quantity": None,
                    "max_quantity": None,
                    "snapshot_date": now.date(),
                }
            )
    return items, prices, inventories


def parse_contacts(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    now = datetime.now(UTC)
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        address = record.get("address") or {}
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "name": record.get("name") or "",
                "identification": record.get("identification"),
                "email": record.get("email"),
                "phone_primary": record.get("phonePrimary"),
                "contact_type": record.get("type"),
                "status": record.get("status"),
                "city": address.get("city") if isinstance(address, dict) else None,
                "raw_json": record,
                "payload_hash": hash_payload(record),
                "first_seen_at": now,
                "last_seen_at": now,
            }
        )
    return rows


def parse_sales_invoices(records: list[dict[str, Any]], company_id: int) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []
    payments: list[dict] = []
    applications: list[dict] = []

    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        number_template = record.get("numberTemplate") or {}
        invoice_number = None
        if isinstance(number_template, dict):
            prefix = number_template.get("prefix") or ""
            number = number_template.get("number")
            invoice_number = f"{prefix}{number}" if number is not None else prefix or None

        currency = record.get("currency") or {}
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "invoice_number": invoice_number,
                "invoice_date": _parse_date(record.get("date")),
                "due_date": _parse_date(record.get("dueDate")),
                "datetime_utc": record.get("datetime"),
                "status": record.get("status"),
                "client_alegra_id": _id(record.get("client")),
                "client_name": _name(record.get("client")),
                "seller_alegra_id": _id(record.get("seller")),
                "seller_name": _name(record.get("seller")),
                "warehouse_alegra_id": _id(record.get("warehouse")),
                "cost_center_alegra_id": _id(record.get("costCenter")),
                "currency_code": currency.get("code") if isinstance(currency, dict) else None,
                "exchange_rate": _dec(currency.get("exchangeRate")) if isinstance(currency, dict) else None,
                "subtotal": _dec(record.get("subtotal")),
                "discount": _dec(record.get("discount")),
                "tax_total": None,
                "retention_total": sum((_dec(r.get("amount")) or Decimal(0)) for r in record.get("retentions") or []),
                "invoice_total": _dec(record.get("total")),
                "total_paid": _dec(record.get("totalPaid")),
                "balance": _dec(record.get("balance")),
                "payment_form": record.get("paymentForm") or record.get("paymentMethod"),
                "raw_json": record,
                "payload_hash": hash_payload(record),
            }
        )
        for idx, item in enumerate(record.get("items") or [], start=1):
            lines.append(
                {
                    "company_id": company_id,
                    "invoice_alegra_id": alegra_id,
                    "line_number": idx,
                    "item_alegra_id": _str_id(item.get("id")),
                    "item_name": item.get("name"),
                    "quantity": _dec(item.get("quantity")),
                    "unit_price": _dec(item.get("price")),
                    "discount": _dec(item.get("discount")),
                    "tax_total": sum((_dec(t.get("amount")) or Decimal(0)) for t in item.get("tax") or []),
                    "line_subtotal": _dec(item.get("subtotal")),
                    "line_total": _dec(item.get("total")),
                    "raw_json": item,
                }
            )
        for payment in record.get("payments") or []:
            payment_id = _str_id(payment.get("id"))
            if not payment_id:
                continue
            payments.append(
                {
                    "company_id": company_id,
                    "alegra_id": payment_id,
                    "payment_date": _parse_date(payment.get("date")),
                    "amount": _dec(payment.get("amount")),
                    "payment_method": payment.get("paymentMethod"),
                    "status": payment.get("status"),
                    "client_alegra_id": _id(record.get("client")),
                    "raw_json": payment,
                }
            )
            applications.append(
                {
                    "company_id": company_id,
                    "payment_alegra_id": payment_id,
                    "invoice_alegra_id": alegra_id,
                    "amount_applied": _dec(payment.get("amount")),
                }
            )
    return headers, lines, payments, applications


def parse_purchase_bills(records: list[dict[str, Any]], company_id: int) -> tuple[list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []

    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        number_template = record.get("numberTemplate") or {}
        bill_number = None
        if isinstance(number_template, dict):
            bill_number = str(number_template.get("number")) if number_template.get("number") is not None else None
        currency = record.get("currency") or {}
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "bill_number": bill_number,
                "bill_date": _parse_date(record.get("date")),
                "due_date": _parse_date(record.get("dueDate")),
                "status": record.get("status"),
                "bill_type": record.get("type") or "bill",
                "provider_alegra_id": _id(record.get("provider")),
                "provider_name": _name(record.get("provider")),
                "warehouse_alegra_id": _id(record.get("warehouse")),
                "currency_code": currency.get("code") if isinstance(currency, dict) else None,
                "exchange_rate": _dec(currency.get("exchangeRate")) if isinstance(currency, dict) else None,
                "bill_total": _dec(record.get("total")),
                "total_paid": _dec(record.get("totalPaid")),
                "balance": _dec(record.get("balance")),
                "raw_json": record,
                "payload_hash": hash_payload(record),
            }
        )
        purchases = record.get("purchases") or {}
        line_no = 0
        for item in purchases.get("items") or []:
            line_no += 1
            lines.append(
                {
                    "company_id": company_id,
                    "bill_alegra_id": alegra_id,
                    "line_number": line_no,
                    "line_kind": "item",
                    "item_alegra_id": _str_id(item.get("id")),
                    "item_name": item.get("name"),
                    "quantity": _dec(item.get("quantity")),
                    "unit_price": _dec(item.get("price")),
                    "line_total": _dec(item.get("total")),
                    "raw_json": item,
                }
            )
        for category in purchases.get("categories") or []:
            line_no += 1
            lines.append(
                {
                    "company_id": company_id,
                    "bill_alegra_id": alegra_id,
                    "line_number": line_no,
                    "line_kind": "category",
                    "category_alegra_id": _str_id(category.get("id")),
                    "category_name": category.get("name"),
                    "quantity": _dec(category.get("quantity")),
                    "unit_price": _dec(category.get("price")),
                    "line_total": _dec(category.get("total")),
                    "raw_json": category,
                }
            )
    return headers, lines


def parse_credit_notes(records: list[dict[str, Any]], company_id: int) -> tuple[list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "note_date": _parse_date(record.get("date")),
                "status": record.get("status"),
                "client_alegra_id": _id(record.get("client")),
                "note_total": _dec(record.get("total")),
                "raw_json": record,
            }
        )
        for idx, item in enumerate(record.get("items") or [], start=1):
            lines.append(
                {
                    "company_id": company_id,
                    "credit_note_alegra_id": alegra_id,
                    "line_number": idx,
                    "item_alegra_id": _str_id(item.get("id")),
                    "item_name": item.get("name"),
                    "quantity": _dec(item.get("quantity")),
                    "unit_price": _dec(item.get("price")),
                    "line_total": _dec(item.get("total")),
                }
            )
    return headers, lines


def parse_simple_dimension(
    records: list[dict[str, Any]],
    company_id: int,
    *,
    id_field: str = "id",
    name_field: str = "name",
) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get(id_field))
        if not alegra_id:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "name": record.get(name_field) or alegra_id,
                "status": record.get("status"),
                "raw_json": record,
            }
        )
    return rows
