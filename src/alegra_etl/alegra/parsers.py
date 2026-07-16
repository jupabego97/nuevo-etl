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


def resolve_tax_id(record: dict[str, Any]) -> str | None:
    """Obtiene el ID del impuesto o una identidad determinística documentada."""
    for key in ("id", "idTax", "taxId", "taxID"):
        value = record.get(key)
        if isinstance(value, dict):
            value = value.get("id")
        if value is not None and str(value).strip():
            return _str_id(value)

    favorable = record.get("categoryFavorable")
    payable = record.get("categoryToBePaid")
    stable_identity = {
        "name": record.get("name"),
        "percentage": record.get("percentage"),
        "type": record.get("type"),
        "rate": record.get("rate"),
        "code": record.get("code"),
        "category_favorable": favorable.get("id") if isinstance(favorable, dict) else None,
        "category_payable": payable.get("id") if isinstance(payable, dict) else None,
    }
    if not any(value is not None for value in stable_identity.values()):
        return None
    return f"synthetic-tax:{hash_payload(stable_identity)[:40]}"


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


def parse_items(
    records: list[dict[str, Any]], company_id: int
) -> tuple[list[dict], list[dict], list[dict]]:
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
                "unit_cost": _dec(inventory.get("unitCost"))
                if isinstance(inventory, dict)
                else None,
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


def _boolish(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "si", "sí"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _invoice_numbering(record: dict[str, Any]) -> dict[str, Any]:
    """Extrae numeración y señales de factura electrónica desde payload Alegra."""
    number_template = record.get("numberTemplate") or {}
    if not isinstance(number_template, dict):
        number_template = {}

    prefix_raw = number_template.get("prefix")
    prefix = str(prefix_raw).strip() if prefix_raw not in (None, "") else None
    number_raw = number_template.get("number")
    number_value = str(number_raw) if number_raw is not None else None
    if prefix and number_value is not None:
        invoice_number = f"{prefix}{number_value}"
    elif number_value is not None:
        invoice_number = number_value
    else:
        invoice_number = prefix

    is_electronic = _boolish(number_template.get("isElectronic"))
    if is_electronic is None:
        is_electronic = _boolish(record.get("isElectronic"))

    cufe = None
    for key in ("cufe", "CUFE"):
        value = record.get(key)
        if value:
            cufe = str(value)
            break
    if cufe is None:
        stamp = record.get("stamp")
        if isinstance(stamp, dict):
            for key in ("cufe", "uuid"):
                if stamp.get(key):
                    cufe = str(stamp[key])
                    break

    # Heurística segura: presencia de CUFE implica electrónica.
    if is_electronic is None and cufe:
        is_electronic = True

    return {
        "invoice_number": invoice_number,
        "number_template_id": _str_id(number_template.get("id")),
        "number_template_name": number_template.get("name") or number_template.get("documentName"),
        "number_prefix": prefix,
        "number_value": number_value,
        "is_electronic": is_electronic,
        "cufe": cufe,
    }


def parse_sales_invoices(
    records: list[dict[str, Any]], company_id: int
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []
    payments: list[dict] = []
    applications: list[dict] = []

    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        numbering = _invoice_numbering(record)
        invoice_date = _parse_date(record.get("date"))
        if invoice_date is None:
            continue

        currency = record.get("currency") or {}
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                **numbering,
                "invoice_date": invoice_date,
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
                "exchange_rate": _dec(currency.get("exchangeRate"))
                if isinstance(currency, dict)
                else None,
                "subtotal": _dec(record.get("subtotal")),
                "discount": _dec(record.get("discount")),
                "tax_total": None,
                "retention_total": sum(
                    (_dec(r.get("amount")) or Decimal(0)) for r in record.get("retentions") or []
                ),
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
                    "tax_total": sum(
                        (_dec(t.get("amount")) or Decimal(0)) for t in item.get("tax") or []
                    ),
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


def parse_purchase_bills(
    records: list[dict[str, Any]], company_id: int
) -> tuple[list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []

    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        number_template = record.get("numberTemplate") or {}
        bill_number = None
        if isinstance(number_template, dict):
            bill_number = (
                str(number_template.get("number"))
                if number_template.get("number") is not None
                else None
            )
        currency = record.get("currency") or {}
        bill_date = _parse_date(record.get("date"))
        if bill_date is None:
            # Columna NOT NULL; sin fecha no se puede tipar (queda en source_documents).
            continue
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "bill_number": bill_number,
                "bill_date": bill_date,
                "due_date": _parse_date(record.get("dueDate")),
                "status": record.get("status"),
                "bill_type": record.get("type") or "bill",
                "provider_alegra_id": _id(record.get("provider")),
                "provider_name": _name(record.get("provider")),
                "warehouse_alegra_id": _id(record.get("warehouse")),
                "currency_code": currency.get("code") if isinstance(currency, dict) else None,
                "exchange_rate": _dec(currency.get("exchangeRate"))
                if isinstance(currency, dict)
                else None,
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
            # Todas las líneas deben compartir las mismas claves (UPSERT multiparam).
            lines.append(
                {
                    "company_id": company_id,
                    "bill_alegra_id": alegra_id,
                    "line_number": line_no,
                    "line_kind": "item",
                    "item_alegra_id": _str_id(item.get("id")),
                    "item_name": item.get("name"),
                    "category_alegra_id": None,
                    "category_name": None,
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
                    "item_alegra_id": None,
                    "item_name": None,
                    "category_alegra_id": _str_id(category.get("id")),
                    "category_name": category.get("name"),
                    "quantity": _dec(category.get("quantity")),
                    "unit_price": _dec(category.get("price")),
                    "line_total": _dec(category.get("total")),
                    "raw_json": category,
                }
            )
    return headers, lines


def parse_credit_notes(
    records: list[dict[str, Any]], company_id: int
) -> tuple[list[dict], list[dict]]:
    headers: list[dict] = []
    lines: list[dict] = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        note_date = _parse_date(record.get("date"))
        if note_date is None:
            continue
        headers.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "note_date": note_date,
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


def parse_company(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id")) or "singleton"
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "name": record.get("name"),
                "identification": record.get("identification"),
                "currency_code": (record.get("currency") or {}).get("code")
                if isinstance(record.get("currency"), dict)
                else record.get("currency"),
                "raw_json": record,
            }
        )
    return rows


def parse_currencies(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        code = record.get("code") or record.get("name")
        if not code:
            continue
        rows.append(
            {
                "company_id": company_id,
                "code": str(code),
                "name": record.get("name"),
                "symbol": record.get("symbol"),
                "exchange_rate": _dec(record.get("exchangeRate") or record.get("rate")),
                "is_default": bool(record.get("main") or record.get("isDefault")),
            }
        )
    return rows


def parse_cost_centers(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "code": record.get("code"),
                "name": record.get("name") or alegra_id,
                "status": record.get("status"),
            }
        )
    return rows


def parse_bank_accounts(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "name": record.get("name") or alegra_id,
                "account_type": record.get("type") or record.get("accountType"),
                "balance": _dec(record.get("balance")),
                "currency_code": record.get("currency"),
                "raw_json": record,
            }
        )
    return rows


def parse_purchase_orders(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        order_date = _parse_date(record.get("date"))
        if order_date is None:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "order_date": order_date,
                "delivery_date": _parse_date(record.get("deliveryDate")),
                "status": record.get("status"),
                "provider_alegra_id": _id(record.get("provider") or record.get("client")),
                "order_total": _dec(record.get("total")),
                "raw_json": record,
            }
        )
    return rows


def parse_inventory_adjustments(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        adj_date = _parse_date(record.get("date"))
        if adj_date is None:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "adjustment_date": adj_date,
                "warehouse_alegra_id": _id(record.get("warehouse")),
                "raw_json": record,
            }
        )
    return rows


def parse_warehouse_transfers(records: list[dict[str, Any]], company_id: int) -> list[dict]:
    rows = []
    for record in records:
        alegra_id = _str_id(record.get("id"))
        if not alegra_id:
            continue
        rows.append(
            {
                "company_id": company_id,
                "alegra_id": alegra_id,
                "transfer_date": _parse_date(record.get("date")),
                "origin_warehouse_id": _id(
                    record.get("originWarehouse") or record.get("warehouseOrigin")
                ),
                "destination_warehouse_id": _id(
                    record.get("destinationWarehouse") or record.get("warehouseDestination")
                ),
                "raw_json": record,
            }
        )
    return rows
