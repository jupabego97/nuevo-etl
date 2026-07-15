"""Hechos normalizados de ingresos, gastos, inventario y finanzas."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from alegra_etl.db.models.base import JSONB_EMPTY, Base, TimestampMixin


class FactSalesInvoice(Base, TimestampMixin):
    __tablename__ = "fact_sales_invoice"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_sales_invoice"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    invoice_number: Mapped[str | None] = mapped_column(String(100))
    number_template_id: Mapped[str | None] = mapped_column(String(50), index=True)
    number_template_name: Mapped[str | None] = mapped_column(String(200))
    number_prefix: Mapped[str | None] = mapped_column(String(50), index=True)
    number_value: Mapped[str | None] = mapped_column(String(50))
    is_electronic: Mapped[bool | None] = mapped_column(Boolean, index=True)
    cufe: Mapped[str | None] = mapped_column(String(200))
    invoice_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    due_date: Mapped[date | None] = mapped_column(Date)
    datetime_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str | None] = mapped_column(String(20), index=True)
    client_alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    client_name: Mapped[str | None] = mapped_column(String(500))
    seller_alegra_id: Mapped[str | None] = mapped_column(String(50))
    seller_name: Mapped[str | None] = mapped_column(String(300))
    warehouse_alegra_id: Mapped[str | None] = mapped_column(String(50))
    cost_center_alegra_id: Mapped[str | None] = mapped_column(String(50))
    currency_code: Mapped[str | None] = mapped_column(String(10))
    exchange_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    subtotal: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    discount: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    tax_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    retention_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    invoice_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    total_paid: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    balance: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    payment_form: Mapped[str | None] = mapped_column(String(50))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)
    payload_hash: Mapped[str | None] = mapped_column(String(64))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FactSalesInvoiceLine(Base, TimestampMixin):
    __tablename__ = "fact_sales_invoice_line"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "invoice_alegra_id",
            "line_number",
            name="uq_fact_sales_invoice_line",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    invoice_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    item_alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    item_name: Mapped[str | None] = mapped_column(String(500))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    discount: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    tax_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    line_subtotal: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    line_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactIncomePayment(Base, TimestampMixin):
    __tablename__ = "fact_income_payment"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_income_payment"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    payment_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    payment_method: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str | None] = mapped_column(String(20))
    client_alegra_id: Mapped[str | None] = mapped_column(String(50))
    bank_account_alegra_id: Mapped[str | None] = mapped_column(String(50))
    currency_code: Mapped[str | None] = mapped_column(String(10))
    exchange_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactIncomePaymentApplication(Base, TimestampMixin):
    __tablename__ = "fact_income_payment_application"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "payment_alegra_id",
            "invoice_alegra_id",
            name="uq_fact_income_payment_app",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payment_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    invoice_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    amount_applied: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))


class FactCreditNote(Base, TimestampMixin):
    __tablename__ = "fact_credit_note"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_credit_note"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    note_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    status: Mapped[str | None] = mapped_column(String(20))
    client_alegra_id: Mapped[str | None] = mapped_column(String(50))
    note_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactCreditNoteLine(Base, TimestampMixin):
    __tablename__ = "fact_credit_note_line"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "credit_note_alegra_id",
            "line_number",
            name="uq_fact_credit_note_line",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    credit_note_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    item_alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    item_name: Mapped[str | None] = mapped_column(String(500))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    line_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))


class FactPurchaseBill(Base, TimestampMixin):
    __tablename__ = "fact_purchase_bill"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_purchase_bill"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    bill_number: Mapped[str | None] = mapped_column(String(100))
    bill_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    due_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str | None] = mapped_column(String(20))
    bill_type: Mapped[str | None] = mapped_column(String(30))
    provider_alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    provider_name: Mapped[str | None] = mapped_column(String(500))
    warehouse_alegra_id: Mapped[str | None] = mapped_column(String(50))
    currency_code: Mapped[str | None] = mapped_column(String(10))
    exchange_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    bill_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    total_paid: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    balance: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)
    payload_hash: Mapped[str | None] = mapped_column(String(64))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FactPurchaseBillLine(Base, TimestampMixin):
    __tablename__ = "fact_purchase_bill_line"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "bill_alegra_id",
            "line_number",
            name="uq_fact_purchase_bill_line",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    bill_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    line_kind: Mapped[str] = mapped_column(String(20), default="item")
    item_alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    item_name: Mapped[str | None] = mapped_column(String(500))
    category_alegra_id: Mapped[str | None] = mapped_column(String(50))
    category_name: Mapped[str | None] = mapped_column(String(500))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    line_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactPurchaseOrder(Base, TimestampMixin):
    __tablename__ = "fact_purchase_order"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_purchase_order"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    delivery_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str | None] = mapped_column(String(20))
    provider_alegra_id: Mapped[str | None] = mapped_column(String(50))
    order_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactInventoryAdjustment(Base, TimestampMixin):
    __tablename__ = "fact_inventory_adjustment"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_inventory_adjustment"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    adjustment_date: Mapped[date] = mapped_column(Date, nullable=False)
    warehouse_alegra_id: Mapped[str | None] = mapped_column(String(50))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactWarehouseTransfer(Base, TimestampMixin):
    __tablename__ = "fact_warehouse_transfer"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_warehouse_transfer"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    transfer_date: Mapped[date | None] = mapped_column(Date)
    origin_warehouse_id: Mapped[str | None] = mapped_column(String(50))
    destination_warehouse_id: Mapped[str | None] = mapped_column(String(50))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class FactBankAccount(Base, TimestampMixin):
    __tablename__ = "fact_bank_account"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_fact_bank_account"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    account_type: Mapped[str | None] = mapped_column(String(50))
    balance: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    currency_code: Mapped[str | None] = mapped_column(String(10))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class ReplenishmentPolicy(Base, TimestampMixin):
    __tablename__ = "replenishment_policy"
    __table_args__ = (
        UniqueConstraint("company_id", "item_alegra_id", "warehouse_alegra_id", name="uq_replenishment_policy"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    item_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    warehouse_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, default="default")
    lead_time_days: Mapped[int] = mapped_column(Integer, default=7)
    service_level: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=Decimal("0.9500"))
    safety_stock_days: Mapped[int] = mapped_column(Integer, default=3)
    review_period_days: Mapped[int] = mapped_column(Integer, default=30)
    moq: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    order_multiple: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
