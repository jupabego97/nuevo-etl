"""Dimensiones maestras."""

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
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from alegra_etl.db.models.base import Base, TimestampMixin


class DimCompany(Base, TimestampMixin):
    __tablename__ = "dim_company"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_company"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str | None] = mapped_column(String(300))
    identification: Mapped[str | None] = mapped_column(String(100))
    currency_code: Mapped[str | None] = mapped_column(String(10))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DimItem(Base, TimestampMixin):
    __tablename__ = "dim_item"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_item"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    reference: Mapped[str | None] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text)
    item_type: Mapped[str | None] = mapped_column(String(30))
    status: Mapped[str | None] = mapped_column(String(20))
    is_inventoriable: Mapped[bool] = mapped_column(Boolean, default=False)
    unit: Mapped[str | None] = mapped_column(String(50))
    unit_cost: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    category_id: Mapped[str | None] = mapped_column(String(50))
    category_name: Mapped[str | None] = mapped_column(String(200))
    barcode: Mapped[str | None] = mapped_column(String(100))
    family: Mapped[str | None] = mapped_column(String(200))
    brand: Mapped[str | None] = mapped_column(String(200))
    model: Mapped[str | None] = mapped_column(String(200))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")
    payload_hash: Mapped[str | None] = mapped_column(String(64))
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DimItemPrice(Base, TimestampMixin):
    __tablename__ = "dim_item_price"
    __table_args__ = (
        UniqueConstraint("company_id", "item_alegra_id", "price_list_id", name="uq_dim_item_price"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    item_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    price_list_id: Mapped[str] = mapped_column(String(50), nullable=False)
    price_list_name: Mapped[str | None] = mapped_column(String(200))
    price: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))


class DimItemInventory(Base, TimestampMixin):
    __tablename__ = "dim_item_inventory"
    __table_args__ = (
        UniqueConstraint("company_id", "item_alegra_id", "warehouse_alegra_id", name="uq_dim_item_inv"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    item_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    warehouse_alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    warehouse_name: Mapped[str | None] = mapped_column(String(200))
    available_quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    min_quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    max_quantity: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    snapshot_date: Mapped[date | None] = mapped_column(Date)


class DimContact(Base, TimestampMixin):
    __tablename__ = "dim_contact"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_contact"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    identification: Mapped[str | None] = mapped_column(String(100))
    email: Mapped[str | None] = mapped_column(String(300))
    phone_primary: Mapped[str | None] = mapped_column(String(100))
    contact_type: Mapped[str | None] = mapped_column(String(30))
    status: Mapped[str | None] = mapped_column(String(20))
    city: Mapped[str | None] = mapped_column(String(200))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")
    payload_hash: Mapped[str | None] = mapped_column(String(64))
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DimSeller(Base, TimestampMixin):
    __tablename__ = "dim_seller"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_seller"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    identification: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str | None] = mapped_column(String(20))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")


class DimWarehouse(Base, TimestampMixin):
    __tablename__ = "dim_warehouse"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_warehouse"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str | None] = mapped_column(String(20))
    address: Mapped[str | None] = mapped_column(String(500))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")


class DimCostCenter(Base, TimestampMixin):
    __tablename__ = "dim_cost_center"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_cost_center"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    code: Mapped[str | None] = mapped_column(String(50))
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    status: Mapped[str | None] = mapped_column(String(20))


class DimTax(Base, TimestampMixin):
    __tablename__ = "dim_tax"
    __table_args__ = (UniqueConstraint("company_id", "alegra_id", name="uq_dim_tax"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    alegra_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    percentage: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    tax_type: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str | None] = mapped_column(String(20))


class DimCurrency(Base, TimestampMixin):
    __tablename__ = "dim_currency"
    __table_args__ = (UniqueConstraint("company_id", "code", name="uq_dim_currency"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    code: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str | None] = mapped_column(String(100))
    symbol: Mapped[str | None] = mapped_column(String(10))
    exchange_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
