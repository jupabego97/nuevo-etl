"""Unidades de trabajo reanudables para backfill histórico."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from alegra_etl.db.models.base import JSONB_EMPTY, Base, TimestampMixin


class BackfillWorkItem(Base, TimestampMixin):
    """Unidad atómica de backfill: un día (DATE_WINDOW) o rango offset (FULL)."""

    __tablename__ = "backfill_work_items"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "resource_name",
            "work_key",
            name="uq_backfill_work_item",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    resource_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    work_key: Mapped[str] = mapped_column(String(50), nullable=False)
    work_date: Mapped[date | None] = mapped_column(Date, index=True)
    start_offset: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    records_extracted: Mapped[int] = mapped_column(Integer, default=0)
    records_source: Mapped[int] = mapped_column(Integer, default=0)
    records_typed: Mapped[int] = mapped_column(Integer, default=0)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(100))
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, default=dict, server_default=JSONB_EMPTY
    )


class EtlParseSkip(Base, TimestampMixin):
    """Registros que no pudieron tiparse sin tumbar el lote completo."""

    __tablename__ = "etl_parse_skips"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    resource_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    alegra_id: Mapped[str | None] = mapped_column(String(50), index=True)
    reason: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
