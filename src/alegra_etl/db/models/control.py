"""Tablas de control del ETL."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from alegra_etl.db.models.base import JSONB_EMPTY, Base, TimestampMixin


class EtlRun(Base, TimestampMixin):
    __tablename__ = "etl_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_type: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)

    stages: Mapped[list[EtlStageRun]] = relationship(back_populates="run")


class EtlStageRun(Base, TimestampMixin):
    __tablename__ = "etl_stage_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("etl_runs.id", ondelete="CASCADE"), nullable=False
    )
    stage_name: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_name: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="running")
    records_extracted: Mapped[int] = mapped_column(Integer, default=0)
    records_loaded: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)

    run: Mapped[EtlRun] = relationship(back_populates="stages")


class SyncCheckpoint(Base, TimestampMixin):
    __tablename__ = "sync_checkpoints"
    __table_args__ = (UniqueConstraint("company_id", "resource_name", name="uq_checkpoint_resource"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    resource_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_successful_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    watermark_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)


class QualityCheckResult(Base, TimestampMixin):
    __tablename__ = "quality_check_results"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("etl_runs.id"))
    check_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=JSONB_EMPTY)
