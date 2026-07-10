"""Base declarativa y utilidades de modelos."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import MetaData, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# DEFAULT {} es inválido en PostgreSQL para JSONB; hay que castear.
JSONB_EMPTY = text("'{}'::jsonb")


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def configure_schema(schema_name: str) -> None:
    """Asigna el esquema PostgreSQL a metadata y a todas las tablas."""
    Base.metadata.schema = schema_name
    for table in Base.metadata.tables.values():
        table.schema = schema_name


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


def alegra_pk_columns() -> dict[str, Any]:
    return {}
