"""Sesión y engine de base de datos."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from alegra_etl.config import Settings, get_settings
from alegra_etl.db.models.base import configure_schema


def create_db_engine(settings: Settings | None = None) -> Engine:
    cfg = settings or get_settings()
    configure_schema(cfg.db_schema)
    engine = create_engine(
        cfg.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute(f'SET search_path TO "{cfg.db_schema}", public')
        cursor.close()

    return engine


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@contextmanager
def session_scope(settings: Settings | None = None) -> Iterator[Session]:
    engine = create_db_engine(settings)
    factory = create_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ensure_schema(settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    engine = create_db_engine(cfg)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{cfg.db_schema}"'))
