"""Pruebas de source_documents e idempotencia."""

import pytest

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.config import get_settings
from alegra_etl.db.models.base import configure_schema
from alegra_etl.db.models.canonical import SourceDocument
from alegra_etl.db.session import create_db_engine, ensure_schema, session_scope
from alegra_etl.pipeline.source_loader import upsert_source_documents


def _postgres_available(url: str) -> bool:
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(url, connect_args={"connect_timeout": 2})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(
    not _postgres_available("postgresql+psycopg://postgres:postgres@localhost:5432/alegra_etl_test"),
    reason="PostgreSQL local no disponible",
)
def test_source_documents_idempotent(settings):
    get_settings.cache_clear()
    cfg = get_settings()
    configure_schema(cfg.db_schema)
    ensure_schema(cfg)
    engine = create_db_engine(cfg)
    from alegra_etl.db.models import Base

    Base.metadata.create_all(engine)

    resource = resource_by_name("items")
    assert resource is not None
    run_id = __import__("uuid").uuid4()
    records = [{"id": "1", "name": "Item A", "date": "2024-01-01"}]

    with session_scope(cfg) as session:
        first = upsert_source_documents(
            session,
            company_id=cfg.company_id,
            resource=resource,
            records=records,
            run_id=run_id,
        )
        second = upsert_source_documents(
            session,
            company_id=cfg.company_id,
            resource=resource,
            records=[{"id": "1", "name": "Item A updated", "date": "2024-01-02"}],
            run_id=run_id,
        )
        session.commit()
        count = session.query(SourceDocument).count()
        doc = session.query(SourceDocument).one()

    assert first == 1
    assert second == 1
    assert count == 1
    assert doc.payload["name"] == "Item A updated"

    get_settings.cache_clear()
