"""Pruebas de checkpoints reanudables."""

from datetime import date

import pytest

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.config import get_settings
from alegra_etl.db.models import SyncCheckpoint
from alegra_etl.db.models.base import configure_schema
from alegra_etl.db.session import create_db_engine, ensure_schema, session_scope
from alegra_etl.pipeline.checkpoint import CheckpointManager


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
def test_checkpoint_resume_after_partial_batch(settings):
    get_settings.cache_clear()
    cfg = get_settings()
    configure_schema(cfg.db_schema)
    ensure_schema(cfg)
    engine = create_db_engine(cfg)
    from alegra_etl.db.models import Base

    Base.metadata.create_all(engine)

    resource = resource_by_name("invoices")
    assert resource is not None

    with session_scope(cfg) as session:
        manager = CheckpointManager(cfg, session)
        checkpoint = manager.get_or_create(resource)
        manager.mark_running(checkpoint, __import__("uuid").uuid4())
        manager.update_after_batch(
            checkpoint,
            resource,
            {
                "extracted": 30,
                "source_upserted": 30,
                "typed_upserted": 30,
                "completed": 0,
                "next_offset": 30,
                "cursor_date": "2022-01-01",
            },
            __import__("uuid").uuid4(),
        )
        session.commit()

        saved = (
            session.query(SyncCheckpoint)
            .filter_by(company_id=cfg.company_id, resource_name="invoices")
            .one()
        )
        assert saved.status == "pending"
        assert saved.cursor_offset == 30
        assert saved.cursor_date == date(2022, 1, 1)

    get_settings.cache_clear()
