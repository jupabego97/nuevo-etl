import os

import pytest
from sqlalchemy import create_engine, text

from alegra_etl.config import get_settings
from alegra_etl.db.models import DimItem
from alegra_etl.db.models.base import configure_schema
from alegra_etl.db.session import create_db_engine, session_scope
from alegra_etl.pipeline.loader import upsert_rows


def _db_available() -> bool:
    try:
        engine = create_engine(
            os.environ["DATABASE_URL"],
            pool_pre_ping=True,
            connect_args={"connect_timeout": 2},
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason="PostgreSQL no disponible")


@pytest.fixture(scope="module")
def migrated_db():
    get_settings.cache_clear()
    settings = get_settings()
    configure_schema(settings.db_schema)
    engine = create_db_engine(settings)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{settings.db_schema}"'))
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")
    yield settings


def test_upsert_is_idempotent(migrated_db):
    row = {
        "company_id": 1,
        "alegra_id": "test-item-1",
        "name": "Item prueba",
        "is_inventoriable": True,
    }
    with session_scope(migrated_db) as session:
        first = upsert_rows(session, DimItem.__table__, [row], ["company_id", "alegra_id"])
        row["name"] = "Item actualizado"
        second = upsert_rows(
            session,
            DimItem.__table__,
            [row],
            ["company_id", "alegra_id"],
            update_columns=["name"],
        )
        count = session.execute(
            text("SELECT COUNT(*) FROM dim_item WHERE company_id = 1 AND alegra_id = 'test-item-1'")
        ).scalar()
        name = session.execute(
            text("SELECT name FROM dim_item WHERE company_id = 1 AND alegra_id = 'test-item-1'")
        ).scalar()
    assert first >= 1
    assert second >= 1
    assert count == 1
    assert name == "Item actualizado"
