"""Pruebas unitarias de avance de checkpoint (sin PostgreSQL)."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from alegra_etl.alegra.resources import SyncStrategy, resource_by_name
from alegra_etl.config import get_settings
from alegra_etl.pipeline.checkpoint import CheckpointManager


def _settings(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/test")
    monkeypatch.setenv("ALEGRA_EMAIL", "test@example.com")
    monkeypatch.setenv("ALEGRA_TOKEN", "token")
    monkeypatch.setenv("WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("BACKFILL_DAYS_PER_STEP", "7")
    return get_settings()


def test_cursor_advances_to_next_day_after_completed_batch(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="running",
        cursor_date=date(2022, 11, 18),
        cursor_offset=0,
        backfill_end_date=date(2026, 7, 11),
        last_successful_run_id=None,
        last_synced_at=None,
        backfill_completed_at=None,
    )

    manager.update_after_batch(
        checkpoint,
        resource,
        {
            "completed": 1,
            "next_offset": 0,
            "cursor_date": "2022-11-19",  # próximo día
        },
        __import__("uuid").uuid4(),
    )

    assert checkpoint.cursor_date == date(2022, 11, 19)
    assert checkpoint.cursor_offset == 0
    assert checkpoint.status == "pending"
    get_settings.cache_clear()


def test_backfill_marks_completed_when_cursor_passes_end(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="running",
        cursor_date=date(2026, 7, 11),
        cursor_offset=0,
        backfill_end_date=date(2026, 7, 11),
        last_successful_run_id=None,
        last_synced_at=None,
        backfill_completed_at=None,
    )

    manager.update_after_batch(
        checkpoint,
        resource,
        {
            "completed": 1,
            "next_offset": 0,
            "cursor_date": "2026-07-12",
        },
        __import__("uuid").uuid4(),
    )

    assert checkpoint.status == "completed"
    assert checkpoint.backfill_completed_at is not None
    get_settings.cache_clear()


def test_backfill_window_uses_cursor_as_start(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None
    assert resource.strategy == SyncStrategy.DATE_WINDOW

    checkpoint = SimpleNamespace(
        status="pending",
        cursor_date=date(2022, 11, 19),
        cursor_offset=0,
        backfill_start_date=date(2022, 11, 18),
        backfill_end_date=date(2026, 7, 11),
    )

    start, end = manager.backfill_window(resource, checkpoint)
    assert start == date(2022, 11, 19)
    assert end == date(2022, 11, 25)  # 7 días: 19..25
    get_settings.cache_clear()


def test_partial_day_keeps_same_cursor_and_offset(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="running",
        cursor_date=date(2022, 11, 18),
        cursor_offset=0,
        backfill_end_date=date(2026, 7, 11),
        last_successful_run_id=None,
        last_synced_at=None,
        backfill_completed_at=None,
    )

    manager.update_after_batch(
        checkpoint,
        resource,
        {
            "completed": 0,
            "next_offset": 60,
            "cursor_date": "2022-11-18",
        },
        __import__("uuid").uuid4(),
    )

    assert checkpoint.cursor_date == date(2022, 11, 18)
    assert checkpoint.cursor_offset == 60
    assert checkpoint.status == "pending"
    get_settings.cache_clear()
