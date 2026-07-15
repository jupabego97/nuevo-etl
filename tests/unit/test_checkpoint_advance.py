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
            "cursor_date": "2022-11-19",
        },
        __import__("uuid").uuid4(),
    )

    assert checkpoint.cursor_date == date(2022, 11, 19)
    assert checkpoint.cursor_offset == 0
    assert checkpoint.status == "pending"
    get_settings.cache_clear()


def test_backfill_marks_completed_when_cursor_passes_end(monkeypatch):
    settings = _settings(monkeypatch)
    monkeypatch.setattr(
        "alegra_etl.pipeline.checkpoint.can_mark_backfill_completed",
        lambda *args, **kwargs: True,
    )
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
    assert end == date(2022, 11, 25)
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


def test_full_resource_completed_flag_marks_checkpoint(monkeypatch):
    settings = _settings(monkeypatch)
    monkeypatch.setattr(
        "alegra_etl.pipeline.checkpoint.can_mark_backfill_completed",
        lambda *args, **kwargs: True,
    )
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("items")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="running",
        cursor_date=None,
        cursor_offset=0,
        backfill_end_date=date(2026, 7, 11),
        last_successful_run_id=None,
        last_synced_at=None,
        backfill_completed_at=None,
    )

    manager.update_after_batch(
        checkpoint,
        resource,
        {"completed": 1, "next_offset": 0, "extracted": 10},
        __import__("uuid").uuid4(),
    )
    assert checkpoint.status == "completed"
    assert checkpoint.backfill_completed_at is not None
    get_settings.cache_clear()


def test_reopen_false_completed_date_window(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="completed",
        cursor_date=None,
        cursor_offset=0,
        backfill_start_date=None,
        backfill_end_date=None,
        backfill_completed_at=None,
        verified_at=None,
        backfill_generation=1,
        metadata_json={},
    )
    manager._maybe_reopen_false_completed(checkpoint, resource, date(2026, 7, 12))
    assert checkpoint.status == "pending"
    assert checkpoint.cursor_date == date.fromisoformat(settings.backfill_start_date)
    get_settings.cache_clear()


def test_does_not_reopen_true_historical_completed(monkeypatch):
    settings = _settings(monkeypatch)
    manager = CheckpointManager(settings, MagicMock())
    resource = resource_by_name("invoices")
    assert resource is not None

    checkpoint = SimpleNamespace(
        status="completed",
        cursor_date=date(2026, 7, 13),
        cursor_offset=0,
        backfill_start_date=date(2022, 1, 1),
        backfill_end_date=date(2026, 7, 12),
        backfill_completed_at=__import__("datetime").datetime.now(
            __import__("datetime").UTC
        ),
        verified_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
        metadata_json={},
    )
    manager._maybe_reopen_false_completed(checkpoint, resource, date(2026, 7, 12))
    assert checkpoint.status == "completed"
    get_settings.cache_clear()


def test_company_excluded_from_backfill(monkeypatch):
    _settings(monkeypatch)
    from alegra_etl.alegra.resources import get_backfill_resources

    names = [r.name for r in get_backfill_resources(get_settings())]
    assert "company" not in names
    assert "invoices" in names
    get_settings.cache_clear()
