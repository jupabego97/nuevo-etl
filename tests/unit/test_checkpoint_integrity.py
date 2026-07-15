"""Pruebas de integridad de checkpoints legacy."""

from datetime import date, datetime, UTC

import pytest

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.pipeline.checkpoint_integrity import (
    checkpoint_issues,
    is_truly_complete,
    repair_checkpoint,
)


def _legacy_corrupt_checkpoint():
    from alegra_etl.db.models import SyncCheckpoint

    return SyncCheckpoint(
        company_id=1,
        resource_name="invoices",
        status="completed",
        cursor_date=date(2022, 11, 18),
        backfill_start_date=None,
        backfill_end_date=None,
        backfill_completed_at=datetime.now(UTC),
        cursor_offset=0,
    )


def test_legacy_completed_with_null_range_has_issues():
    resource = resource_by_name("invoices")
    assert resource is not None
    cp = _legacy_corrupt_checkpoint()
    issues = checkpoint_issues(cp, resource)
    assert "missing_backfill_start_date" in issues
    assert "missing_backfill_end_date" in issues
    assert "cursor_not_past_end" in issues
    assert not is_truly_complete(cp, resource)


def test_repair_checkpoint_reopens_legacy(settings):
    resource = resource_by_name("invoices")
    assert resource is not None
    cp = _legacy_corrupt_checkpoint()
    changed = repair_checkpoint(cp, resource, settings, reason="test")
    assert changed is True
    assert cp.status == "pending"
    assert cp.backfill_completed_at is None
    assert cp.backfill_start_date is not None
    assert cp.backfill_end_date is not None
    assert cp.cursor_offset == 0


def test_valid_completed_checkpoint_passes():
    from alegra_etl.db.models import SyncCheckpoint

    resource = resource_by_name("bills")
    assert resource is not None
    cp = SyncCheckpoint(
        company_id=1,
        resource_name="bills",
        status="completed",
        backfill_start_date=date(2022, 1, 1),
        backfill_end_date=date(2024, 1, 1),
        cursor_date=date(2024, 1, 2),
        backfill_completed_at=datetime.now(UTC),
        verified_at=datetime.now(UTC),
        cursor_offset=0,
    )
    assert checkpoint_issues(cp, resource) == []
    assert is_truly_complete(cp, resource)
