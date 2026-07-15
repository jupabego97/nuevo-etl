"""Pruebas del gate de completitud de backfill."""

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.pipeline.checkpoint_integrity import checkpoint_issues
from alegra_etl.pipeline.completion_gate import backfill_completion_blockers


def test_blockers_detect_source_typed_mismatch(monkeypatch):
    resource = resource_by_name("invoices")
    assert resource is not None
    cp = MagicMock()
    cp.status = "completed"
    cp.backfill_start_date = date(2022, 1, 1)
    cp.backfill_end_date = date(2022, 1, 31)
    cp.cursor_date = date(2022, 2, 1)
    cp.backfill_completed_at = datetime.now(UTC)
    cp.verified_at = None

    session = MagicMock()
    session.query.return_value.filter.return_value.count.return_value = 0

    monkeypatch.setattr(
        "alegra_etl.pipeline.completion_gate.count_source_ids",
        lambda *args, **kwargs: 100,
    )
    monkeypatch.setattr(
        "alegra_etl.pipeline.completion_gate.count_typed_ids",
        lambda *args, **kwargs: 50,
    )

    blockers = backfill_completion_blockers(session, MagicMock(), cp, resource)
    assert any("source_typed_mismatch" in b for b in blockers)


def test_legacy_checkpoint_issues_in_blockers():
    resource = resource_by_name("invoices")
    assert resource is not None
    from alegra_etl.db.models import SyncCheckpoint

    cp = SyncCheckpoint(
        company_id=1,
        resource_name="invoices",
        status="completed",
        cursor_date=date(2022, 11, 18),
        backfill_start_date=None,
        backfill_end_date=None,
        backfill_completed_at=datetime.now(UTC),
    )
    issues = checkpoint_issues(cp, resource)
    assert len(issues) >= 3
