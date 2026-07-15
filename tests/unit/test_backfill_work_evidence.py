"""Pruebas de evidencia por unidad de backfill."""

from datetime import date

from alegra_etl.db.models.backfill import BackfillWorkItem
from alegra_etl.pipeline.backfill_work import mark_work_verified


def test_partial_unit_keeps_offset_pending():
    item = BackfillWorkItem(
        company_id=1,
        resource_name="invoices",
        work_key=date(2022, 1, 1).isoformat(),
        work_date=date(2022, 1, 1),
        start_offset=0,
        status="running",
        attempts=1,
    )

    verified = mark_work_verified(
        item,
        {
            "completed": 0,
            "next_offset": 30,
            "extracted": 30,
            "source_upserted": 30,
            "typed_upserted": 30,
        },
        __import__("uuid").uuid4(),
    )

    assert verified is False
    assert item.status == "pending"
    assert item.start_offset == 30
    assert item.verified_at is None


def test_quarantined_unit_cannot_be_verified():
    item = BackfillWorkItem(
        company_id=1,
        resource_name="invoices",
        work_key=date(2022, 1, 1).isoformat(),
        work_date=date(2022, 1, 1),
        status="running",
        attempts=1,
    )

    verified = mark_work_verified(
        item,
        {
            "completed": 1,
            "next_offset": 0,
            "extracted": 2,
            "source_upserted": 2,
            "typed_upserted": 1,
            "skipped_typed": 1,
        },
        __import__("uuid").uuid4(),
    )

    assert verified is False
    assert item.status == "pending"
    assert item.error_message == "registros_en_cuarentena:1"
