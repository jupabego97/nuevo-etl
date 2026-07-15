"""Pruebas de carga tipada resiliente."""

from unittest.mock import MagicMock

import pytest

from alegra_etl.alegra.resources import resource_by_name
from alegra_etl.pipeline.typed_loader import transform_and_load_resilient


def test_resilient_loader_skips_invalid_record(monkeypatch):
    resource = resource_by_name("invoices")
    assert resource is not None
    session = MagicMock()

    calls = {"n": 0}

    def fake_transform(session, res, records, company_id):
        calls["n"] += 1
        if records[0]["id"] == "bad":
            raise ValueError("fecha inválida")
        return 1

    monkeypatch.setattr(
        "alegra_etl.pipeline.typed_loader.transform_and_load",
        fake_transform,
    )

    records = [
        {"id": "good", "date": "2022-01-01"},
        {"id": "bad"},
        {"id": "good2", "date": "2022-01-02"},
    ]
    loaded, skipped = transform_and_load_resilient(session, resource, records, 1)
    assert loaded == 2
    assert skipped == 1
    assert session.add.called
