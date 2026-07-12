"""Pruebas de resolución de id canónico."""

from alegra_etl.pipeline.source_loader import _resolve_alegra_id


def test_resolve_id_prefers_id_field():
    assert _resolve_alegra_id({"id": 42, "name": "x"}, "invoices") == "42"


def test_resolve_company_singleton_without_id():
    assert _resolve_alegra_id({"name": "Mi Empresa", "email": "a@b.com"}, "company") == "singleton"


def test_resolve_unknown_without_id_returns_none():
    assert _resolve_alegra_id({"name": "x"}, "estimates") is None
