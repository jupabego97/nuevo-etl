"""Fixtures compartidas de pruebas."""

from __future__ import annotations

import os

import pytest

from alegra_etl.config import get_settings

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/alegra_etl_test")
os.environ.setdefault("ALEGRA_EMAIL", "test@example.com")
os.environ.setdefault("ALEGRA_TOKEN", "test-token")
os.environ.setdefault("WEBHOOK_SECRET", "test-webhook-secret")
os.environ.setdefault("DB_SCHEMA", "alegra_etl")


@pytest.fixture
def settings(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", os.environ["DATABASE_URL"])
    monkeypatch.setenv("ALEGRA_EMAIL", os.environ["ALEGRA_EMAIL"])
    monkeypatch.setenv("ALEGRA_TOKEN", os.environ["ALEGRA_TOKEN"])
    monkeypatch.setenv("WEBHOOK_SECRET", os.environ["WEBHOOK_SECRET"])
    cfg = get_settings()
    yield cfg
    get_settings.cache_clear()


@pytest.fixture
def sample_invoice() -> dict:    return {
        "id": "101",
        "date": "2025-06-01",
        "dueDate": "2025-06-15",
        "datetime": "2025-06-01 10:00:00",
        "status": "open",
        "client": {"id": "1", "name": "Cliente Demo"},
        "seller": {"id": "2", "name": "Vendedor Demo"},
        "numberTemplate": {"prefix": "FV-", "number": 1001},
        "currency": {"code": "COP", "exchangeRate": 1},
        "subtotal": 100,
        "discount": 0,
        "total": 119,
        "totalPaid": 119,
        "balance": 0,
        "paymentForm": "CASH",
        "retentions": [],
        "items": [
            {
                "id": "501",
                "name": "Producto A",
                "quantity": 2,
                "price": 50,
                "total": 100,
                "tax": [{"amount": 19}],
            }
        ],
        "payments": [
            {
                "id": "9001",
                "date": "2025-06-01",
                "amount": 119,
                "paymentMethod": "cash",
                "status": "open",
            }
        ],
    }


@pytest.fixture
def sample_bill() -> dict:
    return {
        "id": "201",
        "date": "2025-06-02",
        "dueDate": "2025-06-02",
        "status": "closed",
        "provider": {"id": "10", "name": "Proveedor Demo"},
        "total": 200,
        "totalPaid": 200,
        "balance": 0,
        "purchases": {
            "items": [
                {
                    "id": "501",
                    "name": "Producto A",
                    "quantity": 4,
                    "price": 50,
                    "total": 200,
                }
            ],
            "categories": [],
        },
    }


@pytest.fixture
def sample_item() -> dict:
    return {
        "id": "501",
        "name": "Producto A",
        "reference": "REF-501",
        "status": "active",
        "type": "simple",
        "customFields": [
            {"name": "Código de barras", "value": "123456"},
            {"name": "FAMILIA", "value": "Hardware"},
        ],
        "inventory": {
            "unit": "piece",
            "availableQuantity": 12,
            "unitCost": 45,
            "warehouses": [
                {
                    "id": "1",
                    "name": "Principal",
                    "availableQuantity": 12,
                    "minQuantity": 2,
                    "maxQuantity": 50,
                }
            ],
        },
        "price": [{"idPriceList": "1", "name": "General", "price": 80}],
        "category": {"id": "7", "name": "Ventas"},
    }
