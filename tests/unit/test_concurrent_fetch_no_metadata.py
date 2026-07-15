"""Paginación sin metadata confiable."""

import pytest

from alegra_etl.alegra.client import AlegraClient
from alegra_etl.config import get_settings
from alegra_etl.pipeline.concurrent_fetch import fetch_page_batch


@pytest.fixture
def settings(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/test")
    monkeypatch.setenv("ALEGRA_EMAIL", "test@example.com")
    monkeypatch.setenv("ALEGRA_TOKEN", "token")
    monkeypatch.setenv("WEBHOOK_SECRET", "secret")
    cfg = get_settings()
    yield cfg
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_no_metadata_full_page_not_complete(settings, httpx_mock):
    """Sin metadata, una página llena no debe marcar completed."""
    httpx_mock.add_response(method="GET", json={"data": [{"id": str(i)} for i in range(30)]})

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "invoices",
            start_offset=0,
            max_pages=1,
            allow_parallel=False,
        )

    assert result.pages_fetched == 1
    assert result.completed is False
    assert result.next_offset == 30


@pytest.mark.asyncio
async def test_no_metadata_short_page_completes(settings, httpx_mock):
    httpx_mock.add_response(method="GET", json={"data": [{"id": "1"}, {"id": "2"}]})

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "invoices",
            start_offset=0,
            max_pages=1,
            allow_parallel=False,
        )

    assert result.completed is True
    assert result.next_offset == 0
