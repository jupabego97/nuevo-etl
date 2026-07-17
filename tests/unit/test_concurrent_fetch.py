"""Pruebas de paginación concurrente."""

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
    monkeypatch.setenv("SYNC_MAX_CONCURRENT", "3")
    cfg = get_settings()
    yield cfg
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fetch_page_batch_concurrent_offsets(settings, httpx_mock):
    for start in (0, 30, 60):
        httpx_mock.add_response(
            method="GET",
            json={
                "data": [{"id": str(start + i)} for i in range(30)],
                "metadata": {"total": 90},
            },
        )

    pages = []

    async with AlegraClient(settings) as client:

        async def on_page(offset, page, _meta):
            pages.append((offset, len(page)))

        result = await fetch_page_batch(
            client,
            "items",
            start_offset=0,
            max_pages=3,
            on_page=on_page,
        )

    assert result.pages_fetched == 3
    assert result.records_extracted == 90
    assert result.completed is True
    assert pages == [(0, 30), (30, 30), (60, 30)]


@pytest.mark.asyncio
async def test_fetch_page_batch_resumes_from_offset(settings, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        json={
            "data": [{"id": str(30 + i)} for i in range(30)],
            "metadata": {"total": 90},
        },
    )

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "items",
            start_offset=30,
            max_pages=1,
        )

    assert result.next_offset == 60
    assert result.completed is False
