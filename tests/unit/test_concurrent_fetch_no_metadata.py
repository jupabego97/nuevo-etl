"""Paginación sin metadata confiable."""

import httpx
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


@pytest.mark.asyncio
async def test_intermediate_empty_page_blocks_later_pages(settings, httpx_mock):
    async def response_for_offset(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["start"])
        pages = {
            0: [{"id": str(i)} for i in range(30)],
            30: [],
            60: [{"id": "60"}],
        }
        return httpx.Response(
            200,
            json={"data": pages[offset], "metadata": {"total": 90}},
        )

    httpx_mock.add_callback(response_for_offset, method="GET", is_reusable=True)

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "invoices",
            start_offset=0,
            max_pages=3,
        )

    assert result.intermediate_gap is True
    assert result.completed is False
    assert result.next_offset == 30
    assert result.pages_fetched == 1


@pytest.mark.asyncio
async def test_short_page_completes_even_with_inflated_total(settings, httpx_mock):
    """metadata.total mentiroso no debe forzar más offsets tras una página corta."""
    httpx_mock.add_response(
        method="GET",
        json={"data": [{"id": str(i)} for i in range(6)], "metadata": {"total": 200000}},
    )

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "categories",
            start_offset=0,
            max_pages=5,
            allow_parallel=False,
        )

    assert result.completed is True
    assert result.next_offset == 0
    assert result.pages_fetched == 1
    assert result.records_extracted == 6


@pytest.mark.asyncio
async def test_repeated_pages_across_offsets_stop_pagination(settings, httpx_mock):
    """Si la API ignora start y repite el mismo lote, se corta el avance infinito."""

    async def same_page(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    {"id": "1"},
                    {"id": "2"},
                    {"id": "3"},
                    {"id": "4"},
                    {"id": "5"},
                    {"id": "6"},
                ],
                "metadata": {"total": 200000},
            },
        )

    httpx_mock.add_callback(same_page, method="GET", is_reusable=True)

    async with AlegraClient(settings) as client:
        result = await fetch_page_batch(
            client,
            "categories",
            start_offset=126600,
            max_pages=3,
            allow_parallel=True,
        )

    assert result.completed is True
    assert result.next_offset == 0
    assert result.pages_fetched == 1
    assert result.records_extracted == 6
