import pytest

from alegra_etl.alegra.client import AlegraClient, AlegraClientError
from alegra_etl.config import get_settings


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
async def test_fetch_all_pages_uses_offset_pagination(settings, httpx_mock):
    httpx_mock.add_response(
        method="GET",
        json={"data": [{"id": "1"}], "metadata": {"total": 2}},
    )
    httpx_mock.add_response(
        method="GET",
        json={"data": [{"id": "2"}], "metadata": {"total": 2}},
    )

    async with AlegraClient(settings) as client:
        records = await client.fetch_all_pages("items", extra_params={"mode": "advanced"})
    assert len(records) == 2
    assert [r["id"] for r in records] == ["1", "2"]


@pytest.mark.asyncio
async def test_fetch_all_pages_retries_without_order_on_400(settings, httpx_mock):
    httpx_mock.add_response(method="GET", status_code=400, json={"message": "bad order"})
    httpx_mock.add_response(method="GET", json=[{"id": "1"}, {"id": "2"}])

    async with AlegraClient(settings) as client:
        records = await client.fetch_all_pages("item-categories")
    assert len(records) == 2


@pytest.mark.asyncio
async def test_client_retries_recoverable_status(settings, httpx_mock):
    httpx_mock.add_response(method="GET", status_code=429, json={"message": "rate limit"})
    httpx_mock.add_response(method="GET", json=[{"id": "1"}])

    async with AlegraClient(settings) as client:
        page, _ = await client.get_page("items")
    assert page[0]["id"] == "1"


@pytest.mark.asyncio
async def test_client_raises_on_403(settings, httpx_mock):
    httpx_mock.add_response(method="GET", status_code=403, json={"message": "forbidden"})

    async with AlegraClient(settings) as client:
        with pytest.raises(AlegraClientError) as exc:
            await client.get_page("global-invoices")
    assert exc.value.status_code == 403
