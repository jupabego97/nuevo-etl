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


@pytest.mark.asyncio
async def test_get_by_date_preserves_business_filters_on_fallback(settings, httpx_mock):
    httpx_mock.add_response(method="GET", status_code=400, json={"message": "unsupported option"})
    httpx_mock.add_response(method="GET", json={"data": [{"id": "1"}]})

    async with AlegraClient(settings) as client:
        records = await client.get_by_date(
            "payments",
            "2022-01-01",
            extra_params={"type": "in"},
        )

    assert [record["id"] for record in records] == ["1"]
    requests = httpx_mock.get_requests()
    assert len(requests) == 2
    assert requests[1].url.params["type"] == "in"
    assert requests[1].url.params["date"] == "2022-01-01"


@pytest.mark.asyncio
async def test_get_page_unwraps_results_total_envelope(settings, httpx_mock):
    """Alegra /taxes responde {results, total}; no debe tratarse como un solo registro."""
    httpx_mock.add_response(
        method="GET",
        json={
            "results": [
                {"id": "1", "name": "IVA", "percentage": 19},
                {"id": "2", "name": "IVA", "percentage": 5},
            ],
            "total": 2,
        },
    )

    async with AlegraClient(settings) as client:
        page, meta = await client.get_page("taxes")

    assert [record["id"] for record in page] == ["1", "2"]
    assert meta == {"total": 2}
