"""Cliente HTTP resiliente para Alegra API."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from alegra_etl.alegra.rate_limiter import RateLimiter
from alegra_etl.config import Settings

logger = logging.getLogger(__name__)

RECOVERABLE_STATUS = {429, 500, 502, 503, 504}


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, AlegraClientError):
        return exc.status_code in RECOVERABLE_STATUS or exc.status_code is None
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError))


class AlegraClientError(Exception):
    def __init__(self, message: str, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class AlegraClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.rate_limiter = RateLimiter(max_requests_per_minute=150)
        self._client = httpx.AsyncClient(
            base_url=settings.alegra_base_url.rstrip("/"),
            headers={
                "Accept": "application/json",
                "Authorization": settings.alegra_authorization_header(),
            },
            timeout=httpx.Timeout(
                connect=15.0,
                read=float(settings.sync_request_timeout_seconds),
                write=30.0,
                pool=15.0,
            ),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AlegraClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=60),
        reraise=True,
    )
    async def _request(self, method: str, endpoint: str, params: dict[str, Any] | None = None) -> httpx.Response:
        await self.rate_limiter.acquire()
        response = await self._client.request(method, endpoint.lstrip("/"), params=params)
        self.rate_limiter.update_from_headers(
            response.headers.get("X-Rate-Limit-Remaining"),
            response.headers.get("X-Rate-Limit-Reset"),
        )
        if response.status_code in RECOVERABLE_STATUS:
            raise AlegraClientError(
                f"Error recuperable {response.status_code} en {endpoint}",
                status_code=response.status_code,
                payload=_safe_json(response),
            )
        if response.status_code == 403:
            raise AlegraClientError(
                "Recurso no disponible para este plan/país",
                status_code=403,
                payload=_safe_json(response),
            )
        if response.status_code >= 400:
            raise AlegraClientError(
                f"Error {response.status_code} en {endpoint}",
                status_code=response.status_code,
                payload=_safe_json(response),
            )
        return response

    async def get_page(
        self,
        endpoint: str,
        *,
        start: int = 0,
        limit: int | None = None,
        extra_params: dict[str, Any] | None = None,
        metadata: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        params: dict[str, Any] = {
            "start": start,
            "limit": limit or self.settings.sync_page_size,
        }
        if metadata:
            params["metadata"] = "true"
        if extra_params:
            params.update({k: v for k, v in extra_params.items() if v is not None})

        response = await self._request("GET", endpoint, params=params)
        body = _safe_json(response)
        if isinstance(body, dict) and "data" in body:
            records = body.get("data") or []
            meta = body.get("metadata")
            return list(records), meta if isinstance(meta, dict) else None
        if isinstance(body, list):
            return body, None
        # Endpoints como /company devuelven un objeto único.
        if isinstance(body, dict) and body:
            return [body], {"total": 1}
        return [], None

    async def get_total_count(
        self,
        endpoint: str,
        extra_params: dict[str, Any] | None = None,
    ) -> int:
        _, meta = await self.get_page(endpoint, start=0, limit=1, extra_params=extra_params, metadata=True)
        if meta and "total" in meta:
            return int(meta["total"])
        records, _ = await self.get_page(endpoint, start=0, limit=self.settings.sync_page_size, extra_params=extra_params)
        return len(records)

    async def fetch_all_pages(
        self,
        endpoint: str,
        extra_params: dict[str, Any] | None = None,
        order_field: str = "id",
        order_direction: str = "ASC",
    ) -> list[dict[str, Any]]:
        params = dict(extra_params or {})
        params.setdefault("order_field", order_field)
        params.setdefault("order_direction", order_direction)

        total = await self.get_total_count(endpoint, extra_params=params)
        if total == 0:
            return []

        page_size = self.settings.sync_page_size
        all_records: list[dict[str, Any]] = []
        for start in range(0, total, page_size):
            page, _ = await self.get_page(endpoint, start=start, limit=page_size, extra_params=params)
            if not page:
                raise AlegraClientError(
                    f"Página incompleta en {endpoint}: start={start}, esperado hasta {total}",
                    status_code=None,
                )
            all_records.extend(page)
        if len(all_records) != total:
            logger.warning(
                "Conteo divergente en %s: metadata=%s registros=%s",
                endpoint,
                total,
                len(all_records),
            )
        return all_records

    async def get_by_id(self, endpoint_template: str, resource_id: str) -> dict[str, Any]:
        endpoint = endpoint_template.format(id=resource_id)
        response = await self._request("GET", endpoint)
        body = _safe_json(response)
        if isinstance(body, dict):
            return body
        raise AlegraClientError(f"Respuesta inesperada para {endpoint}", payload=body)

    async def get_by_date(
        self,
        endpoint: str,
        target_date: str,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params = dict(extra_params or {})
        params["date"] = target_date
        total = await self.get_total_count(endpoint, extra_params=params)
        if total == 0:
            return []
        page_size = self.settings.sync_page_size
        records: list[dict[str, Any]] = []
        for start in range(0, total, page_size):
            page, _ = await self.get_page(endpoint, start=start, limit=page_size, extra_params=params)
            records.extend(page)
        return records


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except json.JSONDecodeError:
        return {"raw_text": response.text[:500]}


def hash_payload(payload: Any) -> str:
    normalized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode()).hexdigest()


def hash_request(params: dict[str, Any]) -> str:
    return hash_payload(params)
