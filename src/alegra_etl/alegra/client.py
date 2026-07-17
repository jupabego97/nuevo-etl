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
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.alegra_max_requests_per_minute
        )
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
    async def _request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        await self.rate_limiter.acquire()
        response = await self._client.request(method, endpoint.lstrip("/"), params=params)
        self.rate_limiter.update_from_headers(
            response.headers.get("X-Rate-Limit-Remaining"),
            response.headers.get("X-Rate-Limit-Reset"),
        )
        if response.status_code == 429:
            self.rate_limiter.penalize()
        else:
            self.rate_limiter.reward()
        if response.status_code in RECOVERABLE_STATUS:
            raise AlegraClientError(
                f"Error recuperable {response.status_code} en {endpoint}",
                status_code=response.status_code,
                payload=_safe_json(response),
            )
        if response.status_code == 401:
            raise AlegraClientError(
                "Alegra rechazó las credenciales (401). "
                "Revisa ALEGRA_EMAIL + ALEGRA_TOKEN (o ALEGRA_API_KEY) en el servicio cron de Railway.",
                status_code=401,
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
        # Algunos endpoints (p.ej. /taxes) usan {results, total} en lugar de {data, metadata}.
        if isinstance(body, dict) and "results" in body and isinstance(body.get("results"), list):
            records = body.get("results") or []
            total = body.get("total")
            meta = {"total": int(total)} if total is not None else None
            return list(records), meta
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
    ) -> int | None:
        """Total de registros si metadata lo expone; None si es desconocido."""
        params = dict(extra_params or {})
        try:
            _, meta = await self.get_page(
                endpoint, start=0, limit=1, extra_params=params, metadata=True
            )
            if meta and "total" in meta:
                return int(meta["total"])
        except AlegraClientError as exc:
            if exc.status_code not in {400, 404}:
                raise
            logger.warning("metadata no soportado en %s (%s)", endpoint, exc)
        return None

    async def fetch_all_pages(
        self,
        endpoint: str,
        extra_params: dict[str, Any] | None = None,
        order_field: str = "id",
        order_direction: str = "ASC",
    ) -> list[dict[str, Any]]:
        params = dict(extra_params or {})
        if order_field:
            params["order_field"] = order_field
            params["order_direction"] = order_direction

        page_size = self.settings.sync_page_size
        all_records: list[dict[str, Any]] = []
        start = 0
        use_order = bool(order_field)

        while True:
            try:
                page, meta = await self.get_page(
                    endpoint, start=start, limit=page_size, extra_params=params
                )
            except AlegraClientError as exc:
                if exc.status_code == 400 and use_order:
                    logger.warning(
                        "order_field no soportado en %s; reintentando sin orden", endpoint
                    )
                    params.pop("order_field", None)
                    params.pop("order_direction", None)
                    use_order = False
                    continue
                raise

            if not page:
                break
            all_records.extend(page)
            total = int(meta["total"]) if meta and "total" in meta else None
            if total is not None:
                if len(all_records) >= total:
                    break
            elif len(page) < page_size:
                break
            start += page_size
            # Seguridad ante bucles infinitos
            if start > 1_000_000:
                raise AlegraClientError(f"Paginación excesiva en {endpoint}", status_code=None)

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
        fallback_remove_params: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        params = dict(extra_params or {})
        params["date"] = target_date
        page_size = self.settings.sync_page_size
        records: list[dict[str, Any]] = []
        start = 0
        max_pages = 200  # 200 * 30 = 6000 docs/día tope de seguridad
        page_num = 0

        while page_num < max_pages:
            page_num += 1
            try:
                page, meta = await self.get_page(
                    endpoint, start=start, limit=page_size, extra_params=params
                )
            except AlegraClientError as exc:
                if exc.status_code == 400 and start == 0:
                    fallback_params = dict(params)
                    for key in fallback_remove_params:
                        fallback_params.pop(key, None)
                    page, meta = await self.get_page(
                        endpoint,
                        start=0,
                        limit=page_size,
                        extra_params=fallback_params,
                    )
                    params = fallback_params
                else:
                    raise
            if not page:
                break
            records.extend(page)
            total = int(meta["total"]) if meta and "total" in meta else None
            if total is not None and len(records) >= total:
                break
            if len(page) < page_size:
                break
            start += page_size
        else:
            raise AlegraClientError(
                f"Tope de paginación en {endpoint} date={target_date} "
                f"({max_pages} páginas); rango no verificable",
                status_code=None,
            )
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
