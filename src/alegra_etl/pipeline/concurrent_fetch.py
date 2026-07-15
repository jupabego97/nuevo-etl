"""Paginación concurrente acotada y callbacks por página."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from alegra_etl.alegra.client import AlegraClient, AlegraClientError

logger = logging.getLogger(__name__)

PageCallback = Callable[[int, list[dict[str, Any]], dict[str, Any] | None], Awaitable[None]]


@dataclass
class PageBatchResult:
    pages_fetched: int
    records_extracted: int
    completed: bool
    next_offset: int
    total_known: int | None = None


def _compute_next_offset(
    results: list[tuple[int, list[dict[str, Any]], dict[str, Any] | None]],
    page_size: int,
    start_offset: int,
) -> int:
    if not results:
        return start_offset
    last_offset = max(offset for offset, page, _ in results if page)
    last_page = next(page for offset, page, _ in results if offset == last_offset)
    if not last_page:
        return start_offset
    return last_offset + page_size


def _is_batch_complete(
    results: list[tuple[int, list[dict[str, Any]], dict[str, Any] | None]],
    *,
    requested_offsets: list[int],
    page_size: int,
    total: int | None,
    start_offset: int,
) -> bool:
    if not results:
        return True

    non_empty = [(o, p) for o, p, _ in results if p]
    if not non_empty:
        return True

    if total is not None:
        next_off = _compute_next_offset(results, page_size, start_offset)
        return next_off >= total

    # Sin metadata: completar solo si alguna página vino corta o vacía al final del lote
    if len(non_empty) < len(requested_offsets):
        return True

    _, last_page = max(non_empty, key=lambda x: x[0])
    if len(last_page) < page_size:
        return True

    return False


async def fetch_page_batch(
    client: AlegraClient,
    endpoint: str,
    *,
    extra_params: dict[str, Any] | None = None,
    order_field: str = "id",
    order_direction: str = "ASC",
    start_offset: int = 0,
    max_pages: int = 5,
    on_page: PageCallback | None = None,
    allow_parallel: bool = True,
) -> PageBatchResult:
    """Descarga un lote de páginas; persistencia del callback sigue serial."""
    params = dict(extra_params or {})
    use_order = bool(order_field) and allow_parallel
    if use_order:
        params["order_field"] = order_field
        params["order_direction"] = order_direction

    page_size = client.settings.sync_page_size
    total: int | None = None
    if client.settings.backfill_require_metadata:
        try:
            total = await client.get_total_count(endpoint, extra_params=params if use_order else extra_params)
        except AlegraClientError:
            total = None

    offsets: list[int] = []
    current = start_offset
    for _ in range(max_pages):
        if total is not None and current >= total:
            break
        offsets.append(current)
        current += page_size

    if not offsets:
        return PageBatchResult(
            pages_fetched=0,
            records_extracted=0,
            completed=True,
            next_offset=start_offset,
            total_known=total,
        )

    if not allow_parallel or len(offsets) == 1:
        results: list[tuple[int, list[dict[str, Any]], dict[str, Any] | None]] = []
        for offset in offsets:
            _, page, meta = await _fetch_one_page(
                client, endpoint, offset, page_size, params, use_order
            )
            results.append((offset, page, meta))
            if not page or (total is None and len(page) < page_size):
                break
    else:
        semaphore = asyncio.Semaphore(client.settings.sync_max_concurrent)

        async def _fetch_one(offset: int) -> tuple[int, list[dict[str, Any]], dict[str, Any] | None]:
            async with semaphore:
                return await _fetch_one_page(client, endpoint, offset, page_size, params, use_order)

        tasks = [_fetch_one(offset) for offset in offsets]
        results = await asyncio.gather(*tasks)
        results.sort(key=lambda item: item[0])

    pages_fetched = 0
    records_extracted = 0
    last_meta: dict[str, Any] | None = None
    for offset, page, meta in results:
        if not page:
            continue
        pages_fetched += 1
        records_extracted += len(page)
        last_meta = meta
        if on_page:
            await on_page(offset, page, meta)

    if last_meta and "total" in last_meta and total is None:
        total = int(last_meta["total"])

    next_offset = _compute_next_offset(results, page_size, start_offset)
    completed = _is_batch_complete(
        results,
        requested_offsets=offsets,
        page_size=page_size,
        total=total,
        start_offset=start_offset,
    )

    return PageBatchResult(
        pages_fetched=pages_fetched,
        records_extracted=records_extracted,
        completed=completed,
        next_offset=next_offset if not completed else 0,
        total_known=total,
    )


async def _fetch_one_page(
    client: AlegraClient,
    endpoint: str,
    offset: int,
    page_size: int,
    params: dict[str, Any],
    use_order: bool,
) -> tuple[int, list[dict[str, Any]], dict[str, Any] | None]:
    page_params = dict(params)
    try:
        page, meta = await client.get_page(
            endpoint,
            start=offset,
            limit=page_size,
            extra_params=page_params,
        )
    except AlegraClientError as exc:
        if exc.status_code == 400 and use_order:
            page_params.pop("order_field", None)
            page_params.pop("order_direction", None)
            page, meta = await client.get_page(
                endpoint,
                start=offset,
                limit=page_size,
                extra_params=page_params,
            )
        else:
            raise
    return offset, page, meta


async def fetch_date_page_batch(
    client: AlegraClient,
    endpoint: str,
    target_date: str,
    *,
    extra_params: dict[str, Any] | None = None,
    start_offset: int = 0,
    max_pages: int = 5,
    on_page: PageCallback | None = None,
) -> PageBatchResult:
    """Paginación concurrente para un día concreto."""
    params = dict(extra_params or {})
    params["date"] = target_date
    page_size = client.settings.sync_page_size

    total: int | None = None
    try:
        total = await client.get_total_count(endpoint, extra_params=params)
    except AlegraClientError:
        total = None

    offsets: list[int] = []
    current = start_offset
    for _ in range(max_pages):
        if total is not None and current >= total:
            break
        offsets.append(current)
        current += page_size

    if not offsets:
        return PageBatchResult(0, 0, True, start_offset, total_known=total)

    semaphore = asyncio.Semaphore(client.settings.sync_max_concurrent)

    async def _fetch_one(offset: int) -> tuple[int, list[dict[str, Any]], dict[str, Any] | None]:
        async with semaphore:
            try:
                page, meta = await client.get_page(
                    endpoint,
                    start=offset,
                    limit=page_size,
                    extra_params=params,
                )
            except AlegraClientError as exc:
                # Solo en offset 0 reintentar sin params extra (no quitar type=...)
                if exc.status_code == 400 and offset == 0 and len(params) > 1:
                    minimal = {"date": target_date}
                    if "type" in params:
                        minimal["type"] = params["type"]
                    page, meta = await client.get_page(
                        endpoint,
                        start=0,
                        limit=page_size,
                        extra_params=minimal,
                    )
                else:
                    raise
            return offset, page, meta

    tasks = [_fetch_one(offset) for offset in offsets]
    results = await asyncio.gather(*tasks)
    results.sort(key=lambda item: item[0])

    pages_fetched = 0
    records_extracted = 0
    for offset, page, meta in results:
        if not page:
            continue
        pages_fetched += 1
        records_extracted += len(page)
        if meta and "total" in meta and total is None:
            total = int(meta["total"])
        if on_page:
            await on_page(offset, page, meta)

    next_offset = _compute_next_offset(results, page_size, start_offset)
    completed = _is_batch_complete(
        results,
        requested_offsets=offsets,
        page_size=page_size,
        total=total,
        start_offset=start_offset,
    )

    return PageBatchResult(
        pages_fetched=pages_fetched,
        records_extracted=records_extracted,
        completed=completed,
        next_offset=next_offset if not completed else 0,
        total_known=total,
    )
