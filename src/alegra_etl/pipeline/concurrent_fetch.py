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
) -> PageBatchResult:
    """Descarga un lote de páginas en paralelo respetando rate limit global."""
    params = dict(extra_params or {})
    use_order = bool(order_field)
    if use_order:
        params["order_field"] = order_field
        params["order_direction"] = order_direction

    page_size = client.settings.sync_page_size
    total: int | None = None
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
        )

    semaphore = asyncio.Semaphore(client.settings.sync_max_concurrent)

    async def _fetch_one(offset: int) -> tuple[int, list[dict[str, Any]], dict[str, Any] | None]:
        async with semaphore:
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

    next_offset = start_offset + pages_fetched * page_size
    if total is not None:
        completed = next_offset >= total
    elif pages_fetched < len(offsets):
        completed = True
    else:
        completed = pages_fetched == 0

    if last_meta and "total" in last_meta:
        total = int(last_meta["total"])
        completed = next_offset >= total

    return PageBatchResult(
        pages_fetched=pages_fetched,
        records_extracted=records_extracted,
        completed=completed,
        next_offset=next_offset,
    )


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

    offsets: list[int] = []
    current = start_offset
    for _ in range(max_pages):
        offsets.append(current)
        current += page_size

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
                if exc.status_code == 400 and offset == 0:
                    page, meta = await client.get_page(
                        endpoint,
                        start=0,
                        limit=page_size,
                        extra_params={"date": target_date},
                    )
                else:
                    raise
            return offset, page, meta

    tasks = [_fetch_one(offset) for offset in offsets]
    results = await asyncio.gather(*tasks)
    results.sort(key=lambda item: item[0])

    pages_fetched = 0
    records_extracted = 0
    total: int | None = None
    for offset, page, meta in results:
        if not page:
            continue
        pages_fetched += 1
        records_extracted += len(page)
        if meta and "total" in meta:
            total = int(meta["total"])
        if on_page:
            await on_page(offset, page, meta)

    next_offset = start_offset + pages_fetched * page_size
    if total is not None:
        completed = next_offset >= total
    elif pages_fetched < len(offsets):
        completed = True
    else:
        completed = pages_fetched == 0

    return PageBatchResult(
        pages_fetched=pages_fetched,
        records_extracted=records_extracted,
        completed=completed,
        next_offset=next_offset,
    )
