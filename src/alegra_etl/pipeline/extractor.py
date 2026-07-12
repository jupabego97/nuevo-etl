"""Extracción paginada concurrente con persistencia canónica."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from alegra_etl.alegra.client import AlegraClient, hash_payload, hash_request
from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy
from alegra_etl.config import Settings
from alegra_etl.db.models import RawDocument
from alegra_etl.pipeline.concurrent_fetch import fetch_date_page_batch, fetch_page_batch
from alegra_etl.pipeline.source_loader import upsert_source_documents
from alegra_etl.pipeline.typed_loader import transform_and_load

logger = logging.getLogger(__name__)


def _memory_mb() -> float:
    try:
        import resource as resource_module  # Unix only

        usage = resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss
        if usage > 10_000_000:
            return usage / (1024 * 1024)
        return usage / 1024
    except Exception:
        return 0.0


class ResourceExtractor:
    def __init__(self, settings: Settings, client: AlegraClient, session: Session, run_id: uuid.UUID):
        self.settings = settings
        self.client = client
        self.session = session
        self.run_id = run_id
        self.company_id = settings.company_id

    async def extract_resource(
        self,
        resource: ResourceDefinition,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        start_offset: int = 0,
        max_pages: int | None = None,
    ) -> dict[str, int]:
        if resource.strategy == SyncStrategy.FULL and not resource.supports_pagination:
            records = await self._fetch_single(resource)
            result = await self._persist_records(
                resource, records, request_params=resource.extra_params, page_start=0
            )
            # Un solo documento (p.ej. /company): el lote termina en esta ejecución.
            result["completed"] = 1
            result["next_offset"] = 0
            return result

        if resource.strategy == SyncStrategy.FULL:
            return await self._extract_full_paginated(
                resource,
                start_offset=start_offset,
                max_pages=max_pages or self.settings.backfill_pages_per_step,
            )

        if resource.strategy == SyncStrategy.DATE_WINDOW:
            if not start_date or not end_date:
                end_date = date.today()
                start_date = end_date - timedelta(days=self.settings.sync_overlap_days)
            return await self._extract_date_window(
                resource,
                start_date=start_date,
                end_date=end_date,
                start_offset=start_offset,
                max_pages=max_pages,
            )

        return await self._extract_full_paginated(
            resource,
            start_offset=start_offset,
            max_pages=max_pages or self.settings.backfill_pages_per_step,
        )

    async def extract_resource_by_id(self, resource: ResourceDefinition, resource_id: str) -> dict[str, int]:
        if not resource.detail_endpoint_template:
            raise ValueError(f"Recurso {resource.name} no soporta extracción por ID")
        record = await self.client.get_by_id(resource.detail_endpoint_template, resource_id)
        return await self._persist_records(resource, [record], request_params={"id": resource_id}, page_start=0)

    async def _fetch_single(self, resource: ResourceDefinition) -> list[dict[str, Any]]:
        page, _ = await self.client.get_page(resource.endpoint, extra_params=resource.extra_params)
        return page

    async def _extract_full_paginated(
        self,
        resource: ResourceDefinition,
        *,
        start_offset: int,
        max_pages: int | None,
    ) -> dict[str, int]:
        metrics = {"extracted": 0, "source_upserted": 0, "typed_upserted": 0, "pages": 0}
        offset = start_offset
        pages_per_batch = max_pages or self.settings.backfill_pages_per_step

        while True:
            async def on_page(page_offset: int, page: list[dict[str, Any]], _meta: dict[str, Any] | None) -> None:
                batch = await self._persist_records(
                    resource,
                    page,
                    request_params={**resource.extra_params, "start": page_offset},
                    page_start=page_offset,
                )
                for key in ("extracted", "source_upserted", "typed_upserted"):
                    metrics[key] += batch.get(key, 0)
                metrics["pages"] += 1
                self.session.commit()
                print(
                    f"[extract] {resource.name} offset={page_offset} "
                    f"records={len(page)} mem={_memory_mb():.0f}MB",
                    flush=True,
                )

            result = await fetch_page_batch(
                self.client,
                resource.endpoint,
                extra_params=resource.extra_params,
                order_field=resource.order_field,
                order_direction=resource.order_direction,
                start_offset=offset,
                max_pages=pages_per_batch,
                on_page=on_page,
            )
            offset = result.next_offset
            if result.completed or max_pages is not None:
                metrics["completed"] = int(result.completed)
                metrics["next_offset"] = offset if not result.completed else 0
                return metrics

    async def _extract_date_window(
        self,
        resource: ResourceDefinition,
        *,
        start_date: date,
        end_date: date,
        start_offset: int,
        max_pages: int | None,
    ) -> dict[str, int]:
        metrics = {
            "extracted": 0,
            "source_upserted": 0,
            "typed_upserted": 0,
            "pages": 0,
            "completed": 1,
            "next_offset": 0,
        }
        pages_limit = max_pages or self.settings.backfill_max_pages_per_day
        current = start_date
        total_days = (end_date - start_date).days + 1
        day_index = 0
        offset = start_offset

        while current <= end_date:
            day_index += 1
            day_completed = False
            pages_this_day = 0
            day = current

            def _make_on_page(capture_day: date):
                async def _handler(page_offset: int, page: list[dict[str, Any]], _meta: dict[str, Any] | None) -> None:
                    nonlocal pages_this_day
                    batch = await self._persist_records(
                        resource,
                        page,
                        request_params={
                            "date": capture_day.isoformat(),
                            **resource.extra_params,
                            "start": page_offset,
                        },
                        page_start=page_offset,
                    )
                    for key in ("extracted", "source_upserted", "typed_upserted"):
                        metrics[key] += batch.get(key, 0)
                    metrics["pages"] += 1
                    pages_this_day += 1
                    self.session.commit()
                    print(
                        f"[extract] {resource.name} {capture_day.isoformat()} offset={page_offset} "
                        f"records={len(page)} mem={_memory_mb():.0f}MB",
                        flush=True,
                    )

                return _handler

            on_page = _make_on_page(day)

            while pages_this_day < pages_limit:
                batch_result = await fetch_date_page_batch(
                    self.client,
                    resource.endpoint,
                    day.isoformat(),
                    extra_params=resource.extra_params,
                    start_offset=offset,
                    max_pages=min(self.settings.backfill_pages_per_step, pages_limit - pages_this_day),
                    on_page=on_page,
                )
                offset = batch_result.next_offset
                if batch_result.completed or batch_result.pages_fetched == 0:
                    day_completed = True
                    offset = 0
                    break
                if max_pages is not None:
                    break

            if not day_completed and max_pages is not None:
                # Día a medias: reanudar aquí con el offset actual.
                metrics["completed"] = 0
                metrics["next_offset"] = offset
                metrics["cursor_date"] = current.isoformat()
                print(
                    f"[extract] {resource.name} pausa en {current.isoformat()} "
                    f"offset={offset} (lote acotado)",
                    flush=True,
                )
                return metrics

            print(
                f"[extract] {resource.name} día {current.isoformat()} "
                f"({day_index}/{total_days}) OK",
                flush=True,
            )
            current += timedelta(days=1)

        # cursor_date = próximo día a procesar (el siguiente al último terminado).
        next_cursor = end_date + timedelta(days=1)
        metrics["cursor_date"] = next_cursor.isoformat()
        metrics["completed"] = 1
        metrics["next_offset"] = 0
        print(
            f"[extract] {resource.name} lote OK → próximo cursor={next_cursor.isoformat()}",
            flush=True,
        )
        return metrics

    async def _persist_records(
        self,
        resource: ResourceDefinition,
        records: list[dict[str, Any]],
        *,
        request_params: dict[str, Any],
        page_start: int,
    ) -> dict[str, int]:
        if not records:
            return {"extracted": 0, "source_upserted": 0, "typed_upserted": 0}

        payload = {"records": records, "count": len(records)}
        raw = RawDocument(
            run_id=self.run_id,
            resource_name=resource.name,
            endpoint=resource.endpoint,
            request_params=request_params,
            request_hash=hash_request(request_params),
            page_start=page_start,
            http_status=200,
            payload=payload,
            payload_hash=hash_payload(payload),
            extracted_at=datetime.now(UTC),
        )
        self.session.merge(raw)

        source_upserted = upsert_source_documents(
            self.session,
            company_id=self.company_id,
            resource=resource,
            records=records,
            run_id=self.run_id,
        )
        typed_upserted = transform_and_load(self.session, resource, records, self.company_id)

        return {
            "extracted": len(records),
            "source_upserted": source_upserted,
            "typed_upserted": typed_upserted,
            "loaded": typed_upserted,
        }
