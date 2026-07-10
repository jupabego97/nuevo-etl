"""Gestión de checkpoints reanudables por recurso."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta

from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import ResourceDefinition, SyncStrategy
from alegra_etl.config import Settings
from alegra_etl.db.models import SyncCheckpoint


class CheckpointManager:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
        self.company_id = settings.company_id
        self.backfill_start = date.fromisoformat(settings.backfill_start_date)

    def get_or_create(self, resource: ResourceDefinition) -> SyncCheckpoint:
        checkpoint = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.company_id, resource_name=resource.name)
            .one_or_none()
        )
        if checkpoint:
            return checkpoint

        checkpoint = SyncCheckpoint(
            company_id=self.company_id,
            resource_name=resource.name,
            status="pending",
            backfill_start_date=self.backfill_start,
            backfill_end_date=date.today(),
            cursor_date=self.backfill_start if resource.strategy == SyncStrategy.DATE_WINDOW else None,
            cursor_offset=0,
            metadata_json={},
        )
        self.session.add(checkpoint)
        self.session.flush()
        return checkpoint

    def is_backfill_complete(self, resource: ResourceDefinition) -> bool:
        checkpoint = self.get_or_create(resource)
        return checkpoint.status == "completed"

    def mark_running(self, checkpoint: SyncCheckpoint, run_id: uuid.UUID) -> None:
        checkpoint.status = "running"
        checkpoint.last_successful_run_id = run_id
        checkpoint.last_synced_at = datetime.now(UTC)

    def update_after_batch(
        self,
        checkpoint: SyncCheckpoint,
        resource: ResourceDefinition,
        result: dict[str, int | str],
        run_id: uuid.UUID,
    ) -> None:
        checkpoint.last_successful_run_id = run_id
        checkpoint.last_synced_at = datetime.now(UTC)

        completed = bool(result.get("completed", 0))
        if resource.strategy == SyncStrategy.DATE_WINDOW:
            cursor_date_str = result.get("cursor_date")
            if cursor_date_str:
                checkpoint.cursor_date = date.fromisoformat(str(cursor_date_str))
            checkpoint.cursor_offset = int(result.get("next_offset", 0))
            if completed and checkpoint.cursor_date and checkpoint.backfill_end_date:
                if checkpoint.cursor_date >= checkpoint.backfill_end_date:
                    checkpoint.status = "completed"
                    checkpoint.backfill_completed_at = datetime.now(UTC)
                    checkpoint.cursor_offset = 0
                else:
                    checkpoint.status = "pending"
            elif not completed:
                checkpoint.status = "pending"
            else:
                checkpoint.status = "completed"
                checkpoint.backfill_completed_at = datetime.now(UTC)
        else:
            checkpoint.cursor_offset = int(result.get("next_offset", 0))
            if completed:
                checkpoint.status = "completed"
                checkpoint.backfill_completed_at = datetime.now(UTC)
                checkpoint.cursor_offset = 0
            else:
                checkpoint.status = "pending"

    def mark_failed(self, checkpoint: SyncCheckpoint, error: str) -> None:
        checkpoint.status = "failed"
        checkpoint.metadata_json = {**checkpoint.metadata_json, "last_error": error}

    def mark_daily_sync(self, resource_name: str, run_id: uuid.UUID) -> None:
        checkpoint = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.company_id, resource_name=resource_name)
            .one_or_none()
        )
        now = datetime.now(UTC)
        if checkpoint is None:
            checkpoint = SyncCheckpoint(
                company_id=self.company_id,
                resource_name=resource_name,
                status="completed",
                last_successful_run_id=run_id,
                last_synced_at=now,
            )
            self.session.add(checkpoint)
        else:
            checkpoint.last_successful_run_id = run_id
            checkpoint.last_synced_at = now

    def backfill_window(self, resource: ResourceDefinition, checkpoint: SyncCheckpoint) -> tuple[date, date]:
        end = checkpoint.backfill_end_date or date.today()
        if resource.strategy != SyncStrategy.DATE_WINDOW:
            return self.backfill_start, end

        start = checkpoint.cursor_date or checkpoint.backfill_start_date or self.backfill_start
        batch_end = min(
            start + timedelta(days=self.settings.backfill_days_per_step - 1),
            end,
        )
        return start, batch_end
