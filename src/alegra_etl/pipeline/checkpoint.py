"""Gestión de checkpoints reanudables por recurso."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import ResourceDefinition, ResourcePriority, SyncStrategy
from alegra_etl.config import Settings
from alegra_etl.db.models import SyncCheckpoint
from alegra_etl.pipeline.checkpoint_integrity import checkpoint_issues, is_truly_complete
from alegra_etl.pipeline.completion_gate import can_mark_backfill_completed

logger = logging.getLogger(__name__)

_BOGOTA = ZoneInfo("America/Bogota")


def _today_local() -> date:
    return datetime.now(_BOGOTA).date()


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
        today = _today_local()
        if checkpoint:
            self._maybe_repair_inconsistent(checkpoint, resource, today)
            if checkpoint.status != "completed":
                checkpoint.backfill_end_date = today
                if checkpoint.backfill_start_date is None:
                    checkpoint.backfill_start_date = self.backfill_start
                if checkpoint.cursor_date is None and resource.strategy == SyncStrategy.DATE_WINDOW:
                    checkpoint.cursor_date = checkpoint.backfill_start_date or self.backfill_start
                if checkpoint.status in {"running", "failed"}:
                    checkpoint.status = "pending"
            return checkpoint

        checkpoint = SyncCheckpoint(
            company_id=self.company_id,
            resource_name=resource.name,
            status="pending",
            backfill_start_date=self.backfill_start,
            backfill_end_date=today,
            cursor_date=self.backfill_start if resource.strategy == SyncStrategy.DATE_WINDOW else None,
            cursor_offset=0,
            metadata_json={},
        )
        self.session.add(checkpoint)
        self.session.flush()
        return checkpoint

    def _maybe_repair_inconsistent(
        self,
        checkpoint: SyncCheckpoint,
        resource: ResourceDefinition,
        today: date,
    ) -> None:
        """Reabre checkpoints completed con invariantes rotas (incl. legacy con timestamp)."""
        if checkpoint.status != "completed":
            return

        issues = checkpoint_issues(checkpoint, resource, today=today)
        if not issues:
            return

        # FULL critical sin marca de backfill
        if resource.strategy == SyncStrategy.FULL and "full_missing_completed_at" in issues:
            if resource.priority not in {ResourcePriority.CRITICAL, ResourcePriority.HIGH}:
                return

        checkpoint.status = "pending"
        checkpoint.backfill_start_date = checkpoint.backfill_start_date or self.backfill_start
        checkpoint.backfill_end_date = today
        if resource.strategy == SyncStrategy.DATE_WINDOW:
            checkpoint.cursor_date = (
                checkpoint.cursor_date
                if checkpoint.cursor_date and "cursor_not_past_end" not in issues
                else checkpoint.backfill_start_date or self.backfill_start
            )
        checkpoint.cursor_offset = 0
        checkpoint.backfill_completed_at = None
        checkpoint.verified_at = None
        checkpoint.backfill_generation = (getattr(checkpoint, "backfill_generation", None) or 1) + 1
        meta = dict(checkpoint.metadata_json or {})
        meta["repaired_at"] = datetime.now(UTC).isoformat()
        meta["repair_issues"] = issues
        checkpoint.metadata_json = meta
        print(
            f"[checkpoint] Reparando {resource.name}: issues={issues} cursor={checkpoint.cursor_date}",
            flush=True,
        )
        logger.warning("Reparando checkpoint %s: %s", resource.name, issues)

    def _maybe_reopen_false_completed(
        self,
        checkpoint: SyncCheckpoint,
        resource: ResourceDefinition,
        today: date,
    ) -> None:
        """Alias legacy: delega en reparación ampliada."""
        self._maybe_repair_inconsistent(checkpoint, resource, today)

    def is_truly_complete(self, checkpoint: SyncCheckpoint, resource: ResourceDefinition) -> bool:
        return is_truly_complete(checkpoint, resource, today=_today_local())

    def repair_all_inconsistent(self) -> list[str]:
        from alegra_etl.alegra.resources import get_backfill_resources, resource_by_name
        from alegra_etl.pipeline.checkpoint_integrity import repair_checkpoint

        repaired: list[str] = []
        today = _today_local()
        rows = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.company_id)
            .all()
        )
        backfill_names = {r.name for r in get_backfill_resources(self.settings)}
        for row in rows:
            if row.resource_name not in backfill_names:
                continue
            resource = resource_by_name(row.resource_name)
            if not resource:
                continue
            if repair_checkpoint(row, resource, self.settings, reason="startup_repair"):
                repaired.append(row.resource_name)
        return repaired

    def is_backfill_complete(self, resource: ResourceDefinition) -> bool:
        checkpoint = self.get_or_create(resource)
        return self.is_truly_complete(checkpoint, resource)

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
        """Actualiza cursor.

        Para DATE_WINDOW, ``cursor_date`` es el **próximo** día a procesar
        (no el último día ya terminado).
        """
        checkpoint.last_successful_run_id = run_id
        checkpoint.last_synced_at = datetime.now(UTC)

        completed = bool(result.get("completed", 0))
        if resource.strategy == SyncStrategy.DATE_WINDOW:
            cursor_date_str = result.get("cursor_date")
            if cursor_date_str:
                checkpoint.cursor_date = date.fromisoformat(str(cursor_date_str))
            checkpoint.cursor_offset = int(result.get("next_offset", 0))

            end = checkpoint.backfill_end_date or _today_local()
            checkpoint.backfill_end_date = end
            if getattr(checkpoint, "backfill_start_date", None) is None:
                checkpoint.backfill_start_date = self.backfill_start

            if completed:
                checkpoint.cursor_offset = 0
                if (
                    checkpoint.cursor_date
                    and checkpoint.backfill_start_date
                    and checkpoint.backfill_end_date
                    and checkpoint.cursor_date > end
                ):
                    if self.try_mark_backfill_completed(checkpoint, resource):
                        pass
                    else:
                        checkpoint.status = "pending"
                        checkpoint.backfill_completed_at = None
                        checkpoint.verified_at = None
                else:
                    checkpoint.status = "pending"
            else:
                checkpoint.status = "pending"
            return

        checkpoint.cursor_offset = int(result.get("next_offset", 0))
        if completed:
            if getattr(checkpoint, "backfill_start_date", None) is None:
                checkpoint.backfill_start_date = self.backfill_start
            if self.try_mark_backfill_completed(checkpoint, resource):
                checkpoint.cursor_offset = 0
            else:
                checkpoint.status = "pending"
                checkpoint.backfill_completed_at = None
        else:
            checkpoint.status = "pending"

    def try_mark_backfill_completed(
        self,
        checkpoint: SyncCheckpoint,
        resource: ResourceDefinition,
    ) -> bool:
        """Marca completed solo si pasa el gate de reconciliación/verificación."""
        if not can_mark_backfill_completed(self.session, self.settings, checkpoint, resource):
            blockers = checkpoint_issues(checkpoint, resource, today=_today_local())
            logger.warning(
                "No se puede cerrar %s: blockers=%s",
                resource.name,
                blockers,
            )
            return False
        checkpoint.status = "completed"
        checkpoint.backfill_completed_at = datetime.now(UTC)
        checkpoint.verified_at = datetime.now(UTC)
        return True

    def mark_skipped(self, checkpoint: SyncCheckpoint, reason: str) -> None:
        checkpoint.status = "skipped"
        meta = dict(checkpoint.metadata_json or {})
        meta["skip_reason"] = reason
        checkpoint.metadata_json = meta

    def mark_failed(self, checkpoint: SyncCheckpoint, error: str) -> None:
        checkpoint.status = "failed"
        checkpoint.metadata_json = {**(checkpoint.metadata_json or {}), "last_error": error}

    def close_excluded_from_backfill(self) -> None:
        """Cierra checkpoints de recursos que ya no participan en backfill (ej. company)."""
        from alegra_etl.alegra.resources import RESOURCE_REGISTRY

        excluded = {r.name for r in RESOURCE_REGISTRY if not r.include_in_backfill}
        if not excluded:
            return
        now = datetime.now(UTC)
        rows = (
            self.session.query(SyncCheckpoint)
            .filter(
                SyncCheckpoint.company_id == self.company_id,
                SyncCheckpoint.resource_name.in_(excluded),
                SyncCheckpoint.status != "completed",
            )
            .all()
        )
        for row in rows:
            row.status = "completed"
            row.backfill_completed_at = now
            print(
                f"[checkpoint] Cerrando {row.resource_name}: excluido del backfill histórico",
                flush=True,
            )

    def mark_daily_sync(self, resource: ResourceDefinition, run_id: uuid.UUID) -> None:
        """Registra que el daily tocó el recurso sin cerrar el backfill histórico.

        Nunca pone status=completed: eso solo lo hace update_after_batch del backfill
        cuando el cursor supera backfill_end_date (o un FULL termina del todo).
        """
        checkpoint = (
            self.session.query(SyncCheckpoint)
            .filter_by(company_id=self.company_id, resource_name=resource.name)
            .one_or_none()
        )
        now = datetime.now(UTC)
        today = _today_local()
        if checkpoint is None:
            checkpoint = SyncCheckpoint(
                company_id=self.company_id,
                resource_name=resource.name,
                # pending: el histórico aún puede estar incompleto
                status="pending",
                last_successful_run_id=run_id,
                last_synced_at=now,
                backfill_start_date=self.backfill_start,
                backfill_end_date=today,
                cursor_date=self.backfill_start if resource.strategy == SyncStrategy.DATE_WINDOW else None,
                cursor_offset=0,
                metadata_json={"last_daily_sync_at": now.isoformat()},
            )
            self.session.add(checkpoint)
            return

        checkpoint.last_successful_run_id = run_id
        checkpoint.last_synced_at = now
        meta = dict(checkpoint.metadata_json or {})
        meta["last_daily_sync_at"] = now.isoformat()
        checkpoint.metadata_json = meta
        # No tocar status / cursor / backfill_completed_at

    def backfill_window(self, resource: ResourceDefinition, checkpoint: SyncCheckpoint) -> tuple[date, date]:
        end = checkpoint.backfill_end_date or _today_local()
        checkpoint.backfill_end_date = end
        if resource.strategy != SyncStrategy.DATE_WINDOW:
            return self.backfill_start, end

        start = checkpoint.cursor_date or checkpoint.backfill_start_date or self.backfill_start
        if start > end:
            return end, end

        batch_end = min(
            start + timedelta(days=self.settings.backfill_days_per_step - 1),
            end,
        )
        return start, batch_end
