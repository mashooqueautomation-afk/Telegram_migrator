"""Persisted, sequenced migration events (the source of truth for the JSON log AND the SSE stream)."""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import utcnow
from app.models import MigrationEvent, MigrationJob
from app.services.audit import sanitize


class EventType:
    CREATED = "migration.created"
    STARTED = "migration.started"
    PROGRESS = "migration.progress"                 # synthetic snapshot, streamed but not stored
    MEMBER_SUCCESS = "migration.member_success"
    MEMBER_SKIPPED = "migration.member_skipped"
    MEMBER_FAILED = "migration.member_failed"
    RATE_LIMITED = "migration.rate_limited"
    PAUSE_REQUESTED = "migration.pause_requested"
    PAUSED = "migration.paused"
    RESUMED = "migration.resumed"
    CANCEL_REQUESTED = "migration.cancel_requested"
    CANCELLED = "migration.cancelled"
    COMPLETED = "migration.completed"
    FAILED = "migration.failed"
    RECOVERED = "migration.recovered"
    RETRY_CREATED = "migration.retry_created"
    SELECTION_CHANGED = "migration.selection_changed"
    ERRORS_PAUSE = "migration.errors_pause"


async def append_event(db: AsyncSession, job_id: uuid.UUID, event_type: str, message: str, *,
                       member_id: uuid.UUID | None = None, status: str | None = None, reason: str | None = None,
                       error_code: str | None = None, data: dict | None = None) -> int:
    """Insert an event with the next per-job sequence number (inside the caller's transaction)."""
    seq = (await db.execute(update(MigrationJob).where(MigrationJob.id == job_id)
                            .values(event_seq=MigrationJob.event_seq + 1)
                            .returning(MigrationJob.event_seq))).scalar_one()
    db.add(MigrationEvent(job_id=job_id, seq=seq, member_id=member_id, event_type=event_type, message=message,
                          status=status, reason=reason, error_code=error_code,
                          data=sanitize(data) if data else None))
    return seq


def event_to_payload(ev: MigrationEvent) -> dict[str, Any]:
    return {
        "type": ev.event_type, "migration_id": str(ev.job_id), "seq": ev.seq,
        "member_id": str(ev.member_id) if ev.member_id else None, "status": ev.status, "message": ev.message,
        "reason": ev.reason, "error_code": ev.error_code, "metadata": ev.data or {},
        "created_at": ev.created_at.isoformat() if isinstance(ev.created_at, datetime) else None,
    }


def progress_payload(job: MigrationJob, now: datetime | None = None) -> dict[str, Any]:
    from app.services.job_view import cooldown_seconds_left, percent_done
    now = now or utcnow()
    return {
        "type": EventType.PROGRESS, "migration_id": str(job.id), "status": job.status.value,
        "pause_reason": job.pause_reason.value if job.pause_reason else None,
        "pause_requested": job.pause_requested, "cancellation_requested": job.cancellation_requested,
        "processed": job.processed_members, "total": job.total_members, "successful": job.successful_count,
        "skipped": job.skipped_count, "failed": job.failed_count, "rate_limited": job.rate_limited_count,
        "percent": percent_done(job),
        "rate_limited_until": job.rate_limited_until.isoformat() if job.rate_limited_until else None,
        "rate_limit_remaining_seconds": cooldown_seconds_left(job, now),
        "current_member": ({"id": str(job.current_member_id), "label": job.current_member_label}
                           if job.current_member_id else None),
        "error_message": job.error_message,
    }


def sse_frame(event_type: str, payload: dict, event_id: int | None = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event_type}")
    lines.append("data: " + json.dumps(payload, separators=(",", ":"), default=str))
    return "\n".join(lines) + "\n\n"
