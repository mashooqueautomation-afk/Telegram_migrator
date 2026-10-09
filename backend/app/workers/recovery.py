"""Failure recovery. Runs on worker start-up and every 30 s (arq cron).

What it repairs (without ever resetting a whole migration):
  * RUNNING jobs whose worker died (stale/no heartbeat)  -> members left PROCESSING go back to ELIGIBLE, the job is re-queued
  * CANCELLING jobs with a dead worker                    -> finalised as CANCELLED
  * RUNNING jobs with a pending pause and a dead worker   -> finalised as PAUSED
  * PAUSED(RATE_LIMIT) jobs whose cooldown has elapsed    -> re-queued (the engine performs the resume)
  * jobs stuck in ANALYZING (worker/API died mid-analysis)-> marked FAILED so the UI is not stuck
"""
from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.logging import log_event
from app.db.base import utcnow
from app.domain import AnalysisState, JobStatus, MemberState, PauseReason
from app.models import MigrationJob, MigrationMember
from app.services.audit import record_audit
from app.services.events import EventType, append_event
from app.services.job_state import cas_transition
from app.workers.queue import RUN_MIGRATION, JobQueue, minute_token, run_job_id

log = logging.getLogger(__name__)


async def _reset_processing(db: AsyncSession, job_id: uuid.UUID, now: datetime) -> int:
    res = await db.execute(update(MigrationMember).where(MigrationMember.job_id == job_id,
                                                         MigrationMember.state == MemberState.PROCESSING)
                           .values(state=MemberState.ELIGIBLE, updated_at=now))
    return res.rowcount or 0


async def recover(*, sessionmaker: async_sessionmaker[AsyncSession], settings: Settings, queue: JobQueue,
                  clock: Callable[[], datetime] = utcnow) -> dict[str, int]:
    now = clock()
    stale_before = now - timedelta(seconds=settings.migration_lease_seconds)
    report = {"requeued": 0, "reset_members": 0, "cancelled": 0, "paused": 0, "cooldown_requeued": 0,
              "analysis_failed": 0}

    async with sessionmaker() as db:
        orphan = (MigrationJob.worker_id.is_(None)) | (MigrationJob.heartbeat_at.is_(None)) | \
                 (MigrationJob.heartbeat_at < stale_before)

        # ---- RUNNING with no live worker ---------------------------------------------------------------------
        for job in (await db.execute(select(MigrationJob).where(MigrationJob.status == JobStatus.RUNNING,
                                                                orphan))).scalars().all():
            had_worker = job.worker_id is not None      # only the FIRST sweep sees a worker; it then clears worker_id
            reset = await _reset_processing(db, job.id, now)
            if had_worker or reset:
                await db.execute(update(MigrationJob).where(MigrationJob.id == job.id)
                                 .values(worker_id=None, current_member_id=None, current_member_label=None))
            report["reset_members"] += reset
            if job.pause_requested and had_worker:
                if await cas_transition(db, job.id, expected=JobStatus.RUNNING, to=JobStatus.PAUSED, now=now,
                                        pause_reason=PauseReason.USER, pause_requested=False):
                    await append_event(db, job.id, EventType.PAUSED, "Paused at your request (worker was gone).",
                                       status="PAUSED", reason="USER", data={"pause_reason": "USER"})
                    report["paused"] += 1
                continue
            if had_worker:
                await append_event(db, job.id, EventType.RECOVERED,
                                   "Worker stopped unexpectedly; the migration continues from the last consistent state.",
                                   data={"reset_members": reset})
                await record_audit(db, action="MIGRATION_RECOVERED", user_id=job.user_id,
                                   resource_type="migration_job", resource_id=job.id, details={"reset_members": reset})
            await db.commit()
            await queue.enqueue(RUN_MIGRATION, str(job.id), job_id=run_job_id(job.id, minute_token(now)))
            report["requeued"] += 1
            log_event(log, "job_requeued", migration_id=job.id, reset_members=reset)

        # ---- CANCELLING with no live worker ------------------------------------------------------------------
        for job in (await db.execute(select(MigrationJob).where(MigrationJob.status == JobStatus.CANCELLING,
                                                                orphan))).scalars().all():
            await _reset_processing(db, job.id, now)
            if await cas_transition(db, job.id, expected=JobStatus.CANCELLING, to=JobStatus.CANCELLED, now=now,
                                    worker_id=None, pause_requested=False, cancellation_requested=False):
                await append_event(db, job.id, EventType.CANCELLED, "Migration cancelled; previous results kept.",
                                   status="CANCELLED")
                await record_audit(db, action="MIGRATION_CANCELLED", user_id=job.user_id,
                                   resource_type="migration_job", resource_id=job.id, details={"recovered": True})
                report["cancelled"] += 1

        # ---- PAUSED by rate limit, cooldown over ---------------------------------------------------------------
        for job in (await db.execute(select(MigrationJob).where(
                MigrationJob.status == JobStatus.PAUSED, MigrationJob.pause_reason == PauseReason.RATE_LIMIT,
                MigrationJob.pause_requested.is_(False), MigrationJob.rate_limited_until <= now))).scalars().all():
            await db.commit()
            await queue.enqueue(RUN_MIGRATION, str(job.id), job_id=run_job_id(job.id, minute_token(now)))
            report["cooldown_requeued"] += 1

        # ---- analysis that never finished ----------------------------------------------------------------------
        cutoff = now - timedelta(seconds=settings.telegram_operation_timeout_seconds + 90)
        for job in (await db.execute(select(MigrationJob).where(MigrationJob.analysis_state == AnalysisState.ANALYZING,
                                                                MigrationJob.updated_at < cutoff))).scalars().all():
            job.analysis_state = AnalysisState.FAILED
            job.analysis_note = "The analysis was interrupted (worker or server restart). Run it again."
            job.analysis_finished_at = now
            await append_event(db, job.id, "ANALYSIS_FAILED", job.analysis_note, status="FAILED")
            report["analysis_failed"] += 1
        await db.commit()

    if any(report.values()):
        log_event(log, "recovery", **report)
    return report
