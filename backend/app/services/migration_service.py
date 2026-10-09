"""API-side control of migration jobs: create, analyse, select, start, pause, resume, cancel, retry.

The API never does Telegram work for these: it validates, flips database state with compare-and-set transitions and
enqueues a task. The worker (app/workers/engine.py) performs the actual processing.
"""
from __future__ import annotations

import uuid

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import AppError, ConflictError, ForbiddenError, TooManyRequestsError
from app.db.base import utcnow
from app.domain import AnalysisState, Eligibility, JobStatus, MemberState, PauseReason
from app.models import MigrationJob, MigrationMember, TelegramAccount
from app.services.analysis_service import get_job
from app.services.audit import record_audit
from app.services.events import EventType, append_event
from app.services.job_state import ACTIVE, InvalidTransition, cas_transition
from app.services.job_view import RETRYABLE_STATES, cooldown_seconds_left, worker_is_active
from app.services.telegram_service import get_owned_channel
from app.workers.queue import (ANALYZE_JOB, RUN_MIGRATION, JobQueue, QueueUnavailable, analyze_job_id, minute_token,
                               run_job_id)


async def _audit(db, user_id, action, job, ip, **details) -> None:
    await record_audit(db, action=action, user_id=user_id, resource_type="migration_job", resource_id=job.id, ip=ip,
                       details=details or None)


def _selected_count_stmt(job_id: uuid.UUID):
    return select(func.count()).select_from(MigrationMember).where(MigrationMember.job_id == job_id,
                                                                   MigrationMember.selected.is_(True))


# ------------------------------------------------------------------------------------------------ create / analyse
async def create_job(db: AsyncSession, *, settings: Settings, user_id: uuid.UUID, account_id: uuid.UUID | None,
                     source_channel_id: uuid.UUID, destination_channel_id: uuid.UUID, member_limit: int | None,
                     ip: str | None) -> MigrationJob:
    if source_channel_id == destination_channel_id:
        raise AppError("Source and destination must be different channels.", code="same_channel")
    src = await get_owned_channel(db, user_id, source_channel_id)
    dst = await get_owned_channel(db, user_id, destination_channel_id)
    if src.account_id != dst.account_id or (account_id is not None and account_id != src.account_id):
        raise AppError("Both channels must belong to the same connected Telegram account.",
                       code="channel_account_mismatch")
    if src.telegram_id == dst.telegram_id:
        raise AppError("Source and destination must be different channels.", code="same_channel")
    # Stored rights are a cheap pre-check; the worker re-verifies them LIVE before analysing and before inviting.
    if not src.is_admin:
        raise ForbiddenError("You are not an administrator of the source channel/group.", code="source_not_admin")
    if not dst.can_invite_users:
        raise ForbiddenError("You lack the 'invite users' permission in the destination.",
                             code="destination_cannot_invite")
    limit = max(1, min(member_limit or settings.eligibility_default_limit, settings.eligibility_max_limit))
    job = MigrationJob(user_id=user_id, account_id=src.account_id, source_channel_id=src.id,
                       destination_channel_id=dst.id, source_title=src.title, destination_title=dst.title,
                       member_limit=limit, analysis_state=AnalysisState.NOT_ANALYZED)
    db.add(job)
    await db.flush()
    await append_event(db, job.id, EventType.CREATED, "Migration created.", status="PENDING",
                       data={"member_limit": limit})
    await _audit(db, user_id, "MIGRATION_CREATED", job, ip, source=src.title, destination=dst.title)
    await db.commit()
    return job


async def request_analysis(db: AsyncSession, *, queue: JobQueue, user_id: uuid.UUID, job_id: uuid.UUID,
                           ip: str | None) -> MigrationJob:
    job = await get_job(db, user_id, job_id)
    if job.status is not JobStatus.PENDING:
        raise ConflictError("Eligibility can only be (re)analysed before the migration starts.",
                            code="invalid_state")
    previous = job.analysis_state
    if previous is AnalysisState.ANALYZING:
        raise ConflictError("An analysis is already running for this migration.", code="analysis_running")
    won = await db.execute(update(MigrationJob).where(MigrationJob.id == job.id, MigrationJob.status == JobStatus.PENDING,
                                                      MigrationJob.analysis_state != AnalysisState.ANALYZING)
                           .values(analysis_state=AnalysisState.ANALYZING, analysis_note=None, total_members=0,
                                   updated_at=utcnow()))
    if won.rowcount != 1:
        raise ConflictError("The migration changed; reload and try again.", code="conflict")
    await db.execute(delete(MigrationMember).where(MigrationMember.job_id == job.id))
    await _audit(db, user_id, "MIGRATION_ANALYSIS_REQUESTED", job, ip)
    await db.commit()
    try:
        await queue.enqueue(ANALYZE_JOB, str(job.id), job_id=analyze_job_id(job.id, minute_token(utcnow())))
    except QueueUnavailable:
        # The old member rows are already gone, so NOT_ANALYZED is the only consistent state to fall back to.
        await db.execute(update(MigrationJob).where(MigrationJob.id == job.id)
                         .values(analysis_state=AnalysisState.NOT_ANALYZED))
        await db.commit()
        raise
    await db.refresh(job)
    return job


async def set_selection(db: AsyncSession, *, user_id: uuid.UUID, job_id: uuid.UUID, telegram_user_ids: list[int] | None,
                        selected: bool, ip: str | None) -> tuple[int, int, int]:
    job = await get_job(db, user_id, job_id)
    if job.status is not JobStatus.PENDING or job.analysis_state not in (AnalysisState.COMPLETED,
                                                                         AnalysisState.PARTIAL):
        raise ConflictError("Members can only be (de)selected on an analysed migration that has not started.",
                            code="invalid_state")
    stmt = update(MigrationMember).where(MigrationMember.job_id == job.id,
                                         MigrationMember.eligibility == Eligibility.ELIGIBLE,
                                         MigrationMember.selected.is_(not selected))
    if telegram_user_ids is not None:
        stmt = stmt.where(MigrationMember.telegram_user_id.in_(telegram_user_ids))
    updated = (await db.execute(stmt.values(selected=selected))).rowcount or 0
    ignored = 0
    if telegram_user_ids is not None:
        eligible_ids = set((await db.execute(select(MigrationMember.telegram_user_id).where(
            MigrationMember.job_id == job.id, MigrationMember.eligibility == Eligibility.ELIGIBLE,
            MigrationMember.telegram_user_id.in_(telegram_user_ids)))).scalars().all())
        ignored = len(set(telegram_user_ids) - eligible_ids)
    total = (await db.execute(_selected_count_stmt(job.id))).scalar_one()
    await db.execute(update(MigrationJob).where(MigrationJob.id == job.id).values(total_members=total))
    await append_event(db, job.id, EventType.SELECTION_CHANGED, f"{total} member(s) selected for migration.",
                       data={"updated": updated, "selected_total": total})
    await _audit(db, user_id, "MIGRATION_SELECTION_CHANGED", job, ip, updated=updated, selected_total=total)
    await db.commit()
    return updated, ignored, total


# ------------------------------------------------------------------------------------------------ start
async def start_job(db: AsyncSession, *, queue: JobQueue, user_id: uuid.UUID, job_id: uuid.UUID,
                    acknowledge_partial: bool, ip: str | None) -> MigrationJob:
    job = await get_job(db, user_id, job_id)
    if job.status is not JobStatus.PENDING:
        raise InvalidTransition(job.status, JobStatus.RUNNING)
    if job.analysis_state not in (AnalysisState.COMPLETED, AnalysisState.PARTIAL):
        raise ConflictError("Run the eligibility analysis successfully before starting.", code="not_analyzed")
    if job.analysis_state is AnalysisState.PARTIAL and not acknowledge_partial:
        raise ConflictError("The analysis is PARTIAL (a member list was truncated or rate-limited). Review it and "
                            "start again with acknowledge_partial=true.", code="analysis_partial")
    total = (await db.execute(_selected_count_stmt(job.id))).scalar_one()
    if total == 0:
        raise ConflictError("No eligible members are selected, so there is nothing to migrate.",
                            code="nothing_to_migrate")
    account = await db.get(TelegramAccount, job.account_id) if job.account_id else None
    if account is None or account.user_id != user_id:
        raise ConflictError("The Telegram account of this migration is no longer connected.", code="no_account")
    now = utcnow()
    if not await cas_transition(db, job.id, expected=JobStatus.PENDING, to=JobStatus.RUNNING, now=now,
                                total_members=total, processed_members=0, successful_count=0, skipped_count=0,
                                failed_count=0, rate_limited_count=0, error_message=None, error_code=None,
                                pause_reason=None, pause_requested=False, cancellation_requested=False,
                                worker_id=None, heartbeat_at=None):
        raise ConflictError("The migration was started by someone else.", code="conflict")
    await append_event(db, job.id, EventType.STARTED, f"Migration started for {total} member(s).",
                       status="RUNNING", data={"total": total})
    await _audit(db, user_id, "MIGRATION_STARTED", job, ip, total=total, partial=job.analysis_state.value)
    await db.commit()
    try:
        await queue.enqueue(RUN_MIGRATION, str(job.id), job_id=run_job_id(job.id, minute_token(now)))
    except QueueUnavailable:
        # Never leave a job RUNNING that nobody will run: put it back, tell the caller.
        await db.execute(update(MigrationJob).where(MigrationJob.id == job.id, MigrationJob.status == JobStatus.RUNNING,
                                                    MigrationJob.heartbeat_at.is_(None))
                         .values(status=JobStatus.PENDING, started_at=None))
        await db.commit()
        raise
    await db.refresh(job)
    return job


# ------------------------------------------------------------------------------------------------ pause / resume / cancel
async def pause_job(db: AsyncSession, *, settings: Settings, user_id: uuid.UUID, job_id: uuid.UUID,
                    ip: str | None) -> MigrationJob:
    job = await get_job(db, user_id, job_id)
    now = utcnow()
    if job.status is JobStatus.PAUSED:
        if job.pause_reason is PauseReason.RATE_LIMIT:      # keep it paused after the cooldown: take over from auto-resume
            await db.execute(update(MigrationJob).where(MigrationJob.id == job.id, MigrationJob.status == JobStatus.PAUSED)
                             .values(pause_reason=PauseReason.USER, updated_at=now))
            await _audit(db, user_id, "MIGRATION_PAUSE_REQUESTED", job, ip, takeover=True)
            await db.commit()
            await db.refresh(job)
        return job
    if job.status is not JobStatus.RUNNING:
        raise InvalidTransition(job.status, JobStatus.PAUSED)
    if job.cancellation_requested:
        raise ConflictError("The migration is being cancelled.", code="cancelling")
    if worker_is_active(job, settings.migration_lease_seconds, now):
        await db.execute(update(MigrationJob).where(MigrationJob.id == job.id, MigrationJob.status == JobStatus.RUNNING)
                         .values(pause_requested=True, updated_at=now))
        await append_event(db, job.id, EventType.PAUSE_REQUESTED,
                           "Pause requested; it takes effect after the current member finishes.", status="RUNNING")
        await _audit(db, user_id, "MIGRATION_PAUSE_REQUESTED", job, ip)
    else:                                                    # nobody is processing it: pause right away
        if await cas_transition(db, job.id, expected=JobStatus.RUNNING, to=JobStatus.PAUSED, now=now,
                                pause_reason=PauseReason.USER, pause_requested=False):
            await append_event(db, job.id, EventType.PAUSED, "Paused at your request.", status="PAUSED",
                               reason="USER", data={"pause_reason": "USER"})
            await _audit(db, user_id, "MIGRATION_PAUSED", job, ip)
    await db.commit()
    await db.refresh(job)
    return job


async def resume_job(db: AsyncSession, *, queue: JobQueue, user_id: uuid.UUID, job_id: uuid.UUID,
                     ip: str | None) -> MigrationJob:
    job = await get_job(db, user_id, job_id)
    now = utcnow()
    if job.status is JobStatus.RUNNING and job.pause_requested:   # changed our mind before the worker paused
        await db.execute(update(MigrationJob).where(MigrationJob.id == job.id).values(pause_requested=False))
        await append_event(db, job.id, EventType.RESUMED, "Pause request withdrawn.", status="RUNNING")
        await _audit(db, user_id, "MIGRATION_RESUMED", job, ip, withdrawn=True)
        await db.commit()
        await db.refresh(job)
        return job
    if job.status is not JobStatus.PAUSED:
        raise InvalidTransition(job.status, JobStatus.RUNNING)
    previous_reason = job.pause_reason
    left = cooldown_seconds_left(job, now)
    account = await db.get(TelegramAccount, job.account_id) if job.account_id else None
    if account is not None and account.flood_wait_until and account.flood_wait_until > now:
        left = max(left, int((account.flood_wait_until - now).total_seconds()) + 1)
    if left > 0:
        raise TooManyRequestsError(f"Telegram requested a cooldown; resume is possible in {left} seconds.",
                                   code="cooldown_active", retry_after=left)
    if not await cas_transition(db, job.id, expected=JobStatus.PAUSED, to=JobStatus.RUNNING, now=now,
                                pause_reason=None, pause_requested=False, rate_limited_until=None, error_message=None):
        raise ConflictError("The migration changed; reload and try again.", code="conflict")
    await append_event(db, job.id, EventType.RESUMED, "Migration resumed.", status="RUNNING",
                       data={"automatic": False})
    await _audit(db, user_id, "MIGRATION_RESUMED", job, ip)
    await db.commit()
    try:
        await queue.enqueue(RUN_MIGRATION, str(job.id), job_id=run_job_id(job.id, minute_token(now)))
    except QueueUnavailable:
        await cas_transition(db, job.id, expected=JobStatus.RUNNING, to=JobStatus.PAUSED, now=now,
                             pause_reason=previous_reason or PauseReason.USER)
        await db.commit()
        raise
    await db.refresh(job)
    return job


async def cancel_job(db: AsyncSession, *, settings: Settings, user_id: uuid.UUID, job_id: uuid.UUID,
                     ip: str | None) -> MigrationJob:
    job = await get_job(db, user_id, job_id)
    now = utcnow()
    if job.status is JobStatus.CANCELLING:
        return job                                            # idempotent
    if job.status in (JobStatus.PENDING, JobStatus.PAUSED):
        if await cas_transition(db, job.id, expected=job.status, to=JobStatus.CANCELLED, now=now,
                                pause_requested=False, cancellation_requested=False):
            await append_event(db, job.id, EventType.CANCELLED, "Migration cancelled; previous results kept.",
                               status="CANCELLED")
            await _audit(db, user_id, "MIGRATION_CANCELLED", job, ip)
    elif job.status is JobStatus.RUNNING:
        if worker_is_active(job, settings.migration_lease_seconds, now):
            if await cas_transition(db, job.id, expected=JobStatus.RUNNING, to=JobStatus.CANCELLING, now=now,
                                    cancellation_requested=True):
                await append_event(db, job.id, EventType.CANCEL_REQUESTED,
                                   "Cancellation requested; the current member finishes first.", status="CANCELLING")
                await _audit(db, user_id, "MIGRATION_CANCEL_REQUESTED", job, ip)
        else:
            await db.execute(update(MigrationMember).where(MigrationMember.job_id == job.id,
                                                           MigrationMember.state == MemberState.PROCESSING)
                             .values(state=MemberState.ELIGIBLE))
            if await cas_transition(db, job.id, expected=JobStatus.RUNNING, to=JobStatus.CANCELLED, now=now,
                                    worker_id=None, pause_requested=False, cancellation_requested=False):
                await append_event(db, job.id, EventType.CANCELLED, "Migration cancelled; previous results kept.",
                                   status="CANCELLED")
                await _audit(db, user_id, "MIGRATION_CANCELLED", job, ip)
    else:
        raise InvalidTransition(job.status, JobStatus.CANCELLED)
    await db.commit()
    await db.refresh(job)
    return job


# ------------------------------------------------------------------------------------------------ retry / delete
async def create_retry_job(db: AsyncSession, *, user_id: uuid.UUID, job_id: uuid.UUID, ip: str | None) -> MigrationJob:
    """Re-run what did not finish as a NEW linked job. History stays immutable (COMPLETED never goes back to RUNNING).

    Only members that never reached a final state are copied: FAILED (transient/unknown), RATE_LIMITED and never-processed.
    Permanent refusals (privacy, invalid/banned user), already-members and successes are NOT retried.
    Each invite is still preceded by a live membership check, so nothing is ever invited twice.
    """
    parent = await get_job(db, user_id, job_id)
    if parent.status not in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        raise ConflictError("Finish, cancel or let the migration end before retrying what failed.",
                            code="invalid_state")
    base = select(MigrationMember).where(MigrationMember.job_id == parent.id, MigrationMember.selected.is_(True),
                                         MigrationMember.state.in_(RETRYABLE_STATES))
    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    if total == 0:
        raise ConflictError("Nothing to retry: every selected member reached a final result.", code="nothing_to_retry")
    child = MigrationJob(user_id=user_id, account_id=parent.account_id, source_channel_id=parent.source_channel_id,
                         destination_channel_id=parent.destination_channel_id, retry_of_id=parent.id,
                         source_title=parent.source_title, destination_title=parent.destination_title,
                         member_limit=parent.member_limit, analysis_state=AnalysisState.COMPLETED,
                         analysis_note=f"Retry of migration {parent.id}: only unfinished members are included; "
                                       "membership is re-checked live before every invitation.",
                         analysis_finished_at=utcnow(), total_members=total,
                         source_reported_total=parent.source_reported_total,
                         source_listing_complete=parent.source_listing_complete,
                         destination_listing_complete=parent.destination_listing_complete,
                         destination_banned_known=parent.destination_banned_known)
    db.add(child)
    await db.flush()
    offset = 0
    while True:                                      # batched copy: memory stays bounded for large jobs
        batch = (await db.execute(base.order_by(MigrationMember.position, MigrationMember.id)
                                  .limit(500).offset(offset))).scalars().all()
        if not batch:
            break
        db.add_all(MigrationMember(
            job_id=child.id, telegram_user_id=m.telegram_user_id, access_hash=m.access_hash, username=m.username,
            display_name=m.display_name, role=m.role, position=m.position, selected=True,
            eligibility=Eligibility.ELIGIBLE, state=MemberState.ELIGIBLE, reason=m.reason,
            invite_attempted_at=m.invite_attempted_at) for m in batch)
        await db.flush()
        offset += len(batch)
    await append_event(db, child.id, EventType.CREATED, f"Retry of migration {parent.id} created.", status="PENDING",
                       data={"retry_of": str(parent.id), "members": total})
    await append_event(db, parent.id, EventType.RETRY_CREATED, f"Retry job created for {total} member(s).",
                       data={"retry_job": str(child.id), "members": total})
    await _audit(db, user_id, "MIGRATION_RETRY_CREATED", child, ip, retry_of=str(parent.id), members=total)
    await db.commit()
    return child


async def delete_job(db: AsyncSession, *, user_id: uuid.UUID, job_id: uuid.UUID, ip: str | None) -> None:
    job = await get_job(db, user_id, job_id)
    if job.status in ACTIVE:
        raise ConflictError("Cancel or pause the migration before deleting it.", code="job_active")
    await record_audit(db, action="migration.deleted", user_id=user_id, resource_type="migration_job",
                       resource_id=job.id, ip=ip)
    await db.delete(job)
    await db.commit()
