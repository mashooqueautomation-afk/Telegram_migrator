"""Builds the rich job representation the dashboard needs (counters, countdown, allowed actions, recent events)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.base import utcnow
from app.domain import AnalysisState, JobStatus, MemberState, PauseReason
from app.models import MigrationEvent, MigrationJob, MigrationMember
from app.schemas.migration import (ChannelRefOut, CurrentMember, EventOut, JobListItem, JobOut, JobSummary)
from app.services.analysis_service import job_counts

CAVEATS = [
    "ELIGIBLE means no blocking condition was found in the data Telegram provided. Telegram does not reveal "
    "privacy settings in advance, so an invitation may still be refused. Nothing is sent until you start the migration.",
]
RETRYABLE_STATES = (MemberState.FAILED, MemberState.RATE_LIMITED, MemberState.ELIGIBLE, MemberState.PROCESSING)


def cooldown_seconds_left(job: MigrationJob, now: datetime | None = None) -> int:
    if job.rate_limited_until is None:
        return 0
    delta = (job.rate_limited_until - (now or utcnow())).total_seconds()
    return max(0, int(delta) + (1 if delta % 1 else 0)) if delta > 0 else 0


def percent_done(job: MigrationJob) -> float:
    if job.total_members <= 0:
        return 100.0 if job.status is JobStatus.COMPLETED else 0.0
    return round(min(100.0, job.processed_members * 100.0 / job.total_members), 1)


def worker_is_active(job: MigrationJob, lease_seconds: int, now: datetime | None = None) -> bool:
    if job.worker_id is None or job.heartbeat_at is None:
        return False
    return (now or utcnow()) - job.heartbeat_at < timedelta(seconds=lease_seconds)


async def retryable_member_count(db: AsyncSession, job_id: uuid.UUID) -> int:
    return (await db.execute(select(func.count()).select_from(MigrationMember).where(
        MigrationMember.job_id == job_id, MigrationMember.selected.is_(True),
        MigrationMember.state.in_(RETRYABLE_STATES)))).scalar_one()


def allowed_actions(job: MigrationJob, *, now: datetime, retryable: int) -> list[str]:
    s, actions = job.status, []
    if s is JobStatus.PENDING:
        if job.analysis_state not in (AnalysisState.ANALYZING,):
            actions.append("analyze")
        if job.analysis_state in (AnalysisState.COMPLETED, AnalysisState.PARTIAL) and job.total_members > 0:
            actions.append("start")
    if s is JobStatus.RUNNING and not job.pause_requested and not job.cancellation_requested:
        actions.append("pause")
    if (s is JobStatus.PAUSED and cooldown_seconds_left(job, now) == 0) or (s is JobStatus.RUNNING
                                                                          and job.pause_requested):
        actions.append("resume")
    if s in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PAUSED) and not job.cancellation_requested:
        actions.append("cancel")
    if s in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED) and retryable > 0:
        actions.append("retry")
    if s not in (JobStatus.RUNNING, JobStatus.CANCELLING):
        actions.append("delete")
    return actions


async def outcome_counts(db: AsyncSession, job_id: uuid.UUID) -> dict[str, int]:
    rows = (await db.execute(select(MigrationMember.outcome, func.count()).where(
        MigrationMember.job_id == job_id, MigrationMember.outcome.is_not(None))
        .group_by(MigrationMember.outcome))).all()
    return {o.value: n for o, n in rows}


async def recent_events(db: AsyncSession, job_id: uuid.UUID, limit: int) -> list[EventOut]:
    if limit <= 0:
        return []
    rows = (await db.execute(select(MigrationEvent).where(MigrationEvent.job_id == job_id)
                             .order_by(MigrationEvent.seq.desc()).limit(limit))).scalars().all()
    return [EventOut.model_validate(r) for r in reversed(rows)]


async def build_job_out(db: AsyncSession, job: MigrationJob, settings: Settings, *, events: int = 10) -> JobOut:
    now = utcnow()
    counts = await job_counts(db, job.id)
    retryable = await retryable_member_count(db, job.id) if job.status in (
        JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED) else 0
    notes = list(CAVEATS)
    if job.analysis_state is AnalysisState.PARTIAL:
        notes.append("Results are PARTIAL: at least one member list was truncated or rate-limited.")
    if job.analysis_note:
        notes.append(job.analysis_note)
    left = cooldown_seconds_left(job, now)
    if job.status is JobStatus.PAUSED and job.pause_reason is PauseReason.RATE_LIMIT and left:
        notes.append("Telegram requested a cooldown. The migration resumes automatically when it ends.")
    if job.status is JobStatus.FAILED and job.error_message:
        notes.append(f"Stopped: {job.error_message}")
    return JobOut(
        id=job.id, status=job.status, pause_reason=job.pause_reason, pause_requested=job.pause_requested,
        cancellation_requested=job.cancellation_requested, retry_of_id=job.retry_of_id,
        source=ChannelRefOut(id=job.source_channel_id, title=job.source_title),
        destination=ChannelRefOut(id=job.destination_channel_id, title=job.destination_title),
        created_at=job.created_at, started_at=job.started_at, completed_at=job.completed_at,
        total_members=job.total_members, processed_members=job.processed_members,
        successful_count=job.successful_count, skipped_count=job.skipped_count, failed_count=job.failed_count,
        rate_limited_count=job.rate_limited_count,
        remaining_members=max(0, job.total_members - job.processed_members), percent_complete=percent_done(job),
        current_member=(CurrentMember(id=job.current_member_id, label=job.current_member_label)
                        if job.current_member_id else None),
        error_message=job.error_message, error_code=job.error_code,
        rate_limited_until=job.rate_limited_until, rate_limit_remaining_seconds=left,
        worker_active=worker_is_active(job, settings.migration_lease_seconds, now),
        allowed_actions=allowed_actions(job, now=now, retryable=retryable),
        outcome_counts=await outcome_counts(db, job.id),
        analysis_state=job.analysis_state, analysis_note=job.analysis_note, member_limit=job.member_limit,
        source_reported_total=job.source_reported_total, source_listing_complete=job.source_listing_complete,
        destination_listing_complete=job.destination_listing_complete,
        destination_banned_known=job.destination_banned_known, analysis_finished_at=job.analysis_finished_at,
        summary=JobSummary(total_analyzed=sum(counts.values()), by_eligibility=counts), notes=notes,
        recent_events=await recent_events(db, job.id, events))


def to_list_item(job: MigrationJob) -> JobListItem:
    return JobListItem(
        id=job.id, status=job.status, pause_reason=job.pause_reason, analysis_state=job.analysis_state,
        source_title=job.source_title, destination_title=job.destination_title, created_at=job.created_at,
        started_at=job.started_at, completed_at=job.completed_at, total_members=job.total_members,
        processed_members=job.processed_members, successful_count=job.successful_count,
        skipped_count=job.skipped_count, failed_count=job.failed_count, percent_complete=percent_done(job),
        rate_limited_until=job.rate_limited_until)
