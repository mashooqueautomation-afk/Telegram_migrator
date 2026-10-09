"""Eligibility analysis: discover source/destination members, classify, persist. No invitations are sent."""
from __future__ import annotations

import logging
import uuid
from collections import Counter

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import AppError, ForbiddenError, NotFoundError
from app.core.security import SessionVault
from app.db.base import utcnow
from app.domain import AnalysisState, ChannelKind, Eligibility
from app.models import MigrationJob, MigrationMember, TelegramAccount, TelegramChannel
from app.services.audit import record_audit
from app.services.events import append_event
from app.services.eligibility import DestinationSnapshot, classify, initial_state_for
from app.services.telegram_service import (account_guard, decrypt_session, ensure_account_usable, get_owned_channel,
                                           record_flood, refresh_channel_row)
from app.telegram.client import authorized_client, deadline
from app.telegram.errors import AdminPermissionRequired, FloodWait, TelegramError, TelegramSessionInvalid
from app.telegram.members import MemberListing, fetch_members
from app.telegram.rate_limit import cooldown_until

log = logging.getLogger(__name__)


async def create_eligibility_job(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory,
                                 user_id: uuid.UUID, account_id: uuid.UUID | None,
                                 source_channel_id: uuid.UUID, destination_channel_id: uuid.UUID,
                                 member_limit: int | None, ip: str | None = None) -> MigrationJob:
    if source_channel_id == destination_channel_id:
        raise AppError("Source and destination must be different channels.", code="same_channel")
    src = await get_owned_channel(db, user_id, source_channel_id)
    dst = await get_owned_channel(db, user_id, destination_channel_id)
    if src.account_id != dst.account_id or (account_id is not None and account_id != src.account_id):
        raise AppError("Both channels must belong to the same connected Telegram account.",
                       code="channel_account_mismatch")
    if src.telegram_id == dst.telegram_id:
        raise AppError("Source and destination must be different channels.", code="same_channel")

    account = await db.get(TelegramAccount, src.account_id)
    ensure_account_usable(account)
    session = decrypt_session(vault, account)
    limit = max(1, min(member_limit or settings.eligibility_default_limit, settings.eligibility_max_limit))

    job: MigrationJob | None = None
    op_timeout = settings.telegram_operation_timeout_seconds
    # Outer deadline has a margin so the inner (analysis) deadline always fires first and the job gets FAILED.
    async with account_guard(db, account, op_timeout + 30):
        async with authorized_client(factory, session) as client:
            # 1) Live authorisation check: rights may have changed since the channel was discovered.
            src_info = await refresh_channel_row(db, client, src, settings)
            dst_info = await refresh_channel_row(db, client, dst, settings)
            if not src_info.is_admin:
                await db.commit()
                raise ForbiddenError("You are not an administrator of the source channel/group.",
                                     code="source_not_admin")
            if not dst_info.can_invite_users:
                await db.commit()
                raise ForbiddenError("You lack the 'invite users' permission in the destination.",
                                     code="destination_cannot_invite")

            job = MigrationJob(user_id=user_id, account_id=account.id, source_channel_id=src.id,
                               destination_channel_id=dst.id, source_title=src_info.title,
                               destination_title=dst_info.title, member_limit=limit,
                               analysis_state=AnalysisState.ANALYZING)
            db.add(job)
            await db.flush()
            await append_event(db, job.id, "ANALYSIS_STARTED", "Eligibility analysis started.",
                               data={"member_limit": limit})
            await record_audit(db, action="eligibility.started", user_id=user_id, resource_type="migration_job",
                               resource_id=job.id, ip=ip)
            await db.commit()

            try:
                async with deadline(op_timeout):
                    await _run_analysis(db, client, job, account, src_info, dst_info, settings)
            except TelegramSessionInvalid:
                await _fail_job(db, job, "Telegram session became invalid during analysis.")
                raise
            except TelegramError as exc:
                await _fail_job(db, job, exc.message)
    await db.refresh(job)
    return job


async def _fail_job(db: AsyncSession, job: MigrationJob, note: str, *, rate_limited_until=None) -> None:
    job_id = job.id                               # read BEFORE the rollback expires the instance
    await db.rollback()
    job = await db.get(MigrationJob, job_id)      # fresh async load (merge() would lazy-load an expired instance)
    if job is None:                               # deleted meanwhile
        return
    if rate_limited_until is not None:
        job.rate_limited_until = rate_limited_until
    job.analysis_state = AnalysisState.FAILED
    job.analysis_note = note
    job.analysis_finished_at = utcnow()
    await append_event(db, job.id, "ANALYSIS_FAILED", note, status="FAILED")
    await db.commit()


async def _run_analysis(db: AsyncSession, client, job: MigrationJob, account: TelegramAccount, src_info, dst_info,
                        settings: Settings) -> None:
    notes: list[str] = []
    src_listing = await fetch_members(client, src_info.ref, limit=job.member_limit)
    flood = src_listing.rate_limited_for

    dst_listing = MemberListing()
    banned_listing: MemberListing | None = None
    if flood is None:  # never keep sending requests once Telegram asked us to slow down
        dst_listing = await fetch_members(client, dst_info.ref, limit=settings.destination_listing_limit)
        flood = dst_listing.rate_limited_for
    if flood is None and dst_info.kind is not ChannelKind.GROUP:
        try:
            banned_listing = await fetch_members(client, dst_info.ref, limit=settings.destination_listing_limit,
                                                 kind="banned")
        except AdminPermissionRequired:
            banned_listing = None  # needs the 'ban users' right; stays unknown
        if banned_listing is not None and banned_listing.rate_limited_for is not None:
            flood = banned_listing.rate_limited_for
            banned_listing = None

    if flood is not None:
        until = cooldown_until(flood)
        job.rate_limited_until = until
        record_flood(account, flood)
        msg = (f"Telegram requested a cooldown of {flood} seconds; analysis stopped early."
               if flood else "Telegram asked us to slow down; analysis stopped early.")
        notes.append(msg)
        await append_event(db, job.id, "DISCOVERY_RATE_LIMITED", msg, error_code="FLOOD_WAIT",
                           data={"retry_after": flood})

    for label, listing in (("source", src_listing), ("destination", dst_listing)):
        if flood is None and not listing.complete:
            msg = f"The {label} member list is incomplete ({listing.incomplete_reason})."
            notes.append(msg)
            await append_event(db, job.id, "DISCOVERY_TRUNCATED", msg,
                               data={"listing": label, "reason": listing.incomplete_reason,
                                     "reported_total": listing.reported_total, "returned": len(listing.members)})
    if banned_listing is None and dst_info.kind is not ChannelKind.GROUP:
        notes.append("The destination's banned list could not be read; banned users can't be pre-detected.")

    snapshot = DestinationSnapshot(
        member_ids=frozenset(m.telegram_user_id for m in dst_listing.members),
        banned_ids=frozenset(m.telegram_user_id for m in (banned_listing.members if banned_listing else [])),
        membership_complete=dst_listing.complete,
        banned_known=bool(banned_listing and banned_listing.complete),
    )

    counts: Counter[str] = Counter()
    for position, raw in enumerate(src_listing.members):
        result = classify(raw, snapshot)
        counts[result.eligibility.value] += 1
        db.add(MigrationMember(job_id=job.id, telegram_user_id=raw.telegram_user_id, access_hash=raw.access_hash,
                               username=raw.username, display_name=raw.display_name, role=raw.role,
                               position=position, selected=result.eligibility is Eligibility.ELIGIBLE,
                               eligibility=result.eligibility, state=initial_state_for(result.eligibility),
                               reason=result.reason))

    job.source_reported_total = src_listing.reported_total
    job.source_listing_complete = src_listing.complete
    job.destination_listing_complete = dst_listing.complete
    job.destination_banned_known = snapshot.banned_known
    partial = not (src_listing.complete and dst_listing.complete)
    job.analysis_state = AnalysisState.PARTIAL if partial else AnalysisState.COMPLETED
    job.analysis_note = " ".join(notes) or None
    job.analysis_finished_at = utcnow()
    job.total_members = counts[Eligibility.ELIGIBLE.value]      # selectable members; refined by the selection endpoint
    account.last_used_at = utcnow()
    await append_event(db, job.id, "ANALYSIS_COMPLETED", f"Analysis finished ({job.analysis_state.value}).",
                       status=job.analysis_state.value,
                       data={"members_analyzed": len(src_listing.members), "counts": dict(counts)})
    await db.commit()


# ------------------------------------------------------------------ reads
async def get_job(db: AsyncSession, user_id: uuid.UUID, job_id: uuid.UUID) -> MigrationJob:
    job = (await db.execute(select(MigrationJob).where(MigrationJob.id == job_id,
                                                       MigrationJob.user_id == user_id))).scalar_one_or_none()
    if job is None:
        raise NotFoundError("Migration job not found.", code="job_not_found")
    return job


async def job_counts(db: AsyncSession, job_id: uuid.UUID) -> dict[str, int]:
    rows = (await db.execute(select(MigrationMember.eligibility, func.count())
                             .where(MigrationMember.job_id == job_id)
                             .group_by(MigrationMember.eligibility))).all()
    counts = {e.value: 0 for e in Eligibility}
    for eligibility, n in rows:
        counts[eligibility.value] = n
    return counts


# ------------------------------------------------------------------ Stage 2: analysis of an existing job (worker)
async def analyze_existing_job(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory,
                               job_id: uuid.UUID) -> None:
    """Run the eligibility analysis for a job created via POST /migrations. Never raises for expected failures:
    problems are recorded on the job (analysis_state=FAILED + an ANALYSIS_FAILED event) so the UI can show them."""
    job = await db.get(MigrationJob, job_id)
    if job is None or job.analysis_state is not AnalysisState.ANALYZING:
        return                                   # deleted meanwhile, or not waiting for analysis (duplicate task)
    op_timeout = settings.telegram_operation_timeout_seconds
    try:
        account = await db.get(TelegramAccount, job.account_id) if job.account_id else None
        src = await db.get(TelegramChannel, job.source_channel_id) if job.source_channel_id else None
        dst = await db.get(TelegramChannel, job.destination_channel_id) if job.destination_channel_id else None
        if account is None or account.user_id != job.user_id or src is None or dst is None:
            await _fail_job(db, job, "The Telegram account or a channel of this migration no longer exists.")
            return
        await append_event(db, job.id, "ANALYSIS_STARTED", "Eligibility analysis started.",
                           data={"member_limit": job.member_limit})
        await db.commit()
        ensure_account_usable(account)           # FloodWait cooldown / needs-reauth: nothing is sent to Telegram
        session = decrypt_session(vault, account)
        async with account_guard(db, account, op_timeout + 30):
            async with authorized_client(factory, session) as client:
                src_info = await refresh_channel_row(db, client, src, settings)
                dst_info = await refresh_channel_row(db, client, dst, settings)
                if not src_info.is_admin:
                    await _fail_job(db, job, "You are not an administrator of the source channel/group.")
                    return
                if not dst_info.can_invite_users:
                    await _fail_job(db, job, "You lack the 'invite users' permission in the destination.")
                    return
                await db.execute(delete(MigrationMember).where(MigrationMember.job_id == job.id))
                async with deadline(op_timeout):
                    await _run_analysis(db, client, job, account, src_info, dst_info, settings)
    except (TelegramError, AppError) as exc:
        until = cooldown_until(exc.retry_after) if isinstance(exc, FloodWait) else None
        await _fail_job(db, job, getattr(exc, "message", str(exc)), rate_limited_until=until)
    except Exception:  # noqa: BLE001 - never leave a job stuck in ANALYZING
        log.exception("unexpected error while analysing migration %s", job_id)
        await _fail_job(db, job, "Internal error during analysis; see the server logs.")
