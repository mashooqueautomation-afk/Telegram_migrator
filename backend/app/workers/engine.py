"""The migration engine: processes ONE job's members, one safe Telegram operation at a time.

Guarantees
  * Idempotent / resumable   - a member is *claimed* with an atomic compare-and-set (state -> PROCESSING); finished members
                               (SUCCESS, ALREADY_MEMBER, SKIPPED, FAILED) are never claimed again. A crash leaves at most a
                               PROCESSING member, which recovery resets; `invite_attempted_at` tells us we may already have
                               invited them, so we *verify* instead of blindly re-inviting.
  * Telegram stays in charge - FloodWait pauses the whole job until the cooldown (persisted) has elapsed; refusals are
                               recorded and skipped; fatal conditions (admin rights lost, OUR account restricted, session
                               dead) stop the job and report. Nothing here rotates accounts/IPs or retries a refusal.
  * Never assumes success    - an invite that raises nothing is verified with a membership check before SUCCESS.
  * Cooperative control      - pause / cancel are database flags checked between member operations; the in-flight operation
                               always finishes and is recorded.
  * Predictable memory       - members are claimed one at a time by query; the job is never loaded into RAM.
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypeVar

from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.logging import log_event
from app.core.security import DecryptionError, SessionVault
from app.db.base import utcnow
from app.domain import (AccountStatus, Eligibility, InviteOutcome, JobStatus, MemberState, PauseReason)
from app.models import MigrationJob, MigrationMember, TelegramAccount, TelegramChannel
from app.services.audit import record_audit
from app.services.events import EventType, append_event
from app.services.job_state import cas_transition
from app.telegram.channels import ChannelRef, fetch_channel
from app.telegram.client import authorized_client, deadline
from app.telegram.errors import FloodWait, InviteError, classify_invite_exception
from app.telegram.invite import UserRef, invite_user, is_member
from app.workers.queue import RUN_MIGRATION, JobQueue, run_job_id

log = logging.getLogger(__name__)
T = TypeVar("T")

CLAIMABLE = (MemberState.ELIGIBLE, MemberState.RATE_LIMITED)
SLEEP_CHUNK = 10.0          # long sleeps are chopped so the heartbeat and stop flags stay fresh


# ------------------------------------------------------------------------------------------------ data
@dataclass(frozen=True)
class Claimed:
    id: uuid.UUID
    telegram_user_id: int
    access_hash: int | None
    label: str
    invite_attempted_at: datetime | None


@dataclass(frozen=True)
class Resolution:
    outcome: InviteOutcome
    state: MemberState
    eligibility: Eligibility | None = None
    error_code: str | None = None
    reason: str | None = None
    attempts: int = 1


_OUTCOME_STATE: dict[InviteOutcome, tuple[MemberState, Eligibility | None]] = {
    InviteOutcome.SUCCESS: (MemberState.SUCCESS, None),
    InviteOutcome.ALREADY_MEMBER: (MemberState.ALREADY_MEMBER, Eligibility.ALREADY_IN_DESTINATION),
    InviteOutcome.PRIVACY_RESTRICTED: (MemberState.SKIPPED, Eligibility.PRIVACY_RESTRICTED),
    InviteOutcome.INVALID_USER: (MemberState.SKIPPED, None),
    InviteOutcome.USER_RESTRICTED: (MemberState.SKIPPED, Eligibility.RESTRICTED),
    InviteOutcome.TEMPORARY_ERROR: (MemberState.FAILED, None),
    InviteOutcome.NOT_CONFIRMED: (MemberState.FAILED, None),
    InviteOutcome.UNKNOWN_ERROR: (MemberState.FAILED, None),
}


def resolution_for(outcome: InviteOutcome, *, error_code: str | None = None, reason: str | None = None,
                   attempts: int = 1) -> Resolution:
    state, eligibility = _OUTCOME_STATE[outcome]
    return Resolution(outcome, state, eligibility, error_code, reason, attempts)


@dataclass
class _Pending:
    kind: str                       # fatal | cancel | rate_limit | pause | errors
    until: datetime | None = None
    error: InviteError | None = None
    message: str | None = None


_PRIORITY = {"fatal": 0, "cancel": 1, "rate_limit": 2, "errors": 3, "pause": 4}


@dataclass
class RunContext:
    job_id: uuid.UUID
    client: object
    dest: ChannelRef
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    pending: _Pending | None = None
    lost_lease: bool = False
    consecutive_failures: int = 0
    next_slot: float = 0.0
    gate: asyncio.Lock = field(default_factory=asyncio.Lock)

    def request_stop(self, pending: _Pending | None = None) -> None:
        if pending and (self.pending is None or _PRIORITY[pending.kind] < _PRIORITY[self.pending.kind]):
            self.pending = pending
        elif pending and pending.kind == "rate_limit" and self.pending and self.pending.kind == "rate_limit":
            if pending.until and (self.pending.until is None or pending.until > self.pending.until):
                self.pending = pending           # several in-flight operations flooded: honour the longest cooldown
        self.stop.set()


class _StopRun(Exception):
    """Raised inside a member operation to unwind after the run-level stop was requested."""


# ------------------------------------------------------------------------------------------------ engine
class MigrationEngine:
    def __init__(self, *, sessionmaker: async_sessionmaker[AsyncSession], settings: Settings, vault: SessionVault,
                 factory, queue: JobQueue, worker_id: str | None = None,
                 clock: Callable[[], datetime] = utcnow,
                 sleep: Callable[[float], Awaitable[None]] | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.sm, self.settings, self.vault, self.factory, self.queue = sessionmaker, settings, vault, factory, queue
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self._clock = clock
        self._sleep_fn = sleep
        self._monotonic = monotonic
        self._active: set[uuid.UUID] = set()

    async def _sleep(self, seconds: float) -> None:
        await (self._sleep_fn or asyncio.sleep)(seconds)

    # ============================================================================ public entry point
    async def run(self, job_id: uuid.UUID) -> str:
        """Process the job until it completes, pauses, is cancelled, fails or loses its lease. Returns why it returned."""
        if job_id in self._active:
            return "already_running_here"
        self._active.add(job_id)
        try:
            return await self._run(job_id)
        except asyncio.CancelledError:                # e.g. worker shutdown: leave a consistent state, then propagate
            await asyncio.shield(self._release(job_id, reset_processing=True))
            raise
        finally:
            self._active.discard(job_id)
            await asyncio.shield(self._release(job_id))

    # ============================================================================ lifecycle
    async def _run(self, job_id: uuid.UUID) -> str:
        async with self.sm() as db:
            job = await db.get(MigrationJob, job_id)
            if job is None:
                return "missing"
            if not await self._acquire_lease(db, job):
                log_event(log, "lease_held", migration_id=job_id)
                return "lease_held"
            reset = await self._reset_processing(db, job_id)
            await db.commit()
            await db.refresh(job)
            if reset:
                await append_event(db, job_id, EventType.RECOVERED,
                                   f"Recovered {reset} member(s) left in-flight by an interrupted worker.",
                                   data={"reset_members": reset})
                await record_audit(db, action="MIGRATION_RECOVERED", user_id=job.user_id, resource_type="migration_job",
                                   resource_id=job_id, details={"reset_members": reset})
                await db.commit()
            now = self._clock()

            if job.status is JobStatus.PAUSED:
                if job.pause_reason is PauseReason.RATE_LIMIT and not job.pause_requested:
                    if job.rate_limited_until and job.rate_limited_until > now:
                        await self.queue.enqueue(
                            RUN_MIGRATION, str(job_id), defer_until=job.rate_limited_until,
                            job_id=run_job_id(job_id, int(job.rate_limited_until.timestamp())))
                        return "cooldown_pending"
                    resumed = await cas_transition(db, job_id, expected=JobStatus.PAUSED, to=JobStatus.RUNNING,
                                                   now=now, pause_reason=None, pause_requested=False,
                                                   rate_limited_until=None)
                    if resumed:
                        await append_event(db, job_id, EventType.RESUMED, "Cooldown elapsed; migration resumed.",
                                           status="RUNNING", data={"automatic": True})
                        await record_audit(db, action="MIGRATION_RESUMED", user_id=job.user_id,
                                           resource_type="migration_job", resource_id=job_id,
                                           details={"automatic": True})
                        await db.commit()
                        await db.refresh(job)
                    else:
                        return "not_runnable"
                else:
                    return "paused"
            if job.status is JobStatus.CANCELLING or job.cancellation_requested:
                return await self._apply_pending(job_id, _Pending("cancel"))
            if job.status is not JobStatus.RUNNING:
                return "not_runnable"

            account = await db.get(TelegramAccount, job.account_id) if job.account_id else None
            dest_row = await db.get(TelegramChannel, job.destination_channel_id) if job.destination_channel_id else None
            user_id = job.user_id

        # ---- sanity: ownership and credentials (checked server-side on every run) ------------------------------
        if account is None or account.user_id != user_id or dest_row is None:
            return await self._apply_pending(job_id, _Pending("fatal", error=_fatal(
                InviteOutcome.CHANNEL_RESTRICTED, "ACCOUNT_OR_CHANNEL_MISSING",
                "The Telegram account or the destination of this migration no longer exists.")))
        if account.status is AccountStatus.NEEDS_REAUTH:
            return await self._apply_pending(job_id, _Pending("fatal", error=_fatal(
                InviteOutcome.SESSION_INVALID, "SESSION_INVALID",
                "The Telegram session is no longer valid. Reconnect the account, then retry.")))
        try:
            session = self.vault.decrypt(account.session_encrypted)
        except DecryptionError:
            return await self._apply_pending(job_id, _Pending("fatal", error=_fatal(
                InviteOutcome.SESSION_INVALID, "SESSION_UNDECRYPTABLE",
                "The stored Telegram session cannot be decrypted with the configured keys.")))

        # ---- connect, verify our rights live, then process ------------------------------------------------------
        try:
            async with authorized_client(self.factory, session) as client:
                async with deadline(self.settings.telegram_operation_timeout_seconds):
                    info = await fetch_channel(client, ChannelRef(dest_row.kind, dest_row.telegram_id,
                                                                  dest_row.access_hash))
                if not info.can_invite_users:
                    return await self._apply_pending(job_id, _Pending("fatal", error=_fatal(
                        InviteOutcome.ADMIN_REQUIRED, "CHAT_ADMIN_REQUIRED",
                        "The connected account can no longer invite users in the destination.")))
                ctx = RunContext(job_id=job_id, client=client, dest=info.ref)
                return await self._process(ctx)
        except FloodWait as fw:
            return await self._apply_pending(job_id, self._flood_pending(fw.retry_after, source="connect"))
        except Exception as exc:  # noqa: BLE001
            classified = classify_invite_exception(exc)
            if isinstance(classified, FloodWait):
                return await self._apply_pending(job_id, self._flood_pending(classified.retry_after, source="connect"))
            if isinstance(classified, InviteError) and classified.fatal:
                return await self._apply_pending(job_id, _Pending("fatal", error=classified))
            if isinstance(classified, InviteError):      # transient trouble reaching Telegram: stop and let a human decide
                log_event(log, "connect_failed", level=logging.WARNING, migration_id=job_id,
                          error_code=classified.error_code)
                return await self._apply_pending(job_id, _Pending(
                    "errors", message="Could not reach Telegram. Resume when connectivity is back."))
            raise

    # ============================================================================ main loop
    async def _process(self, ctx: RunContext) -> str:
        workers = [asyncio.create_task(self._worker(ctx)) for _ in range(self.settings.migration_concurrency)]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            ctx.stop.set()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        if ctx.lost_lease:
            return "lost_lease"
        if ctx.pending is not None:
            return await self._apply_pending(ctx.job_id, ctx.pending)
        async with self.sm() as db:
            remaining = await self._count_open(db, ctx.job_id)
        if remaining == 0:
            return await self._complete(ctx.job_id)
        return "incomplete"       # should not happen; the sweeper will re-enqueue the still-RUNNING job

    async def _worker(self, ctx: RunContext) -> None:
        while not ctx.stop.is_set():
            if not await self._checkpoint(ctx):
                return
            member = await self._claim_next(ctx)
            if member is None:
                return
            await self._handle_member(ctx, member)

    # ============================================================================ checkpoint (flags, cooldown, lease)
    async def _checkpoint(self, ctx: RunContext) -> bool:
        """Between member operations: heartbeat, then honour cancel/pause flags and the account cooldown."""
        now = self._clock()
        async with self.sm() as db:
            beat = await db.execute(update(MigrationJob).where(MigrationJob.id == ctx.job_id,
                                                               MigrationJob.worker_id == self.worker_id)
                                    .values(heartbeat_at=now))
            if beat.rowcount != 1:
                ctx.lost_lease = True
                ctx.stop.set()
                return False
            await db.commit()
            row = (await db.execute(select(MigrationJob.status, MigrationJob.pause_requested,
                                           MigrationJob.cancellation_requested, MigrationJob.account_id)
                                    .where(MigrationJob.id == ctx.job_id))).one_or_none()
            if row is None:
                ctx.stop.set()
                return False
            status, pause_requested, cancel_requested, account_id = row
            account = await db.get(TelegramAccount, account_id) if account_id else None
        if cancel_requested or status is JobStatus.CANCELLING:
            ctx.request_stop(_Pending("cancel"))
            return False
        if status is not JobStatus.RUNNING:
            ctx.stop.set()                       # someone (API) already moved the job; just stop quietly
            return False
        if pause_requested:
            ctx.request_stop(_Pending("pause"))
            return False
        if account is not None and account.flood_wait_until and account.flood_wait_until > now:
            ctx.request_stop(self._flood_pending(None, source="account_cooldown", until=account.flood_wait_until))
            return False
        return True

    # ============================================================================ claiming (the idempotency core)
    async def _claim_next(self, ctx: RunContext) -> Claimed | None:
        async def op(db: AsyncSession) -> Claimed | None:
            for _ in range(5):                    # a lost CAS race just means another coroutine took that member
                candidate = (await db.execute(
                    select(MigrationMember.id, MigrationMember.telegram_user_id, MigrationMember.access_hash,
                           MigrationMember.display_name, MigrationMember.username,
                           MigrationMember.invite_attempted_at)
                    .where(MigrationMember.job_id == ctx.job_id, MigrationMember.selected.is_(True),
                           MigrationMember.state.in_(CLAIMABLE))
                    .order_by(MigrationMember.position, MigrationMember.id).limit(1)
                    .with_for_update(skip_locked=True))).first()
                if candidate is None:
                    return None
                mid, uid, access_hash, name, username, invited_at = candidate
                now = self._clock()
                won = await db.execute(update(MigrationMember).where(
                    MigrationMember.id == mid, MigrationMember.state.in_(CLAIMABLE))
                    .values(state=MemberState.PROCESSING, last_attempt_at=now, updated_at=now,
                            attempt_count=MigrationMember.attempt_count + 1))
                if won.rowcount != 1:
                    await db.rollback()
                    continue
                label = f"@{username}" if username else name
                await db.execute(update(MigrationJob).where(MigrationJob.id == ctx.job_id)
                                 .values(current_member_id=mid, current_member_label=label[:255]))
                await db.commit()
                log_event(log, "member_claimed", migration_id=ctx.job_id, member_id=mid, status="PROCESSING")
                return Claimed(mid, uid, access_hash, label, invited_at)
            return None
        return await self._with_db(op)

    # ============================================================================ one member
    async def _handle_member(self, ctx: RunContext, member: Claimed) -> None:
        try:
            resolution = await self._attempt_with_retries(ctx, member)
        except _StopRun:
            return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a bug on one member must not kill the whole migration
            log.exception("unexpected error while processing member %s", member.id)
            resolution = resolution_for(InviteOutcome.UNKNOWN_ERROR, error_code="INTERNAL_ERROR",
                                        reason="Unexpected internal error; the member was not invited.")
        await self._record(ctx, member, resolution)
        if resolution.state is MemberState.FAILED:
            ctx.consecutive_failures += 1
            if ctx.consecutive_failures >= self.settings.migration_max_consecutive_failures:
                ctx.request_stop(_Pending("errors", message=(
                    f"{ctx.consecutive_failures} members in a row failed; pausing so you can check Telegram/network.")))
        else:
            ctx.consecutive_failures = 0

    async def _attempt_with_retries(self, ctx: RunContext, member: Claimed) -> Resolution:
        retries = 0
        while True:
            try:
                return await self._attempt(ctx, member)
            except FloodWait as fw:
                await self._on_flood(ctx, member, fw)
                raise _StopRun from fw
            except InviteError as err:
                if err.fatal:
                    await self._on_fatal(ctx, member, err)
                    raise _StopRun from err
                if err.retryable and retries < self.settings.migration_max_retries:
                    delay = min(self.settings.migration_retry_base_seconds * (2 ** retries),
                                self.settings.migration_retry_max_seconds)
                    retries += 1
                    log_event(log, "member_retry", level=logging.WARNING, migration_id=ctx.job_id, member_id=member.id,
                              status="RETRYING", error_code=err.error_code, attempt=retries + 1, backoff_seconds=delay)
                    await self._bump_attempt(member.id)
                    await self._sleep_checked(ctx, delay)
                    if ctx.stop.is_set():          # asked to stop while backing off: hand the member back untouched
                        await self._unclaim(member.id, note="Stopped while waiting to retry a temporary error.")
                        raise _StopRun from err
                    continue
                outcome = err.outcome
                if outcome not in _OUTCOME_STATE:  # defensive: never leave a member in limbo
                    outcome = InviteOutcome.UNKNOWN_ERROR
                return resolution_for(outcome, error_code=err.error_code, reason=err.message, attempts=retries + 1)

    async def _attempt(self, ctx: RunContext, member: Claimed) -> Resolution:
        timeout = self.settings.telegram_operation_timeout_seconds
        await self._guard_account_cooldown(ctx)
        await self._throttle(ctx)
        user = UserRef(member.telegram_user_id, member.access_hash)
        try:
            async with deadline(timeout):
                already = await is_member(ctx.client, ctx.dest, user)
            if already:
                if member.invite_attempted_at is not None:    # we (probably) invited them before a crash/pause
                    return resolution_for(InviteOutcome.SUCCESS,
                                          reason="Telegram lists the user as a member after our earlier invitation.")
                return resolution_for(InviteOutcome.ALREADY_MEMBER,
                                      error_code="USER_ALREADY_PARTICIPANT",
                                      reason="The user is already a member of the destination.")

            await self._mark_invite_attempted(member.id)    # persisted BEFORE asking Telegram (crash safety)
            async with deadline(timeout):
                await invite_user(ctx.client, ctx.dest, user)

            for i in range(3):                              # confirm: an API call that raised nothing is not proof
                async with deadline(timeout):
                    confirmed = await is_member(ctx.client, ctx.dest, user)
                if confirmed:
                    return resolution_for(InviteOutcome.SUCCESS, reason="Membership confirmed by Telegram.")
                if i < 2:
                    await self._sleep_checked(ctx, float(i + 1))   # tiny propagation wait, not a retry of the invite
            return resolution_for(InviteOutcome.NOT_CONFIRMED, error_code="NOT_CONFIRMED",
                                  reason="Telegram accepted the request but does not list the user as a member.")
        except (FloodWait, InviteError):
            raise
        except Exception as exc:  # noqa: BLE001
            classified = classify_invite_exception(exc)
            if classified is None:
                raise
            raise classified from exc

    # ============================================================================ throttling / sleeping
    async def _guard_account_cooldown(self, ctx: RunContext) -> None:
        async with self.sm() as db:
            job = await db.get(MigrationJob, ctx.job_id)
            account = await db.get(TelegramAccount, job.account_id) if job and job.account_id else None
        if account is not None and account.flood_wait_until and account.flood_wait_until > self._clock():
            remaining = int((account.flood_wait_until - self._clock()).total_seconds()) + 1
            raise FloodWait(remaining)

    async def _throttle(self, ctx: RunContext) -> None:
        """Application-level pacing: min gap between the starts of Telegram operations (shared by all coroutines)."""
        delay = self.settings.migration_min_delay_seconds
        if delay <= 0:
            return
        async with ctx.gate:
            wait = ctx.next_slot - self._monotonic()
            if wait > 0:
                await self._sleep_checked(ctx, wait)
            ctx.next_slot = self._monotonic() + delay

    async def _sleep_checked(self, ctx: RunContext, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not ctx.stop.is_set():
            chunk = min(remaining, SLEEP_CHUNK)
            await self._sleep(chunk)
            remaining -= chunk
            if remaining > 0:
                await self._touch_heartbeat(ctx)

    async def _touch_heartbeat(self, ctx: RunContext) -> None:
        async with self.sm() as db:
            res = await db.execute(update(MigrationJob).where(MigrationJob.id == ctx.job_id,
                                                              MigrationJob.worker_id == self.worker_id)
                                   .values(heartbeat_at=self._clock()))
            await db.commit()
            if res.rowcount != 1:
                ctx.lost_lease = True
                ctx.stop.set()

    # ============================================================================ recording results
    async def _bump_attempt(self, member_id: uuid.UUID) -> None:
        async def op(db: AsyncSession) -> None:
            await db.execute(update(MigrationMember).where(MigrationMember.id == member_id,
                                                           MigrationMember.state == MemberState.PROCESSING)
                             .values(attempt_count=MigrationMember.attempt_count + 1,
                                     last_attempt_at=self._clock()))
            await db.commit()
        await self._with_db(op)

    async def _mark_invite_attempted(self, member_id: uuid.UUID) -> None:
        async def op(db: AsyncSession) -> None:
            await db.execute(update(MigrationMember).where(MigrationMember.id == member_id)
                             .values(invite_attempted_at=self._clock()))
            await db.commit()
        await self._with_db(op)

    async def _unclaim(self, member_id: uuid.UUID, *, note: str | None = None) -> None:
        """Hand a PROCESSING member back (e.g. we were asked to stop while backing off); it stays claimable."""
        async def op(db: AsyncSession) -> None:
            await db.execute(update(MigrationMember).where(MigrationMember.id == member_id,
                                                           MigrationMember.state == MemberState.PROCESSING)
                             .values(state=MemberState.ELIGIBLE, failure_reason=note))
            await db.execute(update(MigrationJob).where(MigrationJob.current_member_id == member_id)
                             .values(current_member_id=None, current_member_label=None))
            await db.commit()
        await self._with_db(op)

    async def _record(self, ctx: RunContext, member: Claimed, res: Resolution) -> None:
        """Member result + job counters + event in ONE transaction (compare-and-set on PROCESSING => no double counting)."""
        async def op(db: AsyncSession) -> bool:
            now = self._clock()
            values: dict = dict(state=res.state, outcome=res.outcome, processed_at=now, updated_at=now,
                                failure_reason=None if res.state in (MemberState.SUCCESS, MemberState.ALREADY_MEMBER)
                                else res.reason,
                                telegram_error_code=res.error_code)
            if res.eligibility is not None:
                values["eligibility"] = res.eligibility
            won = await db.execute(update(MigrationMember).where(MigrationMember.id == member.id,
                                                                 MigrationMember.state == MemberState.PROCESSING)
                                   .values(**values))
            if won.rowcount != 1:                 # recovery/cancellation already dealt with this member
                await db.rollback()
                return False
            deltas = {"processed_members": MigrationJob.processed_members + 1, "current_member_id": None,
                      "current_member_label": None}
            if res.state is MemberState.SUCCESS:
                deltas["successful_count"] = MigrationJob.successful_count + 1
            elif res.state is MemberState.FAILED:
                deltas["failed_count"] = MigrationJob.failed_count + 1
            else:
                deltas["skipped_count"] = MigrationJob.skipped_count + 1
            await db.execute(update(MigrationJob).where(MigrationJob.id == ctx.job_id).values(**deltas))
            if res.state is MemberState.SUCCESS:
                etype, msg = EventType.MEMBER_SUCCESS, f"{member.label} was added to the destination."
            elif res.state is MemberState.FAILED:
                etype, msg = EventType.MEMBER_FAILED, f"{member.label} could not be added."
            else:
                etype, msg = EventType.MEMBER_SKIPPED, f"{member.label} was skipped."
            await append_event(db, ctx.job_id, etype, msg, member_id=member.id, status=res.state.value,
                               reason=res.reason, error_code=res.error_code,
                               data={"outcome": res.outcome.value, "attempts": res.attempts,
                                     "telegram_user_id": member.telegram_user_id})
            await db.commit()
            return True
        recorded = await self._with_db(op)
        log_event(log, "member_result", migration_id=ctx.job_id, member_id=member.id, status=res.state.value,
                  error_code=res.error_code, outcome=res.outcome.value, recorded=recorded)

    # ============================================================================ flood / fatal / run-level outcomes
    def _flood_pending(self, retry_after: int | None, *, source: str, until: datetime | None = None) -> _Pending:
        if until is None:
            seconds = retry_after if retry_after is not None else self.settings.default_flood_wait_seconds
            until = self._clock() + timedelta(seconds=seconds + self.settings.flood_wait_margin_seconds)
        return _Pending("rate_limit", until=until, message=source)

    async def _on_flood(self, ctx: RunContext, member: Claimed, fw: FloodWait) -> None:
        pending = self._flood_pending(fw.retry_after, source="telegram")
        retry_after = fw.retry_after if fw.retry_after is not None else self.settings.default_flood_wait_seconds

        async def op(db: AsyncSession) -> None:
            now = self._clock()
            await db.execute(update(MigrationMember).where(MigrationMember.id == member.id,
                                                           MigrationMember.state == MemberState.PROCESSING)
                             .values(state=MemberState.RATE_LIMITED, telegram_error_code="FLOOD_WAIT",
                                     failure_reason=f"Telegram requested a cooldown of {retry_after} seconds.",
                                     updated_at=now))
            job = await db.get(MigrationJob, ctx.job_id)
            account = await db.get(TelegramAccount, job.account_id) if job.account_id else None
            if account is not None and (account.flood_wait_until is None or account.flood_wait_until < pending.until):
                account.flood_wait_until = pending.until
            await db.execute(update(MigrationJob).where(MigrationJob.id == ctx.job_id).values(
                rate_limited_count=MigrationJob.rate_limited_count + 1, rate_limited_until=pending.until,
                current_member_id=None, current_member_label=None))
            await append_event(db, ctx.job_id, EventType.RATE_LIMITED,
                               f"Telegram requested a cooldown of {retry_after} seconds.", member_id=member.id,
                               status=MemberState.RATE_LIMITED.value, error_code="FLOOD_WAIT",
                               data={"retry_after": retry_after, "resume_after": pending.until.isoformat()})
            await db.commit()
        await self._with_db(op)
        log_event(log, "flood_wait", level=logging.WARNING, migration_id=ctx.job_id, member_id=member.id,
                  status="RATE_LIMITED", error_code="FLOOD_WAIT", retry_after=retry_after)
        ctx.request_stop(pending)

    async def _on_fatal(self, ctx: RunContext, member: Claimed, err: InviteError) -> None:
        async def op(db: AsyncSession) -> None:
            await db.execute(update(MigrationMember).where(MigrationMember.id == member.id,
                                                           MigrationMember.state == MemberState.PROCESSING)
                             .values(state=MemberState.ELIGIBLE, telegram_error_code=err.error_code,
                                     failure_reason=err.message))
            await db.commit()
        await self._with_db(op)
        log_event(log, "fatal_error", level=logging.ERROR, migration_id=ctx.job_id, member_id=member.id,
                  error_code=err.error_code, outcome=err.outcome.value)
        ctx.request_stop(_Pending("fatal", error=err))

    async def _apply_pending(self, job_id: uuid.UUID, pending: _Pending) -> str:
        """Apply the run-level outcome AFTER all in-flight member operations have finished."""
        now = self._clock()
        async with self.sm() as db:
            job = await db.get(MigrationJob, job_id)
            if job is None:
                return "missing"
            uid = job.user_id
            if pending.kind == "cancel":
                await self._reset_processing(db, job_id)
                ok = await cas_transition(db, job_id, expected=(JobStatus.CANCELLING, JobStatus.RUNNING,
                                                                JobStatus.PAUSED), to=JobStatus.CANCELLED, now=now,
                                          pause_requested=False, cancellation_requested=False)
                if ok:
                    await append_event(db, job_id, EventType.CANCELLED, "Migration cancelled; previous results kept.",
                                       status="CANCELLED", data=await self._counters(db, job_id))
                    await record_audit(db, action="MIGRATION_CANCELLED", user_id=uid, resource_type="migration_job",
                                       resource_id=job_id)
                await db.commit()
                return "cancelled"
            if pending.kind == "fatal":
                err = pending.error
                ok = await cas_transition(db, job_id, expected=JobStatus.RUNNING, to=JobStatus.FAILED, now=now,
                                          error_message=err.message, error_code=err.error_code)
                if ok:
                    if err.outcome is InviteOutcome.SESSION_INVALID and job.account_id:
                        await db.execute(update(TelegramAccount).where(TelegramAccount.id == job.account_id)
                                         .values(status=AccountStatus.NEEDS_REAUTH))
                    await append_event(db, job_id, EventType.FAILED, err.message, status="FAILED",
                                       error_code=err.error_code, data={"outcome": err.outcome.value})
                    await record_audit(db, action="MIGRATION_FAILED", user_id=uid, resource_type="migration_job",
                                       resource_id=job_id, details={"error_code": err.error_code,
                                                                    "outcome": err.outcome.value})
                await db.commit()
                log_event(log, "job_failed", level=logging.ERROR, migration_id=job_id, error_code=err.error_code)
                return "failed"
            if pending.kind == "rate_limit":
                ok = await cas_transition(db, job_id, expected=JobStatus.RUNNING, to=JobStatus.PAUSED, now=now,
                                          pause_reason=PauseReason.RATE_LIMIT, rate_limited_until=pending.until,
                                          pause_requested=False, current_member_id=None, current_member_label=None)
                if ok:
                    left = max(0, int((pending.until - now).total_seconds()))
                    await append_event(db, job_id, EventType.PAUSED,
                                       "Paused: Telegram requested a cooldown. Resumes automatically afterwards.",
                                       status="PAUSED", reason="RATE_LIMIT",
                                       data={"pause_reason": "RATE_LIMIT", "resume_after": pending.until.isoformat(),
                                             "seconds": left, "source": pending.message})
                    await record_audit(db, action="MIGRATION_RATE_LIMITED", user_id=uid,
                                       resource_type="migration_job", resource_id=job_id,
                                       details={"resume_after": pending.until.isoformat(), "source": pending.message})
                await db.commit()
                if ok:
                    try:                  # best effort; the sweeper is the safety net
                        await self.queue.enqueue(RUN_MIGRATION, str(job_id), defer_until=pending.until,
                                                 job_id=run_job_id(job_id, int(pending.until.timestamp())))
                    except Exception:     # noqa: BLE001
                        log.warning("could not schedule the automatic resume for %s; the sweeper will", job_id)
                return "rate_limited"
            # "pause" (administrator) or "errors" (circuit breaker)
            reason = PauseReason.USER if pending.kind == "pause" else PauseReason.ERRORS
            ok = await cas_transition(db, job_id, expected=JobStatus.RUNNING, to=JobStatus.PAUSED, now=now,
                                      pause_reason=reason, pause_requested=False, current_member_id=None,
                                      current_member_label=None)
            if ok:
                msg = "Paused at your request." if reason is PauseReason.USER else (pending.message or "Paused.")
                await append_event(db, job_id, EventType.PAUSED if reason is PauseReason.USER
                                   else EventType.ERRORS_PAUSE, msg, status="PAUSED", reason=reason.value,
                                   data={"pause_reason": reason.value})
                await record_audit(db, action="MIGRATION_PAUSED", user_id=uid, resource_type="migration_job",
                                   resource_id=job_id, details={"pause_reason": reason.value})
            await db.commit()
            return "paused"

    async def _complete(self, job_id: uuid.UUID) -> str:
        async with self.sm() as db:
            job = await db.get(MigrationJob, job_id)
            ok = await cas_transition(db, job_id, expected=JobStatus.RUNNING, to=JobStatus.COMPLETED,
                                      now=self._clock(), pause_reason=None, rate_limited_until=None)
            if ok:
                counters = await self._counters(db, job_id)
                await append_event(db, job_id, EventType.COMPLETED, "Migration finished.", status="COMPLETED",
                                   data=counters)
                await record_audit(db, action="MIGRATION_COMPLETED", user_id=job.user_id,
                                   resource_type="migration_job", resource_id=job_id, details=counters)
            await db.commit()
        log_event(log, "job_completed", migration_id=job_id, status="COMPLETED")
        return "completed" if ok else "not_runnable"

    # ============================================================================ helpers
    async def _counters(self, db: AsyncSession, job_id: uuid.UUID) -> dict:
        job = await db.get(MigrationJob, job_id)
        await db.refresh(job)
        return {"processed": job.processed_members, "total": job.total_members, "successful": job.successful_count,
                "skipped": job.skipped_count, "failed": job.failed_count, "rate_limited": job.rate_limited_count}

    async def _count_open(self, db: AsyncSession, job_id: uuid.UUID) -> int:
        return (await db.execute(select(func.count()).select_from(MigrationMember).where(
            MigrationMember.job_id == job_id, MigrationMember.selected.is_(True),
            MigrationMember.state.in_((*CLAIMABLE, MemberState.PROCESSING))))).scalar_one()

    async def _acquire_lease(self, db: AsyncSession, job: MigrationJob) -> bool:
        now = self._clock()
        stale_before = now - timedelta(seconds=self.settings.migration_lease_seconds)
        res = await db.execute(update(MigrationJob).where(
            MigrationJob.id == job.id,
            (MigrationJob.worker_id.is_(None)) | (MigrationJob.worker_id == self.worker_id)
            | (MigrationJob.heartbeat_at.is_(None)) | (MigrationJob.heartbeat_at < stale_before))
            .values(worker_id=self.worker_id, heartbeat_at=now))
        await db.commit()
        return res.rowcount == 1

    async def _reset_processing(self, db: AsyncSession, job_id: uuid.UUID) -> int:
        """Members left PROCESSING by a dead run (we hold the lease, so nobody else is working on them)."""
        res = await db.execute(update(MigrationMember).where(MigrationMember.job_id == job_id,
                                                             MigrationMember.state == MemberState.PROCESSING)
                               .values(state=MemberState.ELIGIBLE, updated_at=self._clock()))
        await db.execute(update(MigrationJob).where(MigrationJob.id == job_id)
                         .values(current_member_id=None, current_member_label=None))
        return res.rowcount or 0

    async def _release(self, job_id: uuid.UUID, *, reset_processing: bool = False) -> None:
        try:
            async with self.sm() as db:
                if reset_processing:
                    await self._reset_processing(db, job_id)
                await db.execute(update(MigrationJob).where(MigrationJob.id == job_id,
                                                            MigrationJob.worker_id == self.worker_id)
                                 .values(worker_id=None))
                await db.commit()
        except Exception:  # noqa: BLE001
            log.warning("could not release lease for %s (the lease will simply expire)", job_id)

    async def _with_db(self, fn: Callable[[AsyncSession], Awaitable[T]]) -> T:
        """Run a short transaction; retry transient DATABASE errors with exponential backoff (never infinitely)."""
        attempt = 0
        while True:
            try:
                async with self.sm() as db:
                    return await fn(db)
            except (OperationalError, DBAPIError) as exc:
                transient = isinstance(exc, OperationalError) or getattr(exc, "connection_invalidated", False)
                if not transient or attempt >= 3:
                    raise
                delay = min(0.5 * (2 ** attempt), 4.0)
                attempt += 1
                log_event(log, "db_retry", level=logging.WARNING, attempt=attempt, backoff_seconds=delay,
                          error=type(exc).__name__)
                await self._sleep(delay)


def _fatal(outcome: InviteOutcome, code: str, message: str) -> InviteError:
    return InviteError(outcome, error_code=code, message=message, fatal=True)
