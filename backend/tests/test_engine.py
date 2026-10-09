"""The migration engine against the Telethon fake: idempotency, FloodWait, refusals, retries, pause/cancel, recovery."""
import asyncio
import logging
import uuid
from collections import Counter
from datetime import timedelta

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError

from app.db.base import utcnow
from app.domain import JobStatus, MemberState
from app.models import AuditLog, MigrationEvent, MigrationJob, MigrationMember, TelegramAccount
from app.workers.recovery import recover
from tests.conftest import analysed_job, get_job_json, members_by_status
from tests.fakes import flood, make_channel, make_user, rpc
from telethon import errors as tl_err

JSON = {"json": {}}


async def start(client, headers, job_id, **body):
    r = await client.post(f"/api/migrations/{job_id}/start", headers=headers, json=body)
    assert r.status_code == 200, r.text
    return r.json()


async def event_types(db, job_id) -> list[str]:
    rows = (await db.execute(select(MigrationEvent.event_type).where(MigrationEvent.job_id == uuid.UUID(job_id))
                             .order_by(MigrationEvent.seq))).scalars().all()
    return list(rows)


async def run_job(client, headers, app, world, queue, engine, *, n=5, **kw):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=n, **kw)
    await start(client, headers, job_id)
    return job_id, ids, uuid.UUID(job_id)


# =============================================================================================== happy path
async def test_full_migration_end_to_end(client, headers, account, world, app, queue, engine, db):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=6)
    pre = await get_job_json(client, headers, job_id)
    assert pre["status"] == "PENDING" and pre["total_members"] == 6 and "start" in pre["allowed_actions"]
    assert not world.invite_calls                     # analysis + creation never invite anyone

    started = await start(client, headers, job_id)
    assert started["status"] == "RUNNING" and started["started_at"]
    assert [e["function"] for e in queue.of("run_migration")] == ["run_migration"]
    assert not world.invite_calls                     # the API only queues; the worker does the work

    assert await engine.run(uuid.UUID(job_id)) == "completed"

    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["completed_at"]
    assert (job["total_members"], job["processed_members"], job["successful_count"]) == (6, 6, 6)
    assert job["skipped_count"] == job["failed_count"] == job["rate_limited_count"] == 0
    assert job["percent_complete"] == 100.0 and job["remaining_members"] == 0 and job["current_member"] is None
    assert job["outcome_counts"] == {"SUCCESS": 6} and "delete" in job["allowed_actions"]
    assert {u.id for u in world.members[200]} >= set(ids)                      # Telegram really lists them now
    members = await members_by_status(client, headers, job_id)
    assert all(m["processing_status"] == "SUCCESS" and m["outcome"] == "SUCCESS" and m["processed_at"]
               for m in members.values() if m["telegram_user_id"] in ids)

    types = await event_types(db, job_id)
    assert types.count("migration.member_success") == 6
    assert types[:1] == ["migration.created"] and "migration.started" in types and types[-1] == "migration.completed"
    actions = set((await db.execute(select(AuditLog.action))).scalars().all())
    assert {"MIGRATION_CREATED", "MIGRATION_STARTED", "MIGRATION_COMPLETED"} <= actions


async def test_success_is_only_claimed_after_telegram_confirms_membership(client, headers, account, world, app, queue,
                                                                          engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = ["noop"]          # Telegram accepts the request but never adds the user
    await engine.run(uid)
    members = await members_by_status(client, headers, job_id)
    ghost = members[ids[0]]
    assert ghost["processing_status"] == "FAILED" and ghost["outcome"] == "NOT_CONFIRMED"
    assert ghost["telegram_error_code"] == "NOT_CONFIRMED" and "does not list" in ghost["failure_reason"]
    job = await get_job_json(client, headers, job_id)
    assert (job["successful_count"], job["failed_count"], job["status"]) == (2, 1, "COMPLETED")


# =============================================================================================== idempotency
async def test_finished_members_are_never_processed_again(client, headers, account, world, app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    assert await engine.run(uid) == "completed"
    calls = list(world.invite_calls)
    assert await engine.run(uid) == "not_runnable"    # a duplicate/late task does nothing
    assert await engine.run(uid) == "not_runnable"
    assert world.invite_calls == calls and Counter(world.invite_calls).most_common(1)[0][1] == 1


async def test_already_successful_member_in_db_is_skipped_on_resume(client, headers, account, world, app, queue,
                                                                    engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    async with app.state.sessionmaker() as db:     # a previous run already succeeded for member 0
        await db.execute(update(MigrationMember).where(MigrationMember.job_id == uid,
                                                       MigrationMember.telegram_user_id == ids[0])
                         .values(state=MemberState.SUCCESS))
        await db.commit()
    await engine.run(uid)
    assert ids[0] not in [u for _, u in world.invite_calls]
    assert len(world.invite_calls) == 3


async def test_two_workers_cannot_process_the_same_job(client, headers, account, world, app, queue, make_engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=4)
    world.invite_delay = 0.05
    a, b = make_engine("worker-A"), make_engine("worker-B")
    results = await asyncio.gather(a.run(uid), b.run(uid))
    assert sorted(results) == ["completed", "lease_held"]
    assert Counter(world.invite_calls).most_common(1)[0][1] == 1
    job = await get_job_json(client, headers, job_id)
    assert job["processed_members"] == job["successful_count"] == 4


async def test_fresh_lease_of_another_worker_is_respected(client, headers, account, world, app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    async with app.state.sessionmaker() as db:
        await db.execute(update(MigrationJob).where(MigrationJob.id == uid)
                         .values(worker_id="someone-else", heartbeat_at=utcnow()))
        await db.commit()
    assert await engine.run(uid) == "lease_held"
    assert not world.invite_calls


# =============================================================================================== refusals (never bypassed)
@pytest.mark.parametrize("behaviour,code,eligibility", [
    (rpc(tl_err.UserPrivacyRestrictedError), "USER_PRIVACY_RESTRICTED", "PRIVACY_RESTRICTED"),
    (rpc(tl_err.UserNotMutualContactError), "USER_NOT_MUTUAL_CONTACT", "PRIVACY_RESTRICTED"),
    ("missing", "MISSING_INVITEE", "PRIVACY_RESTRICTED"),
])
async def test_privacy_refusal_is_recorded_skipped_and_never_retried(client, headers, account, world, app, queue,
                                                                     engine, behaviour, code, eligibility):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    world.invite_behavior[ids[1]] = [behaviour]
    assert await engine.run(uid) == "completed"        # the migration carries on

    members = await members_by_status(client, headers, job_id)
    refused = members[ids[1]]
    assert refused["processing_status"] == "SKIPPED" and refused["outcome"] == "PRIVACY_RESTRICTED"
    assert refused["eligibility"] == eligibility and refused["telegram_error_code"] == code
    assert refused["failure_reason"] and refused["attempt_count"] == 1
    assert [u for _, u in world.invite_calls].count(ids[1]) == 1       # asked once, never again
    job = await get_job_json(client, headers, job_id)
    assert (job["skipped_count"], job["successful_count"], job["status"]) == (1, 3, "COMPLETED")
    assert job["outcome_counts"] == {"SUCCESS": 3, "PRIVACY_RESTRICTED": 1}


@pytest.mark.parametrize("behaviour,outcome,eligibility", [
    (rpc(tl_err.UserKickedError), "USER_RESTRICTED", "RESTRICTED"),
    (rpc(tl_err.UserChannelsTooMuchError), "USER_RESTRICTED", "RESTRICTED"),
    (rpc(tl_err.UserIdInvalidError), "INVALID_USER", "ELIGIBLE"),
    (rpc(tl_err.UserBotError), "INVALID_USER", "ELIGIBLE"),
])
async def test_other_permanent_refusals_are_skipped_not_retried(client, headers, account, world, app, queue, engine,
                                                                behaviour, outcome, eligibility):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [behaviour]
    await engine.run(uid)
    m = (await members_by_status(client, headers, job_id))[ids[0]]
    assert (m["processing_status"], m["outcome"], m["eligibility"]) == ("SKIPPED", outcome, eligibility)
    assert [u for _, u in world.invite_calls].count(ids[0]) == 1


async def test_member_who_joined_after_the_analysis_is_already_member(client, headers, account, world, app, queue,
                                                                      engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    world.members[200].append(make_user(ids[1], "Joined meanwhile"))      # joined on their own after the analysis
    assert await engine.run(uid) == "completed"
    m = (await members_by_status(client, headers, job_id))[ids[1]]
    assert m["processing_status"] == "ALREADY_MEMBER" and m["outcome"] == "ALREADY_MEMBER"
    assert m["eligibility"] == "ALREADY_IN_DESTINATION"
    assert ids[1] not in [u for _, u in world.invite_calls]                # not even asked
    job = await get_job_json(client, headers, job_id)
    assert (job["skipped_count"], job["successful_count"]) == (1, 3)


async def test_already_participant_error_from_telegram_is_classified(client, headers, account, world, app, queue,
                                                                     engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [rpc(tl_err.UserAlreadyParticipantError)]    # raced between check and invite
    await engine.run(uid)
    assert (await members_by_status(client, headers, job_id))[ids[0]]["processing_status"] == "ALREADY_MEMBER"


# =============================================================================================== retries / backoff
async def test_temporary_errors_retry_with_exponential_backoff_then_succeed(client, headers, account, world, app,
                                                                            queue, engine, sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [ConnectionError(), ConnectionError(), ConnectionError()]
    await engine.run(uid)
    assert sleeps.calls == [1.0, 2.0, 4.0]                                  # 1s, 2s, 4s
    m = (await members_by_status(client, headers, job_id))[ids[0]]
    assert m["processing_status"] == "SUCCESS" and m["attempt_count"] == 4
    assert [u for _, u in world.invite_calls].count(ids[0]) == 4


async def test_retries_are_bounded_and_the_backoff_is_capped(client, headers, account, world, app, queue, make_engine,
                                                              sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=3)
    world.invite_behavior[ids[0]] = [ConnectionError()] * 50                # would loop forever if unbounded
    await make_engine(migration_max_retries=4, migration_retry_max_seconds=3).run(uid)
    assert sleeps.calls == [1.0, 2.0, 3.0, 3.0]                             # capped at 3s, exactly 4 retries
    assert [u for _, u in world.invite_calls].count(ids[0]) == 5            # 1 try + 4 retries, then it gives up
    members = await members_by_status(client, headers, job_id)
    failed = members[ids[0]]
    assert failed["processing_status"] == "FAILED" and failed["outcome"] == "TEMPORARY_ERROR"
    assert members[ids[1]]["processing_status"] == "SUCCESS"                # the migration continued
    job = await get_job_json(client, headers, job_id)
    assert (job["failed_count"], job["successful_count"], job["status"]) == (1, 2, "COMPLETED")


async def test_zero_retries_means_zero_retries(client, headers, account, world, app, queue, make_engine, sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=2)
    world.invite_behavior[ids[0]] = [ConnectionError()] * 5
    await make_engine(migration_max_retries=0).run(uid)
    assert sleeps.calls == [] and [u for _, u in world.invite_calls].count(ids[0]) == 1


async def test_circuit_breaker_pauses_after_consecutive_failures(client, headers, account, world, app, queue,
                                                                 make_engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=5)
    world.invite_behavior[ids[0]] = [ConnectionError()] * 20
    world.invite_behavior[ids[1]] = [ConnectionError()] * 20
    result = await make_engine(migration_max_consecutive_failures=2, migration_max_retries=1).run(uid)
    assert result == "paused"
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["pause_reason"] == "ERRORS" and job["failed_count"] == 2
    members = await members_by_status(client, headers, job_id)
    assert members[ids[2]]["processing_status"] == "ELIGIBLE"                # untouched, waiting for a human
    assert "migration.errors_pause" in await event_types(db, job_id)


# =============================================================================================== FloodWait
async def test_flood_wait_pauses_the_job_stores_the_cooldown_and_stops_processing(client, headers, account, world,
                                                                                  app, queue, engine, clock, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=5)
    world.fail_next("InviteToChannelRequest", flood(120))
    assert await engine.run(uid) == "rate_limited"

    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["pause_reason"] == "RATE_LIMIT"
    assert job["rate_limited_count"] == 1 and job["processed_members"] == 0
    assert job["rate_limit_remaining_seconds"] > 100                          # the countdown the UI shows
    assert job["current_member"] is None and "resume" not in job["allowed_actions"]
    assert "cancel" in job["allowed_actions"] and "Telegram requested a cooldown" in " ".join(job["notes"])
    until = (await db.get(MigrationJob, uid)).rate_limited_until
    assert until == clock.now + timedelta(seconds=121)                        # 120s requested + 1s margin, persisted
    assert (await db.execute(select(TelegramAccount.flood_wait_until))).scalar_one() == until   # account-wide too

    assert len(world.invite_calls) == 1                                        # nothing else was attempted
    members = await members_by_status(client, headers, job_id)
    assert members[ids[0]]["processing_status"] == "RATE_LIMITED"
    assert all(members[i]["processing_status"] == "ELIGIBLE" for i in ids[1:])

    types = await event_types(db, job_id)
    assert types[-2:] == ["migration.rate_limited", "migration.paused"]
    flood_ev = (await db.execute(select(MigrationEvent).where(MigrationEvent.event_type == "migration.rate_limited")
                                 )).scalar_one()
    assert flood_ev.error_code == "FLOOD_WAIT" and flood_ev.data["retry_after"] == 120
    # an automatic resume is scheduled for exactly when the cooldown ends
    resume = [e for e in queue.of("run_migration") if e["defer_until"] is not None]
    assert resume and resume[-1]["defer_until"] == until


async def test_cannot_resume_before_the_cooldown_ends(client, headers, account, world, app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("InviteToChannelRequest", flood(120))
    await engine.run(uid)
    r = await client.post(f"/api/migrations/{job_id}/resume", headers=headers)
    assert r.status_code == 429 and r.json()["error"]["code"] == "cooldown_active"
    assert 100 < int(r.headers["retry-after"]) <= 122 and r.json()["error"]["retry_after"] == int(r.headers["retry-after"])
    assert (await get_job_json(client, headers, job_id))["status"] == "PAUSED"
    assert len(world.invite_calls) == 1


async def test_job_resumes_automatically_after_the_cooldown(client, headers, account, world, app, queue, engine,
                                                            clock, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    world.fail_next("InviteToChannelRequest", flood(120))
    await engine.run(uid)

    assert await engine.run(uid) == "cooldown_pending"          # the deferred task fires early: still waiting
    assert len(world.invite_calls) == 1
    clock.advance(60)
    assert await engine.run(uid) == "cooldown_pending"

    clock.advance(70)                                             # now past the 121s
    assert await engine.run(uid) == "completed"
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["successful_count"] == 4 and job["rate_limited_until"] is None
    assert len(world.invite_calls) == 5 and len(set(world.invited)) == 4      # 1 flooded attempt + 4 real invites
    resumed = (await db.execute(select(MigrationEvent).where(MigrationEvent.event_type == "migration.resumed"))
               ).scalar_one()
    assert resumed.data == {"automatic": True}


async def test_manual_resume_after_cooldown_works(client, headers, account, world, app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("InviteToChannelRequest", flood(30))
    await engine.run(uid)
    async with app.state.sessionmaker() as db:                    # the cooldown has elapsed
        past = utcnow() - timedelta(seconds=5)
        await db.execute(update(MigrationJob).where(MigrationJob.id == uid).values(rate_limited_until=past))
        await db.execute(update(TelegramAccount).values(flood_wait_until=past))
        await db.commit()
    before = len(queue.of("run_migration"))
    r = await client.post(f"/api/migrations/{job_id}/resume", headers=headers)
    assert r.status_code == 200 and r.json()["status"] == "RUNNING" and r.json()["pause_reason"] is None
    assert len(queue.of("run_migration")) == before + 1
    clock_free = await engine.run(uid)
    assert clock_free == "completed" and len(set(world.invited)) == 3


async def test_account_wide_cooldown_from_elsewhere_is_honoured_without_calling_telegram(
        client, headers, account, world, app, queue, engine, clock, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    until = clock.now + timedelta(seconds=300)
    await db.execute(update(TelegramAccount).values(flood_wait_until=until))     # e.g. set by channel discovery
    await db.commit()
    assert await engine.run(uid) == "rate_limited"
    assert world.invite_calls == []
    assert not any(c[0] == "request" and c[1] == "GetParticipantRequest" for c in world.calls)
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["pause_reason"] == "RATE_LIMIT" and job["rate_limited_count"] == 0
    assert (await db.get(MigrationJob, uid)).rate_limited_until == until


async def test_flood_during_the_membership_check_also_pauses_safely(client, headers, account, world, app, queue,
                                                                    engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("GetParticipantRequest", flood(15))
    assert await engine.run(uid) == "rate_limited"
    assert world.invite_calls == []                                     # never invited while the check was limited
    assert (await members_by_status(client, headers, job_id))[ids[0]]["processing_status"] == "RATE_LIMITED"


async def test_user_pausing_a_rate_limited_job_stops_the_automatic_resume(client, headers, account, world, app,
                                                                          queue, engine, clock):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("InviteToChannelRequest", flood(60))
    await engine.run(uid)
    r = await client.post(f"/api/migrations/{job_id}/pause", headers=headers)
    assert r.json()["pause_reason"] == "USER" and r.json()["status"] == "PAUSED"
    clock.advance(500)
    assert await engine.run(uid) == "paused"                              # cooldown is over, but a human said stop
    assert len(world.invite_calls) == 1


# =============================================================================================== fatal conditions
@pytest.mark.parametrize("exc,code,outcome", [
    (rpc(tl_err.ChatAdminRequiredError), "CHAT_ADMIN_REQUIRED", "ADMIN_REQUIRED"),
    (rpc(tl_err.PeerFloodError), "PEER_FLOOD", "ACCOUNT_RESTRICTED"),
    (rpc(tl_err.UserBannedInChannelError), "USER_BANNED_IN_CHANNEL", "ACCOUNT_RESTRICTED"),
    (rpc(tl_err.ChannelPrivateError), "CHANNEL_PRIVATE", "CHANNEL_RESTRICTED"),
    (rpc(tl_err.UsersTooMuchError), "USERS_TOO_MUCH", "CHANNEL_RESTRICTED"),
])
async def test_fatal_conditions_stop_the_job_and_report(client, headers, account, world, app, queue, engine, db,
                                                        exc, code, outcome):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=5)
    world.invite_behavior[ids[1]] = [exc]
    assert await engine.run(uid) == "failed"

    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "FAILED" and job["error_code"] == code and job["error_message"]
    assert job["processed_members"] == 1 and job["successful_count"] == 1
    assert len(world.invite_calls) == 2                                      # stopped immediately, no retry, no skipping on
    members = await members_by_status(client, headers, job_id)
    assert members[ids[1]]["processing_status"] == "ELIGIBLE"                # not wrongly marked failed/processed
    assert members[ids[1]]["telegram_error_code"] == code
    assert all(members[i]["processing_status"] == "ELIGIBLE" for i in ids[2:])
    failed_ev = (await db.execute(select(MigrationEvent).where(MigrationEvent.event_type == "migration.failed"))
                 ).scalar_one()
    assert failed_ev.error_code == code and failed_ev.data["outcome"] == outcome
    assert "MIGRATION_FAILED" in set((await db.execute(select(AuditLog.action))).scalars().all())


async def test_peer_flood_message_tells_the_admin_to_stop_and_check_spambot(client, headers, account, world, app,
                                                                            queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=2)
    world.invite_behavior[ids[0]] = [rpc(tl_err.PeerFloodError)]
    await engine.run(uid)
    assert "SpamBot" in (await get_job_json(client, headers, job_id))["error_message"]


async def test_revoked_session_fails_the_job_and_flags_the_account(client, headers, account, world, app, queue,
                                                                   engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [rpc(tl_err.AuthKeyUnregisteredError)]
    assert await engine.run(uid) == "failed"
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "FAILED" and job["error_code"] == "SESSION_INVALID"
    assert (await client.get("/api/telegram/accounts", headers=headers)).json()[0]["status"] == "NEEDS_REAUTH"


async def test_lost_admin_rights_are_detected_before_any_invitation(client, headers, account, world, app, queue,
                                                                    engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.entities[200] = make_channel(200, "Destination Group", megagroup=True, admin=True, can_invite=False)
    assert await engine.run(uid) == "failed"
    assert world.invite_calls == []
    assert (await get_job_json(client, headers, job_id))["error_code"] == "CHAT_ADMIN_REQUIRED"


async def test_unknown_telegram_error_is_not_retried_and_does_not_stop_the_job(client, headers, account, world, app,
                                                                               queue, engine, sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [rpc(tl_err.RPCError)]
    await engine.run(uid)
    m = (await members_by_status(client, headers, job_id))[ids[0]]
    assert m["processing_status"] == "FAILED" and m["outcome"] == "UNKNOWN_ERROR" and sleeps.calls == []
    assert [u for _, u in world.invite_calls].count(ids[0]) == 1


# =============================================================================================== pause / resume / cancel
async def test_pause_is_cooperative_and_resume_continues_without_duplicates(client, headers, account, world, app,
                                                                            queue, engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=6)
    seen = {}

    async def hook(cid, user_id):
        if user_id == ids[2]:                         # while member #3 is in flight, the admin presses Pause
            r = await client.get(f"/api/migrations/{job_id}", headers=headers)
            seen["mid_flight"] = r.json()
            r = await client.post(f"/api/migrations/{job_id}/pause", headers=headers)
            seen["pause_response"] = r.json()
    world.on_invite = hook

    assert await engine.run(uid) == "paused"
    assert seen["mid_flight"]["worker_active"] is True and seen["mid_flight"]["current_member"]["label"] == "@m003"
    assert seen["pause_response"]["status"] == "RUNNING" and seen["pause_response"]["pause_requested"] is True
    assert "resume" in seen["pause_response"]["allowed_actions"] and "pause" not in seen["pause_response"]["allowed_actions"]

    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["pause_reason"] == "USER" and job["pause_requested"] is False
    assert job["processed_members"] == 3 and job["successful_count"] == 3     # the in-flight member finished and was recorded
    assert len(world.invite_calls) == 3                                        # ...and nobody new started
    types = await event_types(db, job_id)
    assert types.index("migration.pause_requested") < types.index("migration.paused")

    world.on_invite = None
    r = await client.post(f"/api/migrations/{job_id}/resume", headers=headers)
    assert r.status_code == 200 and r.json()["status"] == "RUNNING"
    assert await engine.run(uid) == "completed"
    assert Counter(world.invite_calls).most_common(1)[0][1] == 1               # no member was ever invited twice
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["successful_count"] == 6 == job["processed_members"]
    assert "migration.resumed" in await event_types(db, job_id)


async def test_cancel_stops_new_members_lets_the_current_one_finish_and_keeps_results(client, headers, account, world,
                                                                                       app, queue, engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=6)
    seen = {}

    async def hook(cid, user_id):
        if user_id == ids[2]:
            r = await client.post(f"/api/migrations/{job_id}/cancel", headers=headers)
            seen["cancel_response"] = r.json()
    world.on_invite = hook

    assert await engine.run(uid) == "cancelled"
    assert seen["cancel_response"]["status"] == "CANCELLING" and seen["cancel_response"]["cancellation_requested"]
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "CANCELLED" and job["completed_at"] and job["cancellation_requested"] is False
    assert job["successful_count"] == 3 and len(world.invite_calls) == 3     # current finished, no new ones
    members = await members_by_status(client, headers, job_id)
    assert [members[i]["processing_status"] for i in ids[:3]] == ["SUCCESS"] * 3            # history preserved
    assert all(members[i]["processing_status"] == "ELIGIBLE" for i in ids[3:])
    assert len(members) >= 6                                                   # nothing was deleted
    assert {"migration.cancel_requested", "migration.cancelled"} <= set(await event_types(db, job_id))
    actions = set((await db.execute(select(AuditLog.action))).scalars().all())
    assert {"MIGRATION_CANCEL_REQUESTED", "MIGRATION_CANCELLED"} <= actions
    assert await engine.run(uid) == "not_runnable"


# =============================================================================================== crash recovery
async def _simulate_crash(app, uid, member_user_id, *, landed: bool, world):
    """A worker invited `member_user_id` (and maybe the invite landed) then died before recording anything."""
    if landed:
        world.members[200].append(make_user(member_user_id, "Landed"))
    old = utcnow() - timedelta(minutes=10)
    async with app.state.sessionmaker() as db:
        await db.execute(update(MigrationMember).where(MigrationMember.job_id == uid,
                                                       MigrationMember.telegram_user_id == member_user_id)
                         .values(state=MemberState.PROCESSING, invite_attempted_at=old, attempt_count=1,
                                 last_attempt_at=old))
        await db.execute(update(MigrationJob).where(MigrationJob.id == uid)
                         .values(worker_id="dead-worker", heartbeat_at=old, current_member_label="@m001"))
        await db.commit()


async def test_recovery_sweep_requeues_a_job_whose_worker_died_and_continues_from_the_last_state(
        client, headers, account, world, app, queue, engine, settings, clock, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=4)
    await _simulate_crash(app, uid, ids[0], landed=True, world=world)
    queued_before = len(queue.of("run_migration"))

    report = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)
    assert report["requeued"] == 1 and report["reset_members"] == 1
    assert len(queue.of("run_migration")) == queued_before + 1
    again = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)   # next 30 s sweep
    assert again["reset_members"] == 0                                                          # ...is quiet:
    recovered_events = (await db.execute(select(MigrationEvent).where(MigrationEvent.event_type == "migration.recovered"))
                        ).scalars().all()
    assert len(recovered_events) == 1                                                           # one event, not one per sweep
    audits = (await db.execute(select(AuditLog).where(AuditLog.action == "MIGRATION_RECOVERED"))).scalars().all()
    assert len(audits) == 1
    assert (await members_by_status(client, headers, job_id))[ids[0]]["processing_status"] == "ELIGIBLE"
    assert "migration.recovered" in await event_types(db, job_id)

    assert await engine.run(uid) == "completed"
    members = await members_by_status(client, headers, job_id)
    recovered = members[ids[0]]
    assert recovered["processing_status"] == "SUCCESS" and "earlier invitation" in (
        (await db.execute(select(MigrationEvent.reason).where(MigrationEvent.member_id == uuid.UUID(recovered["id"])))).scalar_one())
    assert ids[0] not in [u for _, u in world.invite_calls]                  # verified, NOT invited a second time
    job = await get_job_json(client, headers, job_id)
    assert (job["processed_members"], job["successful_count"], job["status"]) == (4, 4, "COMPLETED")
    assert Counter(world.invite_calls).most_common(1)[0][1] == 1


async def test_recovery_of_a_member_whose_invite_never_landed_reinvites_exactly_once(client, headers, account, world,
                                                                                      app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    await _simulate_crash(app, uid, ids[0], landed=False, world=world)
    assert await engine.run(uid) == "completed"                              # engine takes over the stale lease itself
    assert [u for _, u in world.invite_calls].count(ids[0]) == 1
    assert len(set(world.invited)) == 3


async def test_a_cancelled_task_leaves_a_consistent_state(client, headers, account, world, app, queue, engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_delay = 0.4
    task = asyncio.create_task(engine.run(uid))
    while world.inflight == 0:
        await asyncio.sleep(0.01)
    task.cancel()                                                            # e.g. worker shutdown mid-invite
    with pytest.raises(asyncio.CancelledError):
        await task
    job = await db.get(MigrationJob, uid)
    await db.refresh(job)
    assert job.worker_id is None and job.status is JobStatus.RUNNING
    states = (await db.execute(select(MigrationMember.state).where(MigrationMember.job_id == uid))).scalars().all()
    assert MemberState.PROCESSING not in states

    world.invite_delay = 0
    assert await engine.run(uid) == "completed"
    assert len(set(world.invited)) == 3 and len(world.invited) == 3          # the interrupted invite never landed twice


async def test_recovery_finalises_a_dead_cancelling_job_and_a_dead_pausing_job(client, headers, account, world, app,
                                                                                queue, engine, settings, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    old = utcnow() - timedelta(minutes=10)
    await db.execute(update(MigrationJob).where(MigrationJob.id == uid).values(
        status=JobStatus.CANCELLING, cancellation_requested=True, worker_id="dead", heartbeat_at=old))
    await db.commit()
    report = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)
    assert report["cancelled"] == 1
    assert (await get_job_json(client, headers, job_id))["status"] == "CANCELLED"



async def test_dead_job_with_pending_pause_ends_up_paused(client, headers, account, world, app, queue, engine,
                                                          settings, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    old = utcnow() - timedelta(minutes=10)
    await db.execute(update(MigrationJob).where(MigrationJob.id == uid).values(
        pause_requested=True, worker_id="dead", heartbeat_at=old))
    await db.commit()
    report = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)
    assert report["paused"] == 1
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["pause_reason"] == "USER"


async def test_sweep_requeues_rate_limited_jobs_whose_cooldown_ended(client, headers, account, world, app, queue,
                                                                     engine, settings, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("InviteToChannelRequest", flood(10))
    await engine.run(uid)
    before = len(queue.of("run_migration"))
    assert (await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue))["cooldown_requeued"] == 0
    await db.execute(update(MigrationJob).where(MigrationJob.id == uid).values(rate_limited_until=utcnow() - timedelta(seconds=1)))
    await db.commit()
    report = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)
    assert report["cooldown_requeued"] == 1 and len(queue.of("run_migration")) == before + 1


async def test_healthy_running_job_is_left_alone_by_the_sweeper(client, headers, account, world, app, queue, engine,
                                                                settings):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    async with app.state.sessionmaker() as db:
        await db.execute(update(MigrationJob).where(MigrationJob.id == uid)
                         .values(worker_id="alive", heartbeat_at=utcnow()))
        await db.commit()
    queued = len(queue.of("run_migration"))
    report = await recover(sessionmaker=app.state.sessionmaker, settings=settings, queue=queue)
    assert report["requeued"] == 0 and len(queue.of("run_migration")) == queued


# =============================================================================================== concurrency / throttling
async def test_configurable_concurrency_never_duplicates_a_member(client, headers, account, world, app, queue,
                                                                  make_engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=12)
    world.invite_delay = 0.03
    assert await make_engine(migration_concurrency=3).run(uid) == "completed"
    assert 2 <= world.max_inflight <= 3                                      # parallel, but never beyond the setting
    assert Counter(world.invite_calls).most_common(1)[0][1] == 1 and len(world.invited) == 12
    job = await get_job_json(client, headers, job_id)
    assert (job["processed_members"], job["successful_count"]) == (12, 12)    # counters stayed exact under concurrency


async def test_default_concurrency_is_one(client, headers, account, world, app, queue, engine):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=6)
    world.invite_delay = 0.01
    await engine.run(uid)
    assert world.max_inflight == 1


async def test_min_delay_throttles_between_members(client, headers, account, world, app, queue, make_engine, sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=4)
    await make_engine(migration_min_delay_seconds=5.0).run(uid)
    assert sleeps.calls == [5.0, 5.0, 5.0]                                    # a 5 s gap before members 2, 3, 4


async def test_long_waits_are_chopped_so_the_heartbeat_stays_fresh(client, headers, account, world, app, queue,
                                                                    make_engine, sleeps):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, None, n=2)
    await make_engine(migration_min_delay_seconds=25.0).run(uid)
    assert sleeps.calls == [10.0, 10.0, 5.0]


# =============================================================================================== selection
async def test_deselected_members_are_not_migrated(client, headers, account, world, app, queue, engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=5)
    r = await client.post(f"/api/migrations/{job_id}/selection", headers=headers,
                          json={"telegram_user_ids": [ids[0], ids[1], 424242], "selected": False})
    assert r.json() == {"updated": 2, "ignored": 1, "selected_total": 3}
    assert (await get_job_json(client, headers, job_id))["total_members"] == 3
    await start(client, headers, job_id)
    await engine.run(uuid.UUID(job_id))
    assert {u for _, u in world.invite_calls} == set(ids[2:])
    job = await get_job_json(client, headers, job_id)
    assert (job["total_members"], job["processed_members"], job["status"]) == (3, 3, "COMPLETED")
    members = await members_by_status(client, headers, job_id)
    assert members[ids[0]]["selected"] is False and members[ids[0]]["processing_status"] == "ELIGIBLE"


# =============================================================================================== resilience / observability
async def test_transient_database_errors_are_retried_without_double_counting(client, headers, account, world, app,
                                                                              queue, engine, sleeps, monkeypatch):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=2)
    import app.workers.engine as eng
    real, state = eng.append_event, {"failed": False}

    async def flaky(db, job_id_, event_type, *a, **kw):
        if event_type == "migration.member_success" and not state["failed"]:
            state["failed"] = True
            raise OperationalError("INSERT", {}, Exception("database is locked"))
        return await real(db, job_id_, event_type, *a, **kw)
    monkeypatch.setattr(eng, "append_event", flaky)

    assert await engine.run(uid) == "completed"
    assert state["failed"] and 0.5 in sleeps.calls                             # backed off, then retried the transaction
    job = await get_job_json(client, headers, job_id)
    assert (job["processed_members"], job["successful_count"]) == (2, 2)        # counted once, not twice


async def test_structured_logs_carry_ids_and_never_secrets(client, headers, account, world, app, queue, engine,
                                                           caplog, settings):
    caplog.set_level(logging.INFO)
    caplog.set_level(logging.DEBUG, logger="app")
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.invite_behavior[ids[0]] = [rpc(tl_err.UserPrivacyRestrictedError)]   # member 1: a recorded refusal

    async def arm_flood(cid, user_id):
        if user_id == ids[1]:                                                    # member 2: Telegram says slow down
            world.fail_next("InviteToChannelRequest", flood(5))
    world.on_invite = arm_flood
    await engine.run(uid)
    text = "\n".join(r.getMessage() for r in caplog.records if r.name.startswith("app."))
    assert f"migration_id={job_id}" in text and "event=member_result" in text and "member_id=" in text
    assert "status=SKIPPED" in text and "error_code=USER_PRIVACY_RESTRICTED" in text
    assert "event=flood_wait" in text and "error_code=FLOOD_WAIT" in text
    everything = "\n".join(r.getMessage() for r in caplog.records)
    real_session = next(t for t, s in world.sessions.items() if s["user"])
    for secret in (real_session, settings.telegram_api_hash.get_secret_value(), "+14155550123"):
        assert secret not in everything


async def test_event_metadata_and_audit_details_never_contain_credentials(client, headers, account, world, app, queue,
                                                                          engine, db):
    job_id, ids, uid = await run_job(client, headers, app, world, queue, engine, n=3)
    world.fail_next("InviteToChannelRequest", flood(5))
    await engine.run(uid)
    blob = " ".join(str(e.data) for e in (await db.execute(select(MigrationEvent))).scalars().all())
    blob += " ".join(str(a.details) for a in (await db.execute(select(AuditLog))).scalars().all())
    real_session = next(t for t, s in world.sessions.items() if s["user"])
    for secret in (real_session, "access_hash", "+1415", "api_hash"):
        assert secret not in blob
