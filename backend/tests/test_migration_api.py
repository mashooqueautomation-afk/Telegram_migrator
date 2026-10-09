"""Migration API: validation, state errors, ownership, queue failures, worker analysis, retry, SSE."""
import asyncio
import json
import uuid

from sqlalchemy import insert, select, update

from app.db.base import utcnow
from app.domain import AnalysisState, Eligibility, JobStatus, MemberState
from app.models import MigrationEvent, MigrationJob, MigrationMember, TelegramAccount
from app.services import analysis_service
from app.services.stream import event_stream
from tests.conftest import (FakeFactory, analysed_job, discover, get_job_json, make_settings, members_by_status,
                            register_and_login, seed_world_with)
from tests.fakes import flood, make_channel, make_user, rpc
from telethon import errors as tl_err


async def post(client, headers, path, **kw):
    return await client.post(path, headers=headers, **kw)


async def run_to_completion(client, headers, engine, job_id):
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert r.status_code == 200, r.text
    return await engine.run(uuid.UUID(job_id))


def parse_sse(text: str) -> list[dict]:
    frames = []
    for block in text.split("\n\n"):
        block = block.strip()
        if not block or block.startswith((":", "retry:")):
            continue
        frame = {}
        for line in block.splitlines():
            key, _, value = line.partition(": ")
            frame[key] = value
        frame["data"] = json.loads(frame["data"])
        frames.append(frame)
    return frames


# =============================================================================================== create / analyse
async def test_create_migration_is_pending_and_waits_for_analysis(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=3, analysis=False)
    job = await get_job_json(client, headers, job_id)
    assert (job["status"], job["analysis_state"], job["total_members"]) == ("PENDING", "NOT_ANALYZED", 0)
    assert job["allowed_actions"] == ["analyze", "cancel", "delete"] and "start" not in job["allowed_actions"]
    assert job["recent_events"][0]["event_type"] == "migration.created"
    assert queue.enqueued == []                                    # creating a job never queues work


async def test_create_validation_and_authorisation(client, headers, account, world, app, queue):
    seed_world_with(world, 2)
    world.add_channel(make_channel(300, "Not Mine"), [make_user(9)])
    channels = await discover(client, headers)
    src = channels["Source Channel"]["id"]
    r = await post(client, headers, "/api/migrations", json={"source_channel_id": src, "destination_channel_id": src})
    assert r.status_code == 400 and r.json()["error"]["code"] == "same_channel"
    r = await post(client, headers, "/api/migrations",
                   json={"source_channel_id": src, "destination_channel_id": str(uuid.uuid4())})
    assert r.status_code == 404
    world.entities[200] = make_channel(200, "Destination Group", megagroup=True, admin=True, can_invite=False)
    channels = await discover(client, headers)
    r = await post(client, headers, "/api/migrations", json={"source_channel_id": src,
                                                             "destination_channel_id": channels["Destination Group"]["id"]})
    assert r.status_code == 403 and r.json()["error"]["code"] == "destination_cannot_invite"


async def test_analysis_runs_in_the_worker_and_marks_members_selected(client, headers, account, world, app, queue,
                                                                      db):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=4, dest_members=[make_user(1002, "X")])
    job = await get_job_json(client, headers, job_id)
    assert job["analysis_state"] == "COMPLETED" and job["total_members"] == 3 and job["status"] == "PENDING"
    assert job["summary"]["by_eligibility"]["ALREADY_IN_DESTINATION"] == 2          # X and the admin account itself
    assert "start" in job["allowed_actions"]
    members = await members_by_status(client, headers, job_id, eligibility="ELIGIBLE")
    assert len(members) == 3 and all(m["selected"] for m in members.values())
    assert [e["event_type"] for e in job["recent_events"]][-2:] == ["ANALYSIS_STARTED", "ANALYSIS_COMPLETED"]
    assert world.invite_calls == []                                                 # analysing never invites anybody


async def test_reanalysis_replaces_the_previous_results(client, headers, account, world, app, queue):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    world.members[200].append(make_user(ids[0], "Joined"))
    r = await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    assert r.status_code == 202 and r.json()["analysis_state"] == "ANALYZING" and r.json()["total_members"] == 0
    async with app.state.sessionmaker() as db:
        await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                    factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    job = await get_job_json(client, headers, job_id)
    assert job["total_members"] == 2 and job["summary"]["total_analyzed"] == 4      # 3 members + the admin
    assert len(queue.of("analyze_job")) == 2


async def test_analysis_conflicts(client, headers, account, world, app, queue, engine):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2, analysis=False)
    assert (await post(client, headers, f"/api/migrations/{job_id}/eligibility")).status_code == 202
    again = await post(client, headers, f"/api/migrations/{job_id}/eligibility")       # still ANALYZING
    assert again.status_code == 409 and again.json()["error"]["code"] == "analysis_running"
    async with app.state.sessionmaker() as db:
        await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                    factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    await run_to_completion(client, headers, engine, job_id)
    late = await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    assert late.status_code == 409 and late.json()["error"]["code"] == "invalid_state"


async def test_worker_analysis_failures_are_recorded_not_raised(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2, analysis=False)
    await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    world.entities[200] = make_channel(200, "Destination Group", megagroup=True, admin=True, can_invite=False)
    async with app.state.sessionmaker() as db:
        await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                    factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    job = await get_job_json(client, headers, job_id)
    assert job["analysis_state"] == "FAILED" and "invite users" in job["analysis_note"]
    assert job["recent_events"][-1]["event_type"] == "ANALYSIS_FAILED" and "start" not in job["allowed_actions"]
    assert "analyze" in job["allowed_actions"]                                      # the admin can fix it and re-run


async def test_worker_analysis_flood_wait_records_the_cooldown(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2, analysis=False)
    await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    world.fail_next("get_entity", flood(90))
    async with app.state.sessionmaker() as db:
        await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                    factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    job = await get_job_json(client, headers, job_id)
    assert job["analysis_state"] == "FAILED" and job["rate_limited_until"] and "cooldown of 90 seconds" in job["analysis_note"]


# =============================================================================================== start validation
async def test_start_requires_a_successful_analysis(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2, analysis=False)
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert r.status_code == 409 and r.json()["error"]["code"] == "not_analyzed"
    assert queue.of("run_migration") == []


async def test_partial_analysis_needs_explicit_acknowledgement(client, headers, account, world, app, queue, db):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    await db.execute(update(MigrationJob).where(MigrationJob.id == uuid.UUID(job_id))
                     .values(analysis_state=AnalysisState.PARTIAL))
    await db.commit()
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert r.status_code == 409 and r.json()["error"]["code"] == "analysis_partial"
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={"acknowledge_partial": True})
    assert r.status_code == 200 and r.json()["status"] == "RUNNING"


async def test_nothing_selected_means_nothing_to_migrate(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    await post(client, headers, f"/api/migrations/{job_id}/selection", json={"selected": False})
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert r.status_code == 409 and r.json()["error"]["code"] == "nothing_to_migrate"


async def test_start_without_a_body_works_and_a_second_start_is_an_invalid_transition(client, headers, account, world,
                                                                                      app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    assert (await post(client, headers, f"/api/migrations/{job_id}/start")).status_code == 200
    r = await post(client, headers, f"/api/migrations/{job_id}/start")
    assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_transition"
    assert len(queue.of("run_migration")) == 1


async def test_selection_is_locked_once_started_and_needs_an_analysis(client, headers, account, world, app, queue):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3, analysis=False)
    r = await post(client, headers, f"/api/migrations/{job_id}/selection", json={})
    assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_state"
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    r = await post(client, headers, f"/api/migrations/{job_id}/selection", json={"selected": False})
    assert r.status_code == 409


# =============================================================================================== invalid transitions
async def test_a_completed_migration_can_never_run_again(client, headers, account, world, app, queue, engine):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    assert await run_to_completion(client, headers, engine, job_id) == "completed"
    for action in ("resume", "pause", "cancel"):
        r = await post(client, headers, f"/api/migrations/{job_id}/{action}")
        assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_transition", action
    assert (await post(client, headers, f"/api/migrations/{job_id}/start", json={})).status_code == 409
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["allowed_actions"] == ["delete"]
    assert len(queue.of("run_migration")) == 1                                   # nothing was queued again


async def test_pending_migrations_cannot_be_paused_or_resumed(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    for action in ("pause", "resume"):
        r = await post(client, headers, f"/api/migrations/{job_id}/{action}")
        assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_transition"


# =============================================================================================== pause / cancel semantics
async def test_pause_with_no_live_worker_is_immediate_and_resume_requeues(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=3)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    r = await post(client, headers, f"/api/migrations/{job_id}/pause")
    assert r.json()["status"] == "PAUSED" and r.json()["pause_reason"] == "USER"
    assert (await post(client, headers, f"/api/migrations/{job_id}/pause")).json()["status"] == "PAUSED"   # idempotent
    before = len(queue.of("run_migration"))
    r = await post(client, headers, f"/api/migrations/{job_id}/resume")
    assert r.json()["status"] == "RUNNING" and len(queue.of("run_migration")) == before + 1


async def test_pause_request_can_be_withdrawn_while_a_worker_is_busy(client, headers, account, world, app, queue, db):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=3)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await db.execute(update(MigrationJob).where(MigrationJob.id == uuid.UUID(job_id))
                     .values(worker_id="w", heartbeat_at=utcnow()))
    await db.commit()
    r = await post(client, headers, f"/api/migrations/{job_id}/pause")
    assert r.json()["status"] == "RUNNING" and r.json()["pause_requested"] is True      # cooperative: flag only
    r = await post(client, headers, f"/api/migrations/{job_id}/resume")
    assert r.json()["pause_requested"] is False and r.json()["status"] == "RUNNING"


async def test_cancel_variants_preserve_history(client, headers, account, world, app, queue, engine, db):
    pending, _ = await analysed_job(client, headers, app, world, queue, n=2)
    r = await post(client, headers, f"/api/migrations/{pending}/cancel")
    assert r.json()["status"] == "CANCELLED" and r.json()["completed_at"]
    assert (await post(client, headers, f"/api/migrations/{pending}/cancel")).status_code == 409

    paused, _ = await analysed_job(client, headers, app, world, queue, n=2)
    await post(client, headers, f"/api/migrations/{paused}/start", json={})
    await post(client, headers, f"/api/migrations/{paused}/pause")
    assert (await post(client, headers, f"/api/migrations/{paused}/cancel")).json()["status"] == "CANCELLED"

    orphan, ids = await analysed_job(client, headers, app, world, queue, n=2)         # RUNNING but its worker is gone
    await post(client, headers, f"/api/migrations/{orphan}/start", json={})
    await db.execute(update(MigrationMember).where(MigrationMember.job_id == uuid.UUID(orphan),
                                                   MigrationMember.telegram_user_id == ids[0])
                     .values(state=MemberState.PROCESSING))
    await db.commit()
    r = await post(client, headers, f"/api/migrations/{orphan}/cancel")
    assert r.json()["status"] == "CANCELLED"
    members = await members_by_status(client, headers, orphan)
    assert members[ids[0]]["processing_status"] == "ELIGIBLE" and len(members) >= 2          # nothing deleted
    assert world.invite_calls == []


# =============================================================================================== ownership
async def test_every_migration_endpoint_checks_ownership_server_side(client, headers, account, world, app, queue,
                                                                     engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    await run_to_completion(client, headers, engine, job_id)
    intruder = await register_and_login(client)
    base = f"/api/migrations/{job_id}"
    calls = [("get", base, None), ("get", base + "/members", None), ("get", base + "/events", None),
             ("get", base + "/events/stream", None), ("post", base + "/start", {}), ("post", base + "/pause", None),
             ("post", base + "/resume", None), ("post", base + "/cancel", None), ("post", base + "/retry", None),
             ("post", base + "/eligibility", None), ("post", base + "/selection", {"selected": False}),
             ("delete", base, None)]
    for method, path, body in calls:
        r = await getattr(client, method)(path, headers=intruder, **({"json": body} if body is not None else {}))
        assert r.status_code == 404, (method, path, r.status_code)
    r = await client.get(base + "/events", headers={**intruder, "Accept": "text/event-stream"})
    assert r.status_code == 404
    assert (await client.get(base)).status_code == 401                                     # and unauthenticated is refused
    assert (await client.get("/api/migrations", headers=intruder)).json()["total"] == 0
    # the owner is unaffected
    assert (await get_job_json(client, headers, job_id))["status"] == "COMPLETED"


async def test_cannot_create_a_migration_from_someone_elses_channels(client, headers, account, world, app, queue):
    seed_world_with(world, 2)
    channels = await discover(client, headers)
    intruder = await register_and_login(client)
    r = await post(client, intruder, "/api/migrations", json={
        "source_channel_id": channels["Source Channel"]["id"],
        "destination_channel_id": channels["Destination Group"]["id"]})
    assert r.status_code == 404


async def test_worker_refuses_a_job_whose_account_belongs_to_someone_else(client, headers, account, world, app, queue,
                                                                          engine, db):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    other = await register_and_login(client)
    other_id = (await client.get("/api/auth/me", headers=other)).json()["id"]
    await db.execute(update(TelegramAccount).values(user_id=uuid.UUID(other_id)))     # tampered ownership
    await db.commit()
    assert await engine.run(uuid.UUID(job_id)) == "failed"
    assert world.invite_calls == []


# =============================================================================================== queue unavailable
async def test_queue_outage_never_leaves_jobs_in_limbo(client, headers, account, world, app, queue, engine):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=3, analysis=False)
    queue.fail = True
    r = await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    assert r.status_code == 503 and r.json()["error"]["code"] == "queue_unavailable"
    assert (await get_job_json(client, headers, job_id))["analysis_state"] == "NOT_ANALYZED"

    queue.fail = False
    await post(client, headers, f"/api/migrations/{job_id}/eligibility")
    async with app.state.sessionmaker() as db:
        await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                    factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    queue.fail = True
    r = await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert r.status_code == 503
    job = await get_job_json(client, headers, job_id)
    assert job["status"] == "PENDING" and job["started_at"] is None                         # reverted, can try again

    queue.fail = False
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await post(client, headers, f"/api/migrations/{job_id}/pause")
    queue.fail = True
    r = await post(client, headers, f"/api/migrations/{job_id}/resume")
    assert r.status_code == 503
    assert (await get_job_json(client, headers, job_id))["status"] == "PAUSED"


async def test_without_redis_configured_the_api_still_serves_everything_else(schema, db_url, world):
    from app.main import create_app
    import httpx
    app = create_app(make_settings(db_url), client_factory=FakeFactory(world))          # no queue, no REDIS_URL
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        h = await register_and_login(c)
        assert (await c.get("/api/migrations", headers=h)).status_code == 200
        assert (await c.get("/api/health")).status_code == 200
    await app.state.engine.dispose()


# =============================================================================================== retry (new linked job)
async def _finished_job_with_mixed_results(client, headers, app, world, queue, make_engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=6)
    world.invite_behavior[ids[0]] = [ConnectionError()] * 3                       # transient, exhausted
    world.invite_behavior[ids[1]] = [rpc(tl_err.UserPrivacyRestrictedError)]      # permanent
    world.invite_behavior[ids[2]] = ["noop"]                                       # accepted but unconfirmed
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert await make_engine(migration_max_retries=1).run(uuid.UUID(job_id)) == "completed"
    return job_id, ids


async def test_retry_creates_a_linked_job_with_only_unfinished_members(client, headers, account, world, app, queue,
                                                                       make_engine, db):
    job_id, ids = await _finished_job_with_mixed_results(client, headers, app, world, queue, make_engine)
    parent = await get_job_json(client, headers, job_id)
    assert (parent["successful_count"], parent["skipped_count"], parent["failed_count"]) == (3, 1, 2)
    assert "retry" in parent["allowed_actions"]

    r = await post(client, headers, f"/api/migrations/{job_id}/retry")
    assert r.status_code == 201
    child = r.json()
    assert child["retry_of_id"] == job_id and child["status"] == "PENDING" and child["total_members"] == 2
    assert child["analysis_state"] == "COMPLETED" and "Retry of migration" in child["analysis_note"]
    members = await members_by_status(client, headers, child["id"])
    assert set(members) == {ids[0], ids[2]}                                         # not the privacy refusal, not successes
    assert (await get_job_json(client, headers, job_id)) ["status"] == "COMPLETED"  # parent history is untouched
    assert "migration.retry_created" in [e["event_type"] for e in (await get_job_json(client, headers, job_id))["recent_events"]]

    world.invite_behavior.clear()
    await post(client, headers, f"/api/migrations/{child['id']}/start", json={})
    assert await make_engine().run(uuid.UUID(child["id"])) == "completed"
    done = await get_job_json(client, headers, child["id"])
    assert (done["successful_count"], done["failed_count"]) == (2, 0)
    assert [u for _, u in world.invited].count(ids[0]) == 1                          # invited once overall
    again = await post(client, headers, f"/api/migrations/{child['id']}/retry")
    assert again.status_code == 409 and again.json()["error"]["code"] == "nothing_to_retry"
    assert (await post(client, headers, f"/api/migrations/{job_id}/retry")).status_code == 201   # parent can be retried again


async def test_retry_after_a_fatal_failure_includes_the_untouched_members(client, headers, account, world, app, queue,
                                                                          engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=5)
    world.invite_behavior[ids[1]] = [rpc(tl_err.ChatAdminRequiredError)]
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    assert await engine.run(uuid.UUID(job_id)) == "failed"
    r = await post(client, headers, f"/api/migrations/{job_id}/retry")
    members = await members_by_status(client, headers, r.json()["id"])
    assert set(members) == set(ids[1:])                                              # ids[0] already succeeded


async def test_retry_only_for_finished_jobs(client, headers, account, world, app, queue):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    r = await post(client, headers, f"/api/migrations/{job_id}/retry")
    assert r.status_code == 409 and r.json()["error"]["code"] == "invalid_state"


async def test_retry_copies_large_jobs_in_batches(client, headers, account, world, app, queue, db):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    jid = uuid.UUID(job_id)
    await db.execute(update(MigrationMember).where(MigrationMember.job_id == jid).values(state=MemberState.FAILED))
    rows = [dict(job_id=jid, telegram_user_id=500_000 + i, access_hash=i, display_name=f"Bulk {i}", role="member",
                 position=100 + i, selected=True, eligibility=Eligibility.ELIGIBLE, state=MemberState.FAILED)
            for i in range(1197)]
    await db.execute(insert(MigrationMember), rows)
    await db.execute(update(MigrationJob).where(MigrationJob.id == jid).values(status=JobStatus.COMPLETED))
    await db.commit()
    r = await post(client, headers, f"/api/migrations/{job_id}/retry")
    child = r.json()
    assert r.status_code == 201 and child["total_members"] == 1200
    positions = (await db.execute(select(MigrationMember.position).where(
        MigrationMember.job_id == uuid.UUID(child["id"])))).scalars().all()
    assert len(positions) == 1200 == len(set(positions)) and sorted(positions)[0] == 0


# =============================================================================================== delete / list / members
async def test_running_jobs_cannot_be_deleted_but_finished_ones_can(client, headers, account, world, app, queue, engine,
                                                                    db):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    r = await client.delete(f"/api/migrations/{job_id}", headers=headers)
    assert r.status_code == 409 and r.json()["error"]["code"] == "job_active"
    await engine.run(uuid.UUID(job_id))
    child = (await post(client, headers, f"/api/migrations/{job_id}/retry"))      # nothing to retry -> 409, so make one
    assert child.status_code == 409
    assert (await client.delete(f"/api/migrations/{job_id}", headers=headers)).status_code == 204
    assert (await db.execute(select(MigrationMember).where(MigrationMember.job_id == uuid.UUID(job_id)))).first() is None
    assert (await db.execute(select(MigrationEvent).where(MigrationEvent.job_id == uuid.UUID(job_id)))).first() is None


async def test_list_and_member_filters(client, headers, account, world, app, queue, engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=4)
    world.invite_behavior[ids[0]] = [rpc(tl_err.UserPrivacyRestrictedError)]
    await run_to_completion(client, headers, engine, job_id)
    other, _ = await analysed_job(client, headers, app, world, queue, n=2)
    page = (await client.get("/api/migrations", headers=headers)).json()
    assert page["total"] == 2
    done = (await client.get("/api/migrations", headers=headers, params={"status": "COMPLETED"})).json()
    assert [j["id"] for j in done["items"]] == [job_id]
    item = done["items"][0]
    assert (item["total_members"], item["processed_members"], item["successful_count"], item["skipped_count"]) == (4, 4, 3, 1)
    assert item["percent_complete"] == 100.0
    skipped = await members_by_status(client, headers, job_id, processing_status="SKIPPED")
    # (the connected admin account is also SKIPPED: the analysis found it already in the destination)
    assert set(skipped) == {ids[0], 777} and skipped[ids[0]]["eligibility_status"] == "PRIVACY_RESTRICTED"
    assert skipped[777]["outcome"] is None and skipped[777]["eligibility_status"] == "ALREADY_IN_DESTINATION"
    refused = await members_by_status(client, headers, job_id, outcome="PRIVACY_RESTRICTED")
    assert set(refused) == {ids[0]}
    assert len(await members_by_status(client, headers, job_id, outcome="SUCCESS")) == 3
    assert (await client.get(f"/api/migrations/{job_id}/members", headers=headers,
                             params={"processing_status": "NOPE"})).status_code == 422


async def test_detail_gives_the_dashboard_everything_it_needs(client, headers, account, world, app, queue, engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    world.fail_next("InviteToChannelRequest", flood(90))
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await engine.run(uuid.UUID(job_id))
    job = await get_job_json(client, headers, job_id)
    for key in ("status", "pause_reason", "total_members", "processed_members", "successful_count", "skipped_count",
                "failed_count", "rate_limited_count", "remaining_members", "percent_complete", "current_member",
                "error_message", "rate_limited_until", "rate_limit_remaining_seconds", "worker_active",
                "allowed_actions", "outcome_counts", "recent_events", "started_at", "completed_at", "notes"):
        assert key in job, key
    assert job["status"] == "PAUSED" and job["remaining_members"] == 3 and job["worker_active"] is False
    assert job["recent_events"][-1]["event_type"] == "migration.paused"
    assert job["recent_events"][-1]["metadata"]["pause_reason"] == "RATE_LIMIT"


# =============================================================================================== SSE
async def test_sse_streams_the_whole_migration_with_resumable_ids(client, headers, account, world, app, queue, engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=4)
    world.invite_behavior[ids[1]] = [rpc(tl_err.UserPrivacyRestrictedError)]
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await engine.run(uuid.UUID(job_id))

    r = await client.get(f"/api/migrations/{job_id}/events", headers={**headers, "Accept": "text/event-stream"})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-cache"
    frames = parse_sse(r.text)
    types = [f["event"] for f in frames]
    for expected in ("migration.created", "migration.started", "migration.member_success", "migration.member_skipped",
                     "migration.completed", "migration.progress"):
        assert expected in types, expected
    assert types.count("migration.member_success") == 3 and types.count("migration.member_skipped") == 1
    stored = [f for f in frames if f["event"] != "migration.progress" and "id" in f]
    seqs = [int(f["id"]) for f in stored]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)                       # ordered, unique ids
    final = frames[-1]["data"]
    assert final["type"] == "migration.progress" and final["status"] == "COMPLETED"
    assert (final["processed"], final["total"], final["successful"], final["skipped"], final["failed"]) == (4, 4, 3, 1, 0)
    skipped = next(f for f in frames if f["event"] == "migration.member_skipped")["data"]
    assert skipped["error_code"] == "USER_PRIVACY_RESTRICTED" and skipped["status"] == "SKIPPED"
    assert skipped["migration_id"] == job_id and skipped["member_id"]

    resumed = await client.get(f"/api/migrations/{job_id}/events", headers={
        **headers, "Accept": "text/event-stream", "Last-Event-ID": str(seqs[2])})
    newer = [int(f["id"]) for f in parse_sse(resumed.text) if f["event"] != "migration.progress" and "id" in f]
    assert newer == seqs[3:]                                                           # only what was missed
    via_param = await client.get(f"/api/migrations/{job_id}/events/stream", headers=headers,
                                 params={"after_seq": seqs[-2]})
    assert [int(f["id"]) for f in parse_sse(via_param.text) if "id" in f] == seqs[-2:] [1:]


async def test_sse_follows_a_live_migration_until_it_ends(client, headers, account, world, app, queue, engine):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=5)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    world.invite_delay = 0.02
    stream = asyncio.create_task(client.get(f"/api/migrations/{job_id}/events/stream", headers=headers))
    await asyncio.sleep(0.15)                                                          # the client is already listening
    assert not stream.done()                                                           # a RUNNING job keeps the stream open
    await engine.run(uuid.UUID(job_id))
    r = await asyncio.wait_for(stream, timeout=10)                                     # closes by itself at the end
    frames = parse_sse(r.text)
    assert [f["event"] for f in frames].count("migration.member_success") == 5
    assert frames[-1]["data"]["status"] == "COMPLETED" and frames[-1]["data"]["processed"] == 5
    progress = [f["data"]["processed"] for f in frames if f["event"] == "migration.progress"]
    assert progress == sorted(progress) and progress[-1] == 5


async def test_sse_reports_the_rate_limit_countdown_and_stays_open_while_paused(client, headers, account, world, app,
                                                                                queue, engine, settings):
    job_id, ids = await analysed_job(client, headers, app, world, queue, n=3)
    world.fail_next("InviteToChannelRequest", flood(120))
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await engine.run(uuid.UUID(job_id))

    polls = {"n": 0}

    async def disconnected() -> bool:
        polls["n"] += 1
        return polls["n"] > 4

    async def no_wait(_s):
        return None
    chunks = [c async for c in event_stream(
        sessionmaker=app.state.sessionmaker, settings=settings.model_copy(update={"sse_heartbeat_seconds": 0.0}),
        job_id=uuid.UUID(job_id), is_disconnected=disconnected, sleep=no_wait)]
    text = "".join(chunks)
    frames = parse_sse(text)
    types = [f["event"] for f in frames]
    assert "migration.rate_limited" in types and "migration.paused" in types
    progress = frames[-1]["data"] if frames[-1]["event"] == "migration.progress" else \
        [f for f in frames if f["event"] == "migration.progress"][-1]["data"]
    assert progress["status"] == "PAUSED" and progress["pause_reason"] == "RATE_LIMIT"
    assert progress["rate_limit_remaining_seconds"] > 100 and progress["rate_limited_until"]
    assert ": ping" in text                                                            # heartbeat keeps proxies from closing it
    assert polls["n"] == 5                                                             # it kept polling: it did NOT end


async def test_sse_json_log_endpoint_pages_through_events(client, headers, account, world, app, queue, engine):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=3)
    await post(client, headers, f"/api/migrations/{job_id}/start", json={})
    await engine.run(uuid.UUID(job_id))
    events = (await client.get(f"/api/migrations/{job_id}/events", headers=headers)).json()
    assert [e["seq"] for e in events] == sorted(e["seq"] for e in events)
    assert {"id", "seq", "event_type", "message", "status", "reason", "error_code", "metadata", "created_at"} <= set(events[0])
    page = (await client.get(f"/api/migrations/{job_id}/events", headers=headers,
                             params={"after_seq": events[2]["seq"], "limit": 2})).json()
    assert [e["seq"] for e in page] == [events[3]["seq"], events[4]["seq"]]


async def test_sse_ends_cleanly_if_the_job_is_deleted_while_streaming(client, headers, account, world, app, queue,
                                                                      engine, settings):
    job_id, _ = await analysed_job(client, headers, app, world, queue, n=2)
    gen = event_stream(sessionmaker=app.state.sessionmaker, settings=settings, job_id=uuid.UUID(job_id))
    for _ in range(2):                                          # retry hint + first snapshot
        await gen.__anext__()
    await client.delete(f"/api/migrations/{job_id}", headers=headers)
    rest = [c async for c in gen]
    assert any("migration.deleted" in c for c in rest)
