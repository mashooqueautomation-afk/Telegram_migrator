"""Real Redis + real arq worker (in-process, burst mode). Skipped automatically when Redis is not reachable.

Run Redis locally and, if needed, point TEST_REDIS_URL at it (default redis://localhost:6379/15 — this DB is flushed!).
"""
import asyncio
import os
import uuid

import httpx
import pytest
import pytest_asyncio

from tests.conftest import FakeFactory, connect_telegram, discover, make_settings, register_and_login, seed_world_with
from tests.fakes import flood

redis_asyncio = pytest.importorskip("redis.asyncio")
arq_worker = pytest.importorskip("arq.worker")

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest_asyncio.fixture
async def redis_url():
    r = redis_asyncio.from_url(REDIS_URL)
    try:
        await r.ping()
    except Exception:  # noqa: BLE001
        pytest.skip("Redis is not reachable; start redis-server to run the arq integration tests")
    await r.flushdb()
    yield REDIS_URL
    await r.flushdb()
    await r.aclose()


@pytest_asyncio.fixture
async def stack(schema, db_url, world, redis_url):
    from app.main import create_app
    from app.workers.queue import ArqQueue
    settings = make_settings(db_url, redis_url=redis_url)
    queue = ArqQueue(redis_url)
    app = create_app(settings, client_factory=FakeFactory(world), queue=queue)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        headers = await register_and_login(client)
        await connect_telegram(client, headers)
        yield client, headers, settings, queue
    await queue.close()
    await app.state.engine.dispose()


async def drain(settings, world):
    """Run a real arq worker until the queue is empty (burst mode)."""
    from app.workers.worker import worker_config
    worker = arq_worker.Worker(**worker_config(settings, factory=FakeFactory(world)), burst=True, poll_delay=0.05,
                               handle_signals=False)
    try:
        await asyncio.wait_for(worker.async_run(), timeout=60)
    finally:
        await worker.close()


async def job_json(client, headers, job_id):
    return (await client.get(f"/api/migrations/{job_id}", headers=headers)).json()


async def create(client, headers, world, n):
    seed_world_with(world, n)
    channels = await discover(client, headers)
    r = await client.post("/api/migrations", headers=headers, json={
        "source_channel_id": channels["Source Channel"]["id"],
        "destination_channel_id": channels["Destination Group"]["id"]})
    return r.json()["id"]


async def test_api_to_redis_to_worker_to_telegram_end_to_end(stack, world):
    client, headers, settings, queue = stack
    job_id = await create(client, headers, world, 6)

    r = await client.post(f"/api/migrations/{job_id}/eligibility", headers=headers)
    assert r.status_code == 202 and r.json()["analysis_state"] == "ANALYZING"
    queued = await (await queue._get_pool()).queued_jobs()
    assert [j.function for j in queued] == ["analyze_job"]                       # the API only queued it in Redis
    await drain(settings, world)
    job = await job_json(client, headers, job_id)
    assert job["analysis_state"] == "COMPLETED" and job["total_members"] == 6

    r = await client.post(f"/api/migrations/{job_id}/start", headers=headers, json={})
    assert r.status_code == 200 and r.json()["status"] == "RUNNING"
    assert world.invite_calls == []                                              # nothing happens until the worker runs
    queued = await (await queue._get_pool()).queued_jobs()
    assert [j.function for j in queued] == ["run_migration"]
    await drain(settings, world)

    job = await job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["successful_count"] == 6 and job["processed_members"] == 6
    assert len(world.invited) == 6 and len(world.invite_calls) == 6
    frames = (await client.get(f"/api/migrations/{job_id}/events/stream", headers=headers)).text
    assert frames.count("event: migration.member_success") == 6 and "event: migration.completed" in frames


async def test_flood_wait_defers_the_resume_in_redis_and_the_worker_resumes_it(stack, world):
    """FloodWait(1s) -> the engine pauses, schedules a DEFERRED arq job for the end of the cooldown, and the real worker
    picks it up once it is due (arq's burst mode waits for deferred jobs, so one drain covers the whole cycle)."""
    from datetime import datetime
    client, headers, settings, queue = stack
    job_id = await create(client, headers, world, 4)
    await client.post(f"/api/migrations/{job_id}/eligibility", headers=headers)
    await drain(settings, world)
    world.fail_next("InviteToChannelRequest", flood(1))
    await client.post(f"/api/migrations/{job_id}/start", headers=headers, json={})
    await drain(settings, world)

    job = await job_json(client, headers, job_id)
    assert job["status"] == "COMPLETED" and job["successful_count"] == 4 and job["rate_limited_count"] == 1
    assert len(set(world.invited)) == 4 and len(world.invite_calls) == 5          # 1 refused attempt + 4 real invites

    events = (await client.get(f"/api/migrations/{job_id}/events", headers=headers)).json()
    by_type = {e["event_type"]: e for e in events}
    assert {"migration.rate_limited", "migration.paused", "migration.resumed"} <= set(by_type)
    assert by_type["migration.rate_limited"]["error_code"] == "FLOOD_WAIT"
    assert by_type["migration.resumed"]["metadata"] == {"automatic": True}
    paused_at = datetime.fromisoformat(by_type["migration.paused"]["created_at"])
    resumed_at = datetime.fromisoformat(by_type["migration.resumed"]["created_at"])
    assert (resumed_at - paused_at).total_seconds() >= 1.8                        # it really waited out the cooldown
    types = [e["event_type"] for e in events]
    assert types.index("migration.paused") < types.index("migration.resumed") < types.index("migration.completed")


async def test_resume_is_refused_during_the_cooldown_with_a_real_queue(stack, world):
    client, headers, settings, queue = stack
    job_id = await create(client, headers, world, 3)
    await client.post(f"/api/migrations/{job_id}/eligibility", headers=headers)
    await drain(settings, world)
    world.fail_next("InviteToChannelRequest", flood(300))
    await client.post(f"/api/migrations/{job_id}/start", headers=headers, json={})
    from app.workers.worker import worker_config
    worker = arq_worker.Worker(**worker_config(settings, factory=FakeFactory(world)), burst=True, poll_delay=0.05,
                               handle_signals=False)
    task = asyncio.create_task(worker.async_run())                                # burst would wait 300s: don't drain
    for _ in range(100):
        await asyncio.sleep(0.1)
        if (await job_json(client, headers, job_id))["status"] == "PAUSED":
            break
    job = await job_json(client, headers, job_id)
    assert job["status"] == "PAUSED" and job["rate_limit_remaining_seconds"] > 250
    deferred = await (await queue._get_pool()).queued_jobs()
    assert [j.function for j in deferred] == ["run_migration"]                    # scheduled for the end of the cooldown
    r = await client.post(f"/api/migrations/{job_id}/resume", headers=headers)
    assert r.status_code == 429 and r.json()["error"]["code"] == "cooldown_active"
    assert len(world.invite_calls) == 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await worker.close()


async def test_identical_job_ids_are_deduplicated_by_the_queue(stack):
    client, headers, settings, queue = stack
    assert await queue.enqueue("run_migration", str(uuid.uuid4()), job_id="dup-1") is True
    assert await queue.enqueue("run_migration", str(uuid.uuid4()), job_id="dup-1") is False


async def test_unreachable_redis_is_a_clean_503_and_the_job_stays_startable(schema, db_url, world):
    from app.main import create_app
    from app.workers.queue import ArqQueue
    app = create_app(make_settings(db_url), client_factory=FakeFactory(world),
                     queue=ArqQueue("redis://127.0.0.1:1/0"))                    # nothing listens on port 1
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        headers = await register_and_login(client)
        await connect_telegram(client, headers)
        job_id = await create(client, headers, world, 2)
        r = await client.post(f"/api/migrations/{job_id}/eligibility", headers=headers)
        assert r.status_code == 503 and r.json()["error"]["code"] == "queue_unavailable"
        assert (await job_json(client, headers, job_id))["analysis_state"] == "NOT_ANALYZED"
    await app.state.engine.dispose()
