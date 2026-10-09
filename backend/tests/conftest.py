from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.db.base import utcnow
from app.main import create_app
from app.models import Base
from app.workers.engine import MigrationEngine
from app.workers.queue import QueueUnavailable
from tests.fakes import FakeFactory, FakeTelegram, make_channel, make_user

PHONE = "+14155550123"
PASSWORD = "correct-horse-battery"


def make_settings(db_url: str, **overrides) -> Settings:
    values = dict(
        app_env="test", database_url=db_url, telegram_api_id=12345,
        telegram_api_hash="0123456789abcdef0123456789abcdef",
        session_encryption_keys=Fernet.generate_key().decode(),
        jwt_secret="test-secret-" + "x" * 40, auth_rate_limit_per_minute=1000, max_inline_flood_wait_seconds=5,
        migration_min_delay_seconds=0, migration_retry_base_seconds=1, migration_retry_max_seconds=30,
        migration_max_retries=4, sse_poll_interval_seconds=0.02, sse_heartbeat_seconds=60,
    )
    values.update(overrides)
    return Settings(_env_file=None, **values)


@pytest.fixture
def db_url(tmp_path) -> str:
    # Default: a throw-away SQLite file so `pytest` works anywhere.
    # Set TEST_DATABASE_URL=postgresql+asyncpg://... to run the SAME suite against PostgreSQL.
    return os.environ.get("TEST_DATABASE_URL") or f"sqlite+aiosqlite:///{tmp_path}/test.db"


@pytest_asyncio.fixture
async def schema(db_url):
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def world() -> FakeTelegram:
    return FakeTelegram()


@pytest.fixture
def settings(db_url) -> Settings:
    return make_settings(db_url)


class RecordingQueue:
    """Stands in for arq/Redis: records what the API asked the worker to do (the tests then run the worker code)."""

    def __init__(self) -> None:
        self.enqueued: list[dict] = []
        self.fail = False

    async def enqueue(self, function, *args, job_id=None, defer_until=None) -> bool:
        if self.fail:
            raise QueueUnavailable()
        self.enqueued.append({"function": function, "args": args, "job_id": job_id, "defer_until": defer_until})
        return True

    async def close(self) -> None:
        return None

    def of(self, function: str) -> list[dict]:
        return [e for e in self.enqueued if e["function"] == function]


class FakeClock:
    def __init__(self) -> None:
        self.now = utcnow()

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class Sleeps:
    """Injected asyncio.sleep: records durations instead of waiting."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(round(seconds, 3))


@pytest.fixture
def queue() -> RecordingQueue:
    return RecordingQueue()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleeps() -> Sleeps:
    return Sleeps()


@pytest_asyncio.fixture
async def app(schema, settings, world, queue):
    application = create_app(settings, client_factory=FakeFactory(world), queue=queue)
    yield application
    await application.state.engine.dispose()


@pytest_asyncio.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def db(app):
    async with app.state.sessionmaker() as session:
        yield session


# ---------------------------------------------------------------- helpers
async def register_and_login(client, email: str | None = None) -> dict:
    email = email or f"{uuid.uuid4().hex[:8]}@example.com"
    r = await client.post("/api/auth/register", json={"email": email, "password": PASSWORD})
    assert r.status_code == 201, r.text
    r = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


async def connect_telegram(client, headers, phone: str = PHONE, code: str = "12345") -> dict:
    r = await client.post("/api/auth/telegram/start", json={"phone": phone}, headers=headers)
    assert r.status_code == 200, r.text
    login_id = r.json()["login_id"]
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": code}, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "connected", r.text
    return r.json()["account"]


@pytest_asyncio.fixture
async def headers(client):
    return await register_and_login(client)


@pytest_asyncio.fixture
async def account(client, headers, world):
    return await connect_telegram(client, headers)


@pytest.fixture
def seeded_world(world):
    """Source channel (admin), destination channel (admin w/ invite), a channel the user does NOT admin."""
    src_members = [
        make_user(1, "Alice", username="alice", role="member"),
        make_user(2, "Bob", role="member"),
        make_user(3, "Carol", role="member"),                      # will already be in destination
        make_user(4, "Robo", username="robo_bot", bot=True),
        make_user(5, None, deleted=True),
        make_user(6, "Frank", restricted=True),
        make_user(7, "Gina", minimal=True),                        # Telegram gave only a partial stub
        make_user(777, "Ada", username="ada_admin", role="creator"),
    ]
    dst_members = [make_user(3, "Carol"), make_user(777, "Ada")]
    world.add_channel(make_channel(100, "Source Channel", creator=True, username="srcchan"), src_members)
    world.add_channel(make_channel(200, "Destination Group", megagroup=True, admin=True), dst_members)
    world.add_channel(make_channel(300, "Someone Else's Channel"), [make_user(9, "Zed")])
    return world


async def discover(client, headers) -> dict[str, dict]:
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 200, r.text
    return {c["title"]: c for c in r.json()}


@pytest.fixture
def make_engine(app, settings, world, queue, clock, sleeps):
    def build(worker_id: str = "worker-A", **overrides) -> MigrationEngine:
        cfg = settings.model_copy(update=overrides) if overrides else settings
        return MigrationEngine(sessionmaker=app.state.sessionmaker, settings=cfg, vault=app.state.vault,
                               factory=FakeFactory(world), queue=queue, worker_id=worker_id, clock=clock,
                               sleep=sleeps, monotonic=lambda: 0.0)
    return build


@pytest.fixture
def engine(make_engine):
    return make_engine()


def seed_world_with(world, n: int, *, dest_members=()):
    """Source channel 100 with `n` plain members (ids 1001..), destination supergroup 200 (admin: us)."""
    users = [make_user(1000 + i, f"Member{i:03d}", username=f"m{i:03d}") for i in range(1, n + 1)]
    world.add_channel(make_channel(100, "Source Channel", creator=True), users + [make_user(777, "Ada")])
    world.add_channel(make_channel(200, "Destination Group", megagroup=True, admin=True),
                      [make_user(777, "Ada"), *dest_members])
    return [u.id for u in users]


async def analysed_job(client, headers, app, world, queue, *, n: int = 5, dest_members=(), analysis: bool = True):
    """Create a migration through the API and run the queued analysis exactly as the worker would."""
    from app.services import analysis_service
    ids = seed_world_with(world, n, dest_members=dest_members)
    channels = await discover(client, headers)
    r = await client.post("/api/migrations", headers=headers, json={
        "source_channel_id": channels["Source Channel"]["id"],
        "destination_channel_id": channels["Destination Group"]["id"]})
    assert r.status_code == 201, r.text
    job_id = r.json()["id"]
    if analysis:
        r = await client.post(f"/api/migrations/{job_id}/eligibility", headers=headers)
        assert r.status_code == 202, r.text
        assert queue.of("analyze_job"), "the API must queue the analysis, not run it"
        async with app.state.sessionmaker() as db:
            await analysis_service.analyze_existing_job(db, settings=app.state.settings, vault=app.state.vault,
                                                        factory=FakeFactory(world), job_id=uuid.UUID(job_id))
    return job_id, ids


async def get_job_json(client, headers, job_id) -> dict:
    r = await client.get(f"/api/migrations/{job_id}", headers=headers)
    assert r.status_code == 200, r.text
    return r.json()


async def members_by_status(client, headers, job_id, **params) -> dict[int, dict]:
    out, offset = {}, 0
    while True:
        r = await client.get(f"/api/migrations/{job_id}/members", headers=headers,
                             params={"limit": 200, "offset": offset, **params})
        page = r.json()
        for m in page["items"]:
            out[m["telegram_user_id"]] = m
        offset += len(page["items"])
        if offset >= page["total"] or not page["items"]:
            return out
