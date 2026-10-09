"""A stalled Telegram connection must never hang a request (found during a live run in a sandbox)."""
import time

import pytest
from sqlalchemy import select

from app.models import TelegramLoginAttempt
from tests.conftest import PHONE, discover, make_settings
from tests.test_eligibility import run


@pytest.fixture
def settings(db_url):
    return make_settings(db_url, telegram_login_timeout_seconds=0.3, telegram_operation_timeout_seconds=0.4)


async def _timed(coro):
    t0 = time.monotonic()
    r = await coro
    return r, time.monotonic() - t0


async def test_login_start_times_out_cleanly(client, headers, world, db):
    world.hang.add("send_code_request")
    r, took = await _timed(client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers))
    assert r.status_code == 503 and r.json()["error"]["code"] == "telegram_network_error"
    assert "did not respond in time" in r.json()["error"]["message"]
    assert took < 3
    assert (await db.execute(select(TelegramLoginAttempt))).first() is None   # nothing half-created


async def test_channel_discovery_times_out_cleanly(client, headers, account, seeded_world):
    seeded_world.hang.add("iter_dialogs")
    r, took = await _timed(client.get("/api/telegram/channels", headers=headers))
    assert r.status_code == 503 and took < 3


async def test_member_listing_times_out_cleanly(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    seeded_world.hang.add("iter_participants:members")
    r, took = await _timed(client.get(f"/api/telegram/channels/{channels['Source Channel']['id']}/members",
                                      headers=headers))
    assert r.status_code == 503 and took < 3


async def test_analysis_timeout_marks_job_failed_instead_of_leaving_it_analyzing(client, headers, account,
                                                                                  seeded_world):
    channels = await discover(client, headers)
    seeded_world.hang.add("get_participants")         # source listing stalls after the job row exists
    r, took = await _timed(run(client, headers, channels))
    assert r.status_code == 201 and took < 3
    job = r.json()
    assert job["analysis_state"] == "FAILED" and "did not respond in time" in job["analysis_note"]
    events = (await client.get(f"/api/migrations/{job['id']}/events", headers=headers)).json()
    assert [e["event_type"] for e in events] == ["ANALYSIS_STARTED", "ANALYSIS_FAILED"]


async def test_disconnect_still_deletes_locally_when_telegram_stalls(client, headers, account, world, db):
    world.hang.add("log_out")
    from tests.fakes import FakeClient

    async def slow_logout(self):
        await world.maybe_hang("log_out")
    original, FakeClient.log_out = FakeClient.log_out, slow_logout
    try:
        r, took = await _timed(client.delete(f"/api/telegram/accounts/{account['id']}", headers=headers))
    finally:
        FakeClient.log_out = original
    assert r.status_code == 204 and took < 3        # local session removed even though remote logout stalled
    assert (await client.get("/api/telegram/accounts", headers=headers)).json() == []
