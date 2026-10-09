import asyncio
import logging
import uuid

import pytest
from sqlalchemy import select, text

from app.models import AuditLog, TelegramAccount, TelegramLoginAttempt
from app.telegram.auth import mask_phone, normalize_code, normalize_phone, request_login_code
from app.telegram.errors import TelegramApiError
from tests.conftest import PHONE, connect_telegram, register_and_login
from tests.fakes import FakeFactory, FakeTelegram, flood, rpc
from telethon import errors as tl_err

CODE = "12345"


# ------------------------------------------------------------- pure helpers
def test_phone_and_code_normalisation():
    assert normalize_phone("+1 (415) 555-0123") == "+14155550123"
    for bad in ["4155550123", "+12", "+1abc5550123", ""]:
        with pytest.raises(ValueError):
            normalize_phone(bad)
    assert normalize_code("1 2 3-4 5") == "12345"
    with pytest.raises(ValueError):
        normalize_code("12")
    masked = mask_phone("+14155550123")
    assert masked.endswith("23") and "4155550" not in masked


async def test_unexpected_send_code_response_is_an_error():
    world = FakeTelegram()
    factory = FakeFactory(world)

    async def weird(self, phone, force_sms=False):
        from types import SimpleNamespace
        return SimpleNamespace()  # no phone_code_hash (e.g. an unexpected SentCodeSuccess)

    from tests.fakes import FakeClient
    original = FakeClient.send_code_request
    FakeClient.send_code_request = weird
    try:
        with pytest.raises(TelegramApiError):
            await request_login_code(factory, PHONE)
    finally:
        FakeClient.send_code_request = original


# ------------------------------------------------------------- full flow
async def test_successful_login_flow_stores_only_encrypted_session(client, headers, world, db, app):
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert r.status_code == 200
    body = r.json()
    assert body["delivery"] == "telegram_app" and body["phone_masked"].endswith("23")
    assert PHONE not in r.text

    r = await client.post("/api/auth/telegram/verify", json={"login_id": body["login_id"], "code": CODE},
                          headers=headers)
    assert r.status_code == 200 and r.json()["status"] == "connected"
    acct = r.json()["account"]
    assert acct["telegram_user_id"] == 777 and acct["username"] == "ada_admin"
    assert "session" not in r.text.lower() and CODE not in r.text

    row = (await db.execute(select(TelegramAccount))).scalar_one()
    real_session = next(t for t, s in world.sessions.items() if s["user"])
    assert real_session not in row.session_encrypted                       # not plaintext
    assert app.state.vault.decrypt(row.session_encrypted) == real_session  # but recoverable with the key
    assert (await db.execute(select(TelegramLoginAttempt))).first() is None  # pending attempt cleaned up


async def test_no_credentials_persisted_anywhere_in_db(client, headers, world, db):
    """Scan every text column of every table for the phone, the login code and the 2FA password."""
    world.accounts[PHONE]["password"] = "hunter2-2fa-secret"
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    login_id = r.json()["login_id"]
    await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    r = await client.post("/api/auth/telegram/2fa", json={"login_id": login_id, "password": "hunter2-2fa-secret"},
                          headers=headers)
    assert r.json()["status"] == "connected"

    tables = [t for (t,) in (await db.execute(text(
        "SELECT name FROM sqlite_master WHERE type='table'" if db.bind.dialect.name == "sqlite"
        else "SELECT tablename FROM pg_tables WHERE schemaname='public'"))).all()]
    dump = ""
    for t in tables:
        if t == "alembic_version":
            continue
        for row in (await db.execute(text(f'SELECT * FROM "{t}"'))).all():
            dump += " ".join(str(c) for c in row) + "\n"
    real_session = next(t for t, s in world.sessions.items() if s["user"])
    for secret in (PHONE, PHONE[1:], "hunter2-2fa-secret", real_session):
        assert secret not in dump, f"{secret!r} found in database in plaintext"
    # the code is a short number; check it only as a whole word in the audit trail / attempts
    audits = (await db.execute(select(AuditLog))).scalars().all()
    assert all(CODE not in str(a.details) for a in audits)


async def test_no_secrets_in_logs(client, headers, world, caplog):
    caplog.set_level(logging.DEBUG)
    world.accounts[PHONE]["password"] = "hunter2-2fa-secret"
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    login_id = r.json()["login_id"]
    await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    await client.post("/api/auth/telegram/2fa", json={"login_id": login_id, "password": "hunter2-2fa-secret"},
                      headers=headers)
    logged = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "hunter2-2fa-secret" not in logged and PHONE not in logged


async def test_reconnecting_same_telegram_account_updates_instead_of_duplicating(client, headers, world, db):
    await connect_telegram(client, headers)
    await connect_telegram(client, headers)
    assert len((await db.execute(select(TelegramAccount))).scalars().all()) == 1


# ------------------------------------------------------------- failures
async def test_invalid_code_then_lockout_after_max_attempts(client, headers, world, db):
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]
    statuses = []
    for _ in range(5):
        r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": "00000"},
                              headers=headers)
        statuses.append((r.status_code, r.json()["error"]["code"]))
    assert statuses[:4] == [(400, "invalid_login_code")] * 4
    assert statuses[4] == (429, "too_many_login_attempts")
    assert (await db.execute(select(TelegramLoginAttempt))).first() is None   # attempt destroyed
    # even the right code can't be used on a destroyed attempt
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    assert r.status_code == 404 and r.json()["error"]["code"] == "login_not_found"


async def test_expired_code_destroys_attempt(client, headers, world, db):
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]
    world.fail_next("sign_in", rpc(tl_err.PhoneCodeExpiredError))
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    assert r.status_code == 400 and r.json()["error"]["code"] == "login_code_expired"
    assert (await db.execute(select(TelegramLoginAttempt))).first() is None


async def test_login_attempt_times_out_locally(client, headers, db):
    from datetime import timedelta
    from app.db.base import utcnow
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]
    attempt = (await db.execute(select(TelegramLoginAttempt))).scalar_one()
    attempt.expires_at = utcnow() - timedelta(seconds=1)
    await db.commit()
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    assert r.status_code == 410 and r.json()["error"]["code"] == "login_expired"


async def test_two_factor_flow(client, headers, world, db):
    world.accounts[PHONE]["password"] = "hunter2-2fa-secret"
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]

    # 2FA before the code step is rejected
    r = await client.post("/api/auth/telegram/2fa", json={"login_id": login_id, "password": "x"}, headers=headers)
    assert r.status_code == 409 and r.json()["error"]["code"] == "login_wrong_step"

    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    assert r.status_code == 200
    assert r.json() == {"status": "two_factor_required", "login_id": login_id, "account": None, "next_step": "2fa"}
    assert (await db.execute(select(TelegramAccount))).first() is None     # not connected yet

    # re-submitting the code is the wrong step now
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    assert r.status_code == 409

    r = await client.post("/api/auth/telegram/2fa", json={"login_id": login_id, "password": "wrong"}, headers=headers)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_two_factor_password"

    r = await client.post("/api/auth/telegram/2fa",
                          json={"login_id": login_id, "password": "hunter2-2fa-secret"}, headers=headers)
    assert r.status_code == 200 and r.json()["status"] == "connected"
    assert "hunter2" not in r.text


async def test_two_factor_password_attempt_limit(client, headers, world, db):
    world.accounts[PHONE]["password"] = "right"
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]
    await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=headers)
    codes = []
    for _ in range(5):
        r = await client.post("/api/auth/telegram/2fa", json={"login_id": login_id, "password": "wrong"},
                              headers=headers)
        codes.append(r.status_code)
    assert codes == [400, 400, 400, 400, 429]
    assert (await db.execute(select(TelegramLoginAttempt))).first() is None


async def test_unknown_phone_rejected_by_telegram(client, headers, world):
    r = await client.post("/api/auth/telegram/start", json={"phone": "+14155559999"}, headers=headers)
    assert r.status_code == 400 and r.json()["error"]["code"] == "phone_number_invalid"


async def test_malformed_phone_and_code_fail_validation_without_echo(client, headers):
    r = await client.post("/api/auth/telegram/start", json={"phone": "12345"}, headers=headers)
    assert r.status_code == 422 and "12345" not in r.text
    r = await client.post("/api/auth/telegram/verify", json={"login_id": str(uuid.uuid4()), "code": "9"},
                          headers=headers)
    assert r.status_code == 422


async def test_flood_wait_on_send_code_is_surfaced_not_bypassed(client, headers, world):
    world.fail_next("send_code_request", flood(120))
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert r.status_code == 429
    assert r.headers["retry-after"] == "120"
    err = r.json()["error"]
    assert err["code"] == "telegram_flood_wait" and err["retry_after"] == 120
    assert "cooldown of 120 seconds" in err["message"]
    assert world.called("send_code_request") == 1       # no automatic retry for a long wait


async def test_invalid_api_credentials_report_server_misconfiguration(client, headers, world):
    world.fail_next("send_code_request", rpc(tl_err.ApiIdInvalidError))
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert r.status_code == 500 and r.json()["error"]["code"] == "telegram_not_configured"


async def test_network_failure_maps_to_503(client, headers, world):
    world.connect_error = ConnectionError("boom")
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert r.status_code == 503 and r.json()["error"]["code"] == "telegram_network_error"


async def test_login_attempts_are_private_to_their_owner(client, headers, world):
    login_id = (await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)).json()["login_id"]
    other = await register_and_login(client)
    r = await client.post("/api/auth/telegram/verify", json={"login_id": login_id, "code": CODE}, headers=other)
    assert r.status_code == 404


async def test_starting_a_new_login_replaces_the_old_one(client, headers, db):
    await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert len((await db.execute(select(TelegramLoginAttempt))).scalars().all()) == 1


async def test_disconnect_logs_out_remotely_and_deletes_session(client, headers, account, world, db):
    r = await client.delete(f"/api/telegram/accounts/{account['id']}", headers=headers)
    assert r.status_code == 204
    assert len(world.logged_out) == 1
    assert (await db.execute(select(TelegramAccount))).first() is None
    assert (await client.get("/api/telegram/accounts", headers=headers)).json() == []
    actions = [a.action for a in (await db.execute(select(AuditLog))).scalars().all()]
    assert "telegram.account_disconnected" in actions


async def test_cannot_disconnect_someone_elses_account(client, headers, account):
    other = await register_and_login(client)
    r = await client.delete(f"/api/telegram/accounts/{account['id']}", headers=other)
    assert r.status_code == 404
    assert (await client.delete("/api/telegram/accounts/not-a-uuid", headers=headers)).status_code == 404


# ------------------------------------------------------------- transport-level failures (found in a live run)
@pytest.mark.parametrize("exc", [
    asyncio.IncompleteReadError(b"abc", 1347703872),   # e.g. an egress proxy answering with HTTP instead of MTProto
    EOFError(), asyncio.TimeoutError(), ConnectionResetError(), OSError("network unreachable"),
    tl_err.InvalidBufferError(b"\xf3\xff\xff\xff"), tl_err.InvalidChecksumError(1, 2),
])
async def test_transport_failures_map_to_503_not_500(client, headers, world, exc):
    world.connect_error = exc
    r = await client.post("/api/auth/telegram/start", json={"phone": PHONE}, headers=headers)
    assert r.status_code == 503, r.text
    assert r.json()["error"]["code"] == "telegram_network_error"
