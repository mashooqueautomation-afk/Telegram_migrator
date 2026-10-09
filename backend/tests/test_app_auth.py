import pytest

from app.core.security import create_access_token
from tests.conftest import PASSWORD, make_settings, register_and_login
from app.main import create_app
from tests.fakes import FakeFactory
import httpx


async def test_register_login_me(client):
    h = await register_and_login(client, "Me@Example.com")
    r = await client.get("/api/auth/me", headers=h)
    assert r.status_code == 200 and r.json()["email"] == "me@example.com"
    assert "password" not in r.text


async def test_duplicate_email_conflict(client):
    await register_and_login(client, "dup@example.com")
    r = await client.post("/api/auth/register", json={"email": "DUP@example.com", "password": PASSWORD})
    assert r.status_code == 409 and r.json()["error"]["code"] == "email_taken"


async def test_weak_password_rejected(client):
    r = await client.post("/api/auth/register", json={"email": "a@example.com", "password": "short"})
    assert r.status_code == 422
    assert "short" not in r.text  # submitted value is never echoed back


async def test_login_wrong_password_and_unknown_user_look_identical(client):
    await register_and_login(client, "real@example.com")
    bad_pw = await client.post("/api/auth/login", json={"email": "real@example.com", "password": "nope-nope-nope"})
    unknown = await client.post("/api/auth/login", json={"email": "ghost@example.com", "password": "nope-nope-nope"})
    assert bad_pw.status_code == unknown.status_code == 401
    assert bad_pw.json() == unknown.json()


@pytest.mark.parametrize("auth", [None, "Bearer garbage"])
async def test_protected_routes_require_valid_token(client, auth):
    hdrs = {"Authorization": auth} if auth else {}
    for method, path in [("get", "/api/auth/me"), ("get", "/api/telegram/channels"), ("get", "/api/migrations"),
                         ("post", "/api/auth/telegram/start")]:
        r = await getattr(client, method)(path, headers=hdrs)
        assert r.status_code == 401, (path, r.text)


async def test_expired_token_rejected(client, settings):
    await register_and_login(client, "exp@example.com")
    me = (await client.get("/api/auth/me", headers=await register_and_login(client))).json()
    import uuid
    token, _ = create_access_token(user_id=uuid.UUID(me["id"]), secret=settings.jwt_secret.get_secret_value(),
                                   algorithm="HS256", ttl_minutes=-1)
    r = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401 and r.json()["error"]["code"] == "invalid_token"


async def test_registration_can_be_disabled(schema, db_url, world):
    app = create_app(make_settings(db_url, allow_registration=False), client_factory=FakeFactory(world))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/auth/register", json={"email": "x@example.com", "password": PASSWORD})
    await app.state.engine.dispose()
    assert r.status_code == 403 and r.json()["error"]["code"] == "registration_disabled"


async def test_auth_endpoints_are_throttled(schema, db_url, world):
    app = create_app(make_settings(db_url, auth_rate_limit_per_minute=3), client_factory=FakeFactory(world))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        codes = [(await c.post("/api/auth/login", json={"email": "a@example.com", "password": "whatever-1"}))
                 for _ in range(5)]
    await app.state.engine.dispose()
    assert [r.status_code for r in codes] == [401, 401, 401, 429, 429]
    assert int(codes[3].headers["Retry-After"]) > 0


async def test_health_docs_and_security_headers(client):
    assert (await client.get("/api/health")).json() == {"status": "ok"}
    assert (await client.get("/api/health/ready")).json() == {"status": "ready"}
    docs = await client.get("/docs")
    assert docs.status_code == 200 and "swagger" in docs.text.lower()
    spec = (await client.get("/openapi.json")).json()
    assert "/api/auth/telegram/start" in spec["paths"] and "/api/migrations/eligibility" in spec["paths"]
    r = await client.get("/api/health")
    assert r.headers["x-content-type-options"] == "nosniff" and r.headers["cache-control"] == "no-store"
