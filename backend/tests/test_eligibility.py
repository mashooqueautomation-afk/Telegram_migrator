import uuid

from sqlalchemy import select

from app.domain import Eligibility, MemberState
from app.models import MigrationEvent, MigrationMember
from app.services.eligibility import DestinationSnapshot, classify, initial_state_for
from app.telegram.members import raw_member_from_user
from tests.conftest import discover, register_and_login
from tests.fakes import flood, make_channel, make_user, rpc
from telethon import errors as tl_err


def raw(**kw):
    return raw_member_from_user(make_user(kw.pop("id", 1), **kw))


FULL = DestinationSnapshot(member_ids=frozenset({3}), banned_ids=frozenset({8}), membership_complete=True,
                           banned_known=True)
INCOMPLETE = DestinationSnapshot(membership_complete=False)


# ------------------------------------------------------------- pure rules
def test_classification_table():
    assert classify(raw(id=3), FULL).eligibility is Eligibility.ALREADY_IN_DESTINATION
    assert classify(raw(first=None, deleted=True), FULL).eligibility is Eligibility.DELETED_ACCOUNT
    assert classify(raw(bot=True), FULL).eligibility is Eligibility.BOT
    assert classify(raw(restricted=True), FULL).eligibility is Eligibility.RESTRICTED
    banned = classify(raw(id=8), FULL)
    assert banned.eligibility is Eligibility.RESTRICTED and "destination" in banned.reason.lower()
    assert classify(raw(minimal=True), FULL).eligibility is Eligibility.UNKNOWN
    ok = classify(raw(), FULL)
    assert ok.eligibility is Eligibility.ELIGIBLE and "may still be refused" in ok.reason


def test_never_eligible_without_a_complete_destination_list():
    result = classify(raw(), INCOMPLETE)
    assert result.eligibility is Eligibility.UNKNOWN
    # ...but definite blockers still win even when the list is incomplete
    assert classify(raw(bot=True), INCOMPLETE).eligibility is Eligibility.BOT
    assert classify(raw(id=3), DestinationSnapshot(member_ids=frozenset({3}))).eligibility \
        is Eligibility.ALREADY_IN_DESTINATION


def test_privacy_restricted_is_never_produced_by_analysis():
    members = [raw(id=i, **kw) for i, kw in enumerate(
        [{}, {"bot": True}, {"deleted": True}, {"restricted": True}, {"minimal": True}], start=10)]
    assert Eligibility.PRIVACY_RESTRICTED not in {classify(m, FULL).eligibility for m in members}


def test_initial_states():
    assert initial_state_for(Eligibility.ELIGIBLE) is MemberState.ELIGIBLE
    assert initial_state_for(Eligibility.UNKNOWN) is MemberState.DISCOVERED
    assert initial_state_for(Eligibility.BOT) is MemberState.SKIPPED


# ------------------------------------------------------------- end to end through the API
async def run(client, headers, channels, **extra):
    body = {"source_channel_id": channels["Source Channel"]["id"],
            "destination_channel_id": channels["Destination Group"]["id"], **extra}
    return await client.post("/api/migrations/eligibility", json=body, headers=headers)


async def test_full_flow_eligibility_analysis(client, headers, account, seeded_world, db):
    channels = await discover(client, headers)
    r = await run(client, headers, channels)
    assert r.status_code == 201, r.text
    job = r.json()
    assert job["analysis_state"] == "COMPLETED" and job["status"] == "PENDING"
    assert job["source_listing_complete"] and job["destination_listing_complete"]
    assert job["summary"]["total_analyzed"] == 8
    assert job["summary"]["by_eligibility"] == {
        "ELIGIBLE": 2,                  # Alice, Bob
        "ALREADY_IN_DESTINATION": 2,    # Carol, Ada (the admin account itself)
        "PRIVACY_RESTRICTED": 0,        # only knowable after an actual invite attempt (Stage 2)
        "DELETED_ACCOUNT": 1, "BOT": 1, "RESTRICTED": 1,
        "UNKNOWN": 1,                   # Gina: Telegram gave a partial stub
    }
    assert any("may still be refused" in n for n in job["notes"])
    assert job["source"]["title"] == "Source Channel"

    # No invitation is ever sent in Stage 1: the fake has no invite method at all, and no job moved past PENDING.
    got = (await client.get(f"/api/migrations/{job['id']}", headers=headers)).json()
    assert got == job

    page = (await client.get(f"/api/migrations/{job['id']}/members?eligibility=ELIGIBLE", headers=headers)).json()
    assert page["total"] == 2 and {m["display_name"] for m in page["items"]} == {"Alice", "Bob"}
    assert all(m["state"] == "ELIGIBLE" for m in page["items"])
    assert "access_hash" not in str(page)
    everyone = (await client.get(f"/api/migrations/{job['id']}/members?limit=3&offset=3", headers=headers)).json()
    assert everyone["total"] == 8 and len(everyone["items"]) == 3
    found = (await client.get(f"/api/migrations/{job['id']}/members?search=ali", headers=headers)).json()
    assert [m["username"] for m in found["items"]] == ["alice"]
    pct = (await client.get(f"/api/migrations/{job['id']}/members?search=%25", headers=headers)).json()
    assert pct["total"] == 0                                            # wildcard is escaped, not injected

    events = (await client.get(f"/api/migrations/{job['id']}/events", headers=headers)).json()
    assert [e["event_type"] for e in events] == ["ANALYSIS_STARTED", "ANALYSIS_COMPLETED"]

    listed = (await client.get("/api/migrations", headers=headers)).json()
    assert listed["total"] == 1 and listed["items"][0]["id"] == job["id"]


async def test_incomplete_destination_listing_downgrades_eligible_to_unknown(client, headers, account, seeded_world):
    seeded_world.totals[200] = 9_999          # Telegram says the destination is bigger than what it returned
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    assert job["analysis_state"] == "PARTIAL" and job["destination_listing_complete"] is False
    by = job["summary"]["by_eligibility"]
    assert by["ELIGIBLE"] == 0 and by["UNKNOWN"] == 3          # Alice, Bob, Gina
    assert by["ALREADY_IN_DESTINATION"] == 2 and by["BOT"] == 1   # still-certain facts survive
    assert any("PARTIAL" in n for n in job["notes"])
    events = (await client.get(f"/api/migrations/{job['id']}/events", headers=headers)).json()
    assert "DISCOVERY_TRUNCATED" in {e["event_type"] for e in events}


async def test_member_limit_makes_source_partial(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    job = (await run(client, headers, channels, member_limit=3)).json()
    assert job["member_limit"] == 3 and job["summary"]["total_analyzed"] == 3
    assert job["analysis_state"] == "PARTIAL" and job["source_listing_complete"] is False


async def test_member_limit_is_clamped_to_server_maximum(client, headers, account, seeded_world, settings):
    channels = await discover(client, headers)
    job = (await run(client, headers, channels, member_limit=10**9)).json()
    assert job["member_limit"] == settings.eligibility_max_limit


async def test_banned_in_destination_is_restricted(client, headers, account, seeded_world):
    seeded_world.banned[200] = [make_user(2, "Bob")]
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    assert job["destination_banned_known"] is True
    assert job["summary"]["by_eligibility"]["RESTRICTED"] == 2      # Frank + banned Bob
    assert job["summary"]["by_eligibility"]["ELIGIBLE"] == 1        # only Alice


async def test_unreadable_banned_list_is_noted_but_not_fatal(client, headers, account, seeded_world):
    seeded_world.fail_next("iter_participants:banned", rpc(tl_err.ChatAdminRequiredError))
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    assert job["destination_banned_known"] is False and job["analysis_state"] == "COMPLETED"
    assert any("banned list could not be read" in n for n in job["notes"])


async def test_flood_wait_during_source_stops_everything_and_records_cooldown(client, headers, account,
                                                                               seeded_world):
    seeded_world.iter_fail_after[(100, "members")] = (4, flood(600))
    channels = await discover(client, headers)
    dest_calls = lambda: sum(1 for c in seeded_world.calls if c[0] == "iter_participants")  # noqa: E731
    r = await run(client, headers, channels)
    job = r.json()
    assert r.status_code == 201 and job["analysis_state"] == "PARTIAL"
    assert job["summary"]["total_analyzed"] == 4 and job["rate_limited_until"] is not None
    assert "cooldown of 600 seconds" in job["analysis_note"]
    assert dest_calls() == 1       # destination was NOT queried after Telegram said slow down
    by = job["summary"]["by_eligibility"]
    assert by["ELIGIBLE"] == 0     # nothing is declared eligible against an unread destination
    events = (await client.get(f"/api/migrations/{job['id']}/events", headers=headers)).json()
    assert "DISCOVERY_RATE_LIMITED" in {e["event_type"] for e in events}
    # and the account is now in cooldown: a new analysis is refused without touching Telegram
    r = await run(client, headers, channels)
    assert r.status_code == 429 and int(r.headers["retry-after"]) > 0


async def test_flood_wait_during_destination_listing(client, headers, account, seeded_world):
    seeded_world.iter_fail_after[(200, "members")] = (1, flood(45))
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    assert job["analysis_state"] == "PARTIAL" and job["rate_limited_until"]
    assert job["summary"]["by_eligibility"]["ELIGIBLE"] == 0
    assert job["summary"]["total_analyzed"] == 8      # full source was read before the flood


async def test_telegram_error_mid_analysis_persists_failed_job(client, headers, account, seeded_world, db):
    seeded_world.fail_next("iter_participants:members", rpc(tl_err.RPCError))   # source listing blows up
    channels = await discover(client, headers)
    r = await run(client, headers, channels)
    assert r.status_code == 201 and r.json()["analysis_state"] == "FAILED"
    assert r.json()["summary"]["total_analyzed"] == 0
    events = (await client.get(f"/api/migrations/{r.json()['id']}/events", headers=headers)).json()
    assert [e["event_type"] for e in events] == ["ANALYSIS_STARTED", "ANALYSIS_FAILED"]


async def test_session_revoked_mid_analysis(client, headers, account, seeded_world):
    seeded_world.fail_next("iter_participants:members", rpc(tl_err.AuthKeyUnregisteredError))
    channels = await discover(client, headers)
    r = await run(client, headers, channels)
    assert r.status_code == 409 and r.json()["error"]["code"] == "telegram_session_invalid"
    listing = (await client.get("/api/migrations", headers=headers)).json()
    assert listing["items"][0]["analysis_state"] == "FAILED"


# ------------------------------------------------------------- authorisation / validation
async def test_destination_without_invite_right_is_refused(client, headers, account, seeded_world):
    seeded_world.entities[200] = make_channel(200, "Destination Group", megagroup=True, admin=True, can_invite=False)
    channels = await discover(client, headers)
    r = await run(client, headers, channels)
    assert r.status_code == 403 and r.json()["error"]["code"] == "destination_cannot_invite"
    assert (await client.get("/api/migrations", headers=headers)).json()["total"] == 0   # no job created


async def test_source_not_admin_anymore_is_refused(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    seeded_world.entities[100] = make_channel(100, "Source Channel")
    r = await run(client, headers, channels)
    assert r.status_code == 403 and r.json()["error"]["code"] == "source_not_admin"


async def test_same_channel_and_unknown_channel_rejected(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    sid = channels["Source Channel"]["id"]
    r = await client.post("/api/migrations/eligibility",
                          json={"source_channel_id": sid, "destination_channel_id": sid}, headers=headers)
    assert r.status_code == 400 and r.json()["error"]["code"] == "same_channel"
    r = await client.post("/api/migrations/eligibility",
                          json={"source_channel_id": sid, "destination_channel_id": str(uuid.uuid4())},
                          headers=headers)
    assert r.status_code == 404


async def test_jobs_are_private_to_their_owner_and_deletable(client, headers, account, seeded_world, db):
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    other = await register_and_login(client)
    for path in ("", "/members", "/events"):
        assert (await client.get(f"/api/migrations/{job['id']}{path}", headers=other)).status_code == 404
    assert (await client.delete(f"/api/migrations/{job['id']}", headers=other)).status_code == 404
    # another user cannot analyse using my channel ids either
    r = await client.post("/api/migrations/eligibility", json={
        "source_channel_id": channels["Source Channel"]["id"],
        "destination_channel_id": channels["Destination Group"]["id"]}, headers=other)
    assert r.status_code == 404

    assert (await client.delete(f"/api/migrations/{job['id']}", headers=headers)).status_code == 204
    assert (await client.get(f"/api/migrations/{job['id']}", headers=headers)).status_code == 404
    assert (await db.execute(select(MigrationMember))).first() is None      # members erased with the job
    assert (await db.execute(select(MigrationEvent))).first() is None


async def test_invalid_eligibility_filter_is_422(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    job = (await run(client, headers, channels)).json()
    r = await client.get(f"/api/migrations/{job['id']}/members?eligibility=SUPER_ELIGIBLE", headers=headers)
    assert r.status_code == 422
