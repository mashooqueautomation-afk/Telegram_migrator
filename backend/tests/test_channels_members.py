import pytest
from sqlalchemy import select

from app.domain import AccountStatus
from app.models import TelegramAccount, TelegramChannel
from app.telegram.channels import channel_info_from_entity
from app.telegram.members import raw_member_from_user
from tests.conftest import discover, register_and_login
from tests.fakes import flood, make_basic_group, make_channel, make_user, rpc
from telethon import errors as tl_err


# ------------------------------------------------------------- pure parsing
def test_channel_info_kinds_and_rights():
    ch = channel_info_from_entity(make_channel(1, "C", creator=True))
    assert ch.kind == "CHANNEL" and ch.is_admin and ch.can_invite_users
    sg = channel_info_from_entity(make_channel(2, "S", megagroup=True, admin=True, can_invite=False))
    assert sg.kind == "SUPERGROUP" and sg.is_admin and not sg.can_invite_users
    plain = channel_info_from_entity(make_channel(3, "P"))
    assert not plain.is_admin and not plain.can_invite_users
    grp = channel_info_from_entity(make_basic_group(4, "G", admin=True))
    assert grp.kind == "GROUP" and grp.is_admin


@pytest.mark.parametrize("entity", [make_user(1), make_channel(5, "left", left=True),
                                    make_basic_group(6, "dead", deactivated=True),
                                    make_basic_group(7, "upgraded", migrated=True)])
def test_non_usable_entities_are_skipped(entity):
    assert channel_info_from_entity(entity) is None


def test_raw_member_flags():
    m = raw_member_from_user(make_user(1, "A", last="B", username="ab", role="admin"))
    assert (m.display_name, m.role, m.has_usable_hash) == ("A B", "admin", True)
    assert raw_member_from_user(make_user(2, None, deleted=True)).display_name == "Deleted account"
    assert not raw_member_from_user(make_user(3, "x", minimal=True)).has_usable_hash
    assert raw_member_from_user(make_user(4, None)).display_name == "User 4"
    assert raw_member_from_user(object()) is None


# ------------------------------------------------------------- channel discovery API
async def test_discovers_only_administered_channels_by_default(client, headers, account, seeded_world):
    channels = await discover(client, headers)
    assert set(channels) == {"Source Channel", "Destination Group"}      # not "Someone Else's Channel", not DMs
    src = channels["Source Channel"]
    assert src["kind"] == "CHANNEL" and src["is_creator"] and src["username"] == "srcchan"
    dst = channels["Destination Group"]
    assert dst["kind"] == "SUPERGROUP" and dst["is_admin"] and dst["can_invite_users"]
    assert "access_hash" not in src


async def test_administered_only_false_includes_everything_visible(client, headers, account, seeded_world):
    r = await client.get("/api/telegram/channels?administered_only=false", headers=headers)
    assert {c["title"] for c in r.json()} == {"Source Channel", "Destination Group", "Someone Else's Channel"}


async def test_refresh_false_serves_cached_list_without_contacting_telegram(client, headers, account, seeded_world):
    await discover(client, headers)
    before = seeded_world.called("iter_dialogs")
    r = await client.get("/api/telegram/channels?refresh=false", headers=headers)
    assert len(r.json()) == 2 and seeded_world.called("iter_dialogs") == before


async def test_rediscovery_updates_rows_instead_of_duplicating(client, headers, account, seeded_world, db):
    await discover(client, headers)
    seeded_world.entities[100].title = "Renamed"
    await discover(client, headers)
    rows = (await db.execute(select(TelegramChannel))).scalars().all()
    assert len(rows) == 2 and "Renamed" in {r.title for r in rows}


async def test_channels_without_connected_account(client, headers):
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 404 and r.json()["error"]["code"] == "no_telegram_account"


async def test_channel_discovery_flood_wait_sets_cooldown_and_blocks_further_calls(client, headers, account,
                                                                                    seeded_world, db):
    seeded_world.fail_next("iter_dialogs", flood(300))
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 429 and r.json()["error"]["retry_after"] == 300
    row = (await db.execute(select(TelegramAccount))).scalar_one()
    assert row.flood_wait_until is not None
    calls_before = seeded_world.called("iter_dialogs")
    # while the cooldown runs we must NOT contact Telegram again
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 429 and 0 < int(r.headers["retry-after"]) <= 300
    assert seeded_world.called("iter_dialogs") == calls_before


async def test_short_flood_wait_is_waited_out_once_then_succeeds(client, headers, account, seeded_world, monkeypatch):
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("app.telegram.rate_limit.asyncio.sleep", fake_sleep)
    seeded_world.fail_next("iter_dialogs", flood(2))
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 200 and slept == [3]            # waited exactly requested+1s, retried once


async def test_repeated_short_flood_wait_is_surfaced(client, headers, account, seeded_world, monkeypatch):
    async def fake_sleep(s):
        pass
    monkeypatch.setattr("app.telegram.rate_limit.asyncio.sleep", fake_sleep)
    seeded_world.fail_next("iter_dialogs", flood(2), times=2)
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 429 and seeded_world.called("iter_dialogs") == 2     # no third attempt


async def test_revoked_session_marks_account_needs_reauth(client, headers, account, seeded_world, db):
    seeded_world.invalidate_all_sessions()
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 409 and r.json()["error"]["code"] == "telegram_session_invalid"
    assert (await db.execute(select(TelegramAccount))).scalar_one().status is AccountStatus.NEEDS_REAUTH
    again = await client.get("/api/telegram/channels", headers=headers)       # short-circuits, no Telegram call
    assert again.status_code == 409


async def test_generic_telegram_rpc_error_is_502(client, headers, account, seeded_world):
    seeded_world.fail_next("iter_dialogs", rpc(tl_err.RPCError, ))
    r = await client.get("/api/telegram/channels", headers=headers)
    assert r.status_code == 502 and r.json()["error"]["code"] == "telegram_api_error"


# ------------------------------------------------------------- live member listing API
async def test_member_listing_complete(client, headers, account, seeded_world):
    ch = (await discover(client, headers))["Source Channel"]
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    assert r.status_code == 200
    body = r.json()
    assert body["returned"] == 8 and body["reported_total"] == 8 and body["complete"] is True
    by_id = {m["telegram_user_id"]: m for m in body["members"]}
    assert by_id[4]["is_bot"] and by_id[5]["is_deleted"] and by_id[6]["is_restricted"]
    assert by_id[777]["role"] == "creator"
    assert "access_hash" not in r.text and "phone" not in r.text
    assert ("iter_participants", "members", False) in seeded_world.calls   # never aggressive=True
    assert not any(c[0] == "iter_participants" and c[2] for c in seeded_world.calls)


async def test_member_listing_truncated_by_our_limit_is_flagged(client, headers, account, seeded_world):
    ch = (await discover(client, headers))["Source Channel"]
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members?limit=3", headers=headers)
    body = r.json()
    assert body["returned"] == 3 and body["complete"] is False and body["incomplete_reason"] == "member_limit_reached"
    assert "incomplete" in body["notes"][0]


async def test_member_listing_telegram_partial_list_is_flagged_not_worked_around(client, headers, account,
                                                                                  seeded_world):
    seeded_world.totals[100] = 50_000      # Telegram says 50k members but only returns 8
    ch = (await discover(client, headers))["Source Channel"]
    body = (await client.get(f"/api/telegram/channels/{ch['id']}/members?limit=1000", headers=headers)).json()
    assert body["returned"] == 8 and body["reported_total"] == 50_000
    assert body["complete"] is False and body["incomplete_reason"] == "telegram_partial_list"


async def test_member_listing_flood_wait_midway_returns_partial_and_cooldown(client, headers, account,
                                                                              seeded_world, db):
    seeded_world.iter_fail_after[(100, "members")] = (3, flood(90))
    ch = (await discover(client, headers))["Source Channel"]
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    body = r.json()
    assert r.status_code == 200 and body["returned"] == 3
    assert body["complete"] is False and body["incomplete_reason"] == "rate_limited"
    assert body["rate_limited_for_seconds"] == 90
    assert (await db.execute(select(TelegramAccount))).scalar_one().flood_wait_until is not None
    assert (await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)).status_code == 429


async def test_member_listing_requires_admin_rights_at_request_time(client, headers, account, seeded_world, db):
    ch = (await discover(client, headers))["Source Channel"]
    seeded_world.entities[100] = make_channel(100, "Source Channel")     # rights revoked since discovery
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    assert r.status_code == 403 and r.json()["error"]["code"] == "not_admin"
    assert (await db.execute(select(TelegramChannel).where(TelegramChannel.telegram_id == 100))).scalar_one().is_admin is False


async def test_member_listing_admin_required_error_from_telegram(client, headers, account, seeded_world):
    ch = (await discover(client, headers))["Source Channel"]
    seeded_world.fail_next("get_participants", rpc(tl_err.ChatAdminRequiredError))
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    assert r.status_code == 403 and r.json()["error"]["code"] == "admin_permission_required"


async def test_private_or_removed_channel_reports_inaccessible(client, headers, account, seeded_world):
    ch = (await discover(client, headers))["Source Channel"]
    del seeded_world.entities[100]      # we were removed -> Telegram returns ChannelForbidden
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    assert r.status_code == 403 and r.json()["error"]["code"] == "channel_inaccessible"
    seeded_world.entities[100] = make_channel(100, "x", creator=True)
    seeded_world.fail_next("get_entity", rpc(tl_err.ChannelPrivateError))
    r = await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=headers)
    assert r.json()["error"]["code"] == "channel_inaccessible"


async def test_unknown_channel_id_is_404_and_other_users_cannot_read_it(client, headers, account, seeded_world):
    import uuid
    assert (await client.get(f"/api/telegram/channels/{uuid.uuid4()}/members", headers=headers)).status_code == 404
    ch = (await discover(client, headers))["Source Channel"]
    other = await register_and_login(client)
    assert (await client.get(f"/api/telegram/channels/{ch['id']}/members", headers=other)).status_code == 404
