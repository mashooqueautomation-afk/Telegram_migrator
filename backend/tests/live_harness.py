"""Run the REAL worker (arq + engine + Redis + PostgreSQL) against a FAKE Telegram, to watch a migration end to end
without touching a real account.

    arq tests.live_harness.WorkerSettings        # needs the same environment as the worker (see README)

Environment knobs (all optional):
    FAKE_MEMBERS=24        members in the source channel
    FAKE_FLOOD_AT=8        the Nth member triggers ONE FloodWait ...
    FAKE_FLOOD_SECONDS=3   ... of this many seconds
    FAKE_PRIVACY_EVERY=7   every Nth member refuses (USER_PRIVACY_RESTRICTED)
    FAKE_STATE=/tmp/x.log  invites that landed are appended here and re-loaded on restart, so a crash/restart test can
                           count duplicates across worker processes
"""
import os
from pathlib import Path

from app.core.config import get_settings
from app.workers.worker import worker_config
from tests.fakes import FakeFactory, FakeTelegram, flood, make_channel, make_user, rpc
from telethon import errors as tl_err

N = int(os.environ.get("FAKE_MEMBERS", "24"))
FLOOD_AT = int(os.environ.get("FAKE_FLOOD_AT", "0"))
FLOOD_SECONDS = int(os.environ.get("FAKE_FLOOD_SECONDS", "3"))
PRIVACY_EVERY = int(os.environ.get("FAKE_PRIVACY_EVERY", "0"))
STATE = os.environ.get("FAKE_STATE")
SESSION = "live-session-1"


def build_world() -> FakeTelegram:
    world = FakeTelegram()
    world.sessions[SESSION] = {"user": world.accounts["+14155550123"], "pending": None, "needs_password": False,
                               "revoked": False}
    users = [make_user(1000 + i, f"Member {i:03d}", username=f"m{i:03d}") for i in range(1, N + 1)]
    world.add_channel(make_channel(100, "Source Channel", creator=True), users + [make_user(777, "Ada")])
    world.add_channel(make_channel(200, "Destination Group", megagroup=True, admin=True), [make_user(777, "Ada")])
    if PRIVACY_EVERY:
        for i in range(PRIVACY_EVERY, N + 1, PRIVACY_EVERY):
            world.invite_behavior[1000 + i] = [rpc(tl_err.UserPrivacyRestrictedError)]
    if STATE and Path(STATE).exists():                      # a previous worker process already added these
        for line in Path(STATE).read_text().split():
            world.members[200].append(make_user(int(line), "Landed earlier"))
    if STATE:
        world.on_landed = lambda cid, uid: open(STATE, "a").write(f"{uid}\n")
    if FLOOD_AT:
        armed = {"done": False}

        async def maybe_flood(cid, uid):
            if uid == 1000 + FLOOD_AT and not armed["done"]:
                armed["done"] = True
                world.fail_next("InviteToChannelRequest", flood(FLOOD_SECONDS))
        world.on_invite = maybe_flood
    return world


WorkerSettings = type("WorkerSettings", (), worker_config(get_settings(), factory=FakeFactory(build_world())))
