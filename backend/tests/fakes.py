"""A scriptable fake of the Telethon surface this project uses.

It raises REAL Telethon error classes and builds REAL Telethon TL types, so the
error-translation and entity-parsing code under test is exercised for real.
No network, no Telegram credentials.
"""
from __future__ import annotations

import asyncio
import itertools
from datetime import datetime, timezone
from types import SimpleNamespace

from telethon import errors as tl_err
from telethon.helpers import TotalList
from telethon.tl import types


# ----------------------------------------------------------------- builders
def make_channel(id: int, title: str, *, megagroup: bool = False, creator: bool = False, admin: bool = False,
                 can_invite: bool = True, username: str | None = None, left: bool = False,
                 participants_count: int | None = None) -> types.Channel:
    rights = None
    if admin and not creator:
        rights = types.ChatAdminRights(invite_users=can_invite, change_info=True)
    return types.Channel(id=id, title=title, photo=types.ChatPhotoEmpty(), date=datetime.now(timezone.utc),
                         creator=creator or None, broadcast=not megagroup, megagroup=megagroup or None,
                         left=left or None, access_hash=1000 + id, username=username, admin_rights=rights,
                         participants_count=participants_count)


def make_basic_group(id: int, title: str, *, creator: bool = False, admin: bool = False,
                     deactivated: bool = False, migrated: bool = False) -> types.Chat:
    return types.Chat(id=id, title=title, photo=types.ChatPhotoEmpty(), participants_count=5,
                      date=datetime.now(timezone.utc), version=1, creator=creator or None,
                      deactivated=deactivated or None,
                      migrated_to=types.InputChannel(99, 1) if migrated else None,
                      admin_rights=types.ChatAdminRights(invite_users=True) if admin and not creator else None)


def make_user(id: int, first: str | None = "User", *, last: str | None = None, username: str | None = None,
              bot: bool = False, deleted: bool = False, restricted: bool = False, minimal: bool = False,
              role: str | None = None) -> types.User:
    u = types.User(id=id, first_name=first, last_name=last, username=username, bot=bot or None,
                   deleted=deleted or None, restricted=restricted or None, min=minimal or None,
                   access_hash=None if minimal else 5000 + id)
    if role == "admin":
        u.participant = types.ChannelParticipantAdmin(user_id=id, promoted_by=1,
                                                      date=datetime.now(timezone.utc),
                                                      admin_rights=types.ChatAdminRights())
    elif role == "creator":
        u.participant = types.ChannelParticipantCreator(user_id=id, admin_rights=types.ChatAdminRights())
    elif role == "member":
        u.participant = types.ChannelParticipant(user_id=id, date=datetime.now(timezone.utc))
    return u


def flood(seconds: int) -> tl_err.FloodWaitError:
    return tl_err.FloodWaitError(request=None, capture=seconds)


def rpc(cls, *a):
    if cls is tl_err.RPCError:  # the bare base class needs an explicit message/code
        return cls(request=None, message="UNEXPECTED_ERROR", code=500)
    return cls(request=None, *a)


# ----------------------------------------------------------------- world
class FakeTelegram:
    """Shared state behind every client the factory hands out."""

    def __init__(self) -> None:
        self.accounts: dict[str, dict] = {
            "+14155550123": {"id": 777, "first_name": "Ada", "username": "ada_admin",
                             "code": "12345", "password": None},
        }
        self.sessions: dict[str, dict] = {}
        self.entities: dict[int, object] = {}          # channel/group id -> TL entity
        self.members: dict[int, list] = {}             # channel id -> users returned by iter_participants
        self.banned: dict[int, list] = {}
        self.totals: dict[int, int | None] = {}        # override of the reported total
        self.script: dict[str, list] = {}              # method -> queue of exceptions to raise (one per call)
        self.iter_fail_after: dict[tuple[int, str], tuple[int, Exception]] = {}
        self.connect_error: Exception | None = None
        # ---- Stage 2: invitations. behaviour queues are consumed one entry per call; empty = normal success.
        #   entry: an Exception instance (raised) | "noop" (accepted but nobody is added) | "missing" (missing_invitees)
        self.invite_behavior: dict[int, list] = {}
        self.invite_calls: list[tuple[int, int]] = []        # every InviteToChannel/AddChatUser attempt (channel, user)
        self.invited: list[tuple[int, int]] = []             # attempts that really added the user
        self.on_invite = None                                # optional async callback(channel_id, user_id) mid-invite
        self.on_landed = None                                # optional sync callback(channel_id, user_id) after an add
        self.inflight = 0
        self.max_inflight = 0
        self.invite_delay = 0.0
        self.hang: set[str] = set()                    # methods that never answer (half-open connection)
        self.calls: list[tuple] = []
        self.logged_out: list[str] = []
        self.code_hash = "hash-abc"
        self._ids = itertools.count(1)

    # --- scripting helpers
    def fail_next(self, method: str, exc: Exception, times: int = 1) -> None:
        self.script.setdefault(method, []).extend([exc] * times)

    def add_channel(self, entity, members=(), banned=(), total="auto") -> None:
        self.entities[entity.id] = entity
        self.members[entity.id] = list(members)
        self.banned[entity.id] = list(banned)
        if total != "auto":
            self.totals[entity.id] = total

    def total_for(self, cid: int):
        return self.totals[cid] if cid in self.totals else len(self.members.get(cid, []))

    def new_session(self) -> str:
        token = f"fake-session-{next(self._ids)}"
        self.sessions[token] = {"user": None, "pending": None, "needs_password": False, "revoked": False}
        return token

    def authorize_session(self, token: str, phone: str) -> str:
        """Pretend a user already logged in elsewhere (used to seed an authorised session)."""
        self.sessions[token]["user"] = self.accounts[phone]
        return token

    def invalidate_all_sessions(self) -> None:
        for s in self.sessions.values():
            s["revoked"] = True

    def called(self, name: str) -> int:
        return sum(1 for c in self.calls if c[0] == name)

    async def maybe_hang(self, method: str) -> None:
        if method in self.hang:
            await asyncio.sleep(3600)

    def _maybe_raise(self, method: str) -> None:
        q = self.script.get(method)
        if q:
            raise q.pop(0)


class FakeSession:
    def __init__(self, token: str) -> None:
        self._token = token

    def save(self) -> str:
        return self._token


class FakeClient:
    def __init__(self, world: FakeTelegram, session_string: str | None) -> None:
        self.world = world
        self._token = session_string or None
        self.session: FakeSession | None = None
        self.connected = False

    # --- connection
    async def connect(self):
        self.world.calls.append(("connect",))
        if self.world.connect_error:
            raise self.world.connect_error
        if self._token is None:
            self._token = self.world.new_session()
        elif self._token not in self.world.sessions:
            self.world.sessions[self._token] = {"user": None, "pending": None, "needs_password": False,
                                                "revoked": False}
        self.session = FakeSession(self._token)
        self.connected = True

    async def disconnect(self):
        self.connected = False

    @property
    def _state(self) -> dict:
        return self.world.sessions[self._token]

    def _require_auth(self):
        st = self._state
        if st["revoked"]:
            raise tl_err.AuthKeyUnregisteredError(request=None)
        if st["user"] is None:
            raise tl_err.AuthKeyUnregisteredError(request=None)

    async def is_user_authorized(self) -> bool:
        self.world.calls.append(("is_user_authorized",))
        st = self._state
        if st["revoked"]:
            raise tl_err.AuthKeyUnregisteredError(request=None)
        return st["user"] is not None

    # --- login
    async def send_code_request(self, phone, force_sms=False):
        self.world.calls.append(("send_code_request",))
        await self.world.maybe_hang("send_code_request")
        self.world._maybe_raise("send_code_request")
        if phone not in self.world.accounts:
            raise tl_err.PhoneNumberInvalidError(request=None)
        self._state["pending"] = phone
        return SimpleNamespace(phone_code_hash=self.world.code_hash, type=types.auth.SentCodeTypeApp(length=5))

    async def sign_in(self, phone=None, code=None, *, password=None, bot_token=None, phone_code_hash=None):
        self.world.calls.append(("sign_in", "password" if password is not None else "code"))
        self.world._maybe_raise("sign_in")
        st = self._state
        if password is not None:
            if not st["needs_password"]:
                raise tl_err.RPCError(request=None, message="no password required", code=400)
            acct = self.world.accounts[st["pending"]]
            if password != acct["password"]:
                raise tl_err.PasswordHashInvalidError(request=None)
            st["user"] = acct
            return types.User(id=acct["id"])
        if phone_code_hash != self.world.code_hash:
            raise tl_err.PhoneCodeInvalidError(request=None)
        acct = self.world.accounts[phone]
        if code != acct["code"]:
            raise tl_err.PhoneCodeInvalidError(request=None)
        if acct["password"]:
            st["needs_password"] = True
            raise tl_err.SessionPasswordNeededError(request=None)
        st["user"] = acct
        return types.User(id=acct["id"])

    async def __call__(self, request):
        """Raw MTProto requests used by Stage 2: membership checks and invitations."""
        name = type(request).__name__
        self.world.calls.append(("request", name))
        await self.world.maybe_hang(name)
        self._require_auth()
        if name not in ("InviteToChannelRequest", "AddChatUserRequest"):
            self.world._maybe_raise(name)               # invites apply their script inside _invite (after being counted)
        if name == "GetParticipantRequest":
            cid, uid = request.channel.channel_id, request.participant.user_id
            self.world._maybe_raise(f"GetParticipantRequest:{uid}")
            if uid in {u.id for u in self.world.members.get(cid, [])}:
                return SimpleNamespace(participant=types.ChannelParticipant(
                    user_id=uid, date=datetime.now(timezone.utc)))
            raise tl_err.UserNotParticipantError(request=None)
        if name == "GetFullChatRequest":
            cid = request.chat_id
            parts = [SimpleNamespace(user_id=u.id) for u in self.world.members.get(cid, [])]
            return SimpleNamespace(full_chat=SimpleNamespace(participants=SimpleNamespace(participants=parts)))
        if name in ("InviteToChannelRequest", "AddChatUserRequest"):
            if name == "InviteToChannelRequest":
                cid, uid = request.channel.channel_id, request.users[0].user_id
            else:
                cid, uid = request.chat_id, request.user_id.user_id
            return await self._invite(name, cid, uid)
        raise AssertionError(f"FakeClient does not implement {name}")

    async def _invite(self, name, cid, uid):
        w = self.world
        w.invite_calls.append((cid, uid))
        w.inflight += 1
        w.max_inflight = max(w.max_inflight, w.inflight)
        try:
            if w.invite_delay:
                await asyncio.sleep(w.invite_delay)
            if w.on_invite is not None:
                await w.on_invite(cid, uid)
            w._maybe_raise(name)                          # request-level script (e.g. one FloodWait for any user)
            queue = w.invite_behavior.get(uid)
            action = queue.pop(0) if queue else None
            if isinstance(action, Exception):
                raise action
            if action == "noop":
                return SimpleNamespace(missing_invitees=[])
            if action == "missing":
                return SimpleNamespace(missing_invitees=[types.MissingInvitee(user_id=uid)])
            w.members.setdefault(cid, []).append(make_user(uid, f"User{uid}"))
            w.invited.append((cid, uid))
            if w.on_landed is not None:
                w.on_landed(cid, uid)
            return SimpleNamespace(missing_invitees=[])
        finally:
            w.inflight -= 1

    async def get_me(self):
        self._require_auth()
        u = self._state["user"]
        return types.User(id=u["id"], first_name=u["first_name"], username=u["username"], is_self=True)

    async def log_out(self):
        self.world.calls.append(("log_out",))
        self._require_auth()
        self.world.logged_out.append(self._token)
        self._state["revoked"] = True
        return True

    # --- data
    async def iter_dialogs(self, limit=None):
        self.world.calls.append(("iter_dialogs",))
        await self.world.maybe_hang("iter_dialogs")
        self._require_auth()
        self.world._maybe_raise("iter_dialogs")
        for entity in list(self.world.entities.values()):
            yield SimpleNamespace(entity=entity)
        yield SimpleNamespace(entity=make_user(42, "Just a person"))  # private chats must be ignored

    async def get_entity(self, peer):
        self.world.calls.append(("get_entity",))
        self._require_auth()
        self.world._maybe_raise("get_entity")
        cid = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None)
        entity = self.world.entities.get(cid)
        if entity is None:
            return types.ChannelForbidden(id=cid or 0, access_hash=1, title="gone")
        return entity

    async def get_participants(self, peer, limit=None, **kw):
        self.world.calls.append(("get_participants",))
        await self.world.maybe_hang("get_participants")
        self._require_auth()
        self.world._maybe_raise("get_participants")
        cid = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None)
        result = TotalList()
        result.total = self.world.total_for(cid)
        return result

    async def iter_participants(self, peer, limit=None, *, search="", filter=None, aggressive=False):
        kind = "banned" if isinstance(filter, types.ChannelParticipantsKicked) else "members"
        self.world.calls.append(("iter_participants", kind, aggressive))
        await self.world.maybe_hang(f"iter_participants:{kind}")
        self._require_auth()
        self.world._maybe_raise(f"iter_participants:{kind}")
        cid = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None)
        pool = self.world.banned[cid] if kind == "banned" else self.world.members[cid]
        fail = self.world.iter_fail_after.get((cid, kind))
        for i, user in enumerate(pool):
            if limit is not None and i >= limit:
                return
            if fail and i >= fail[0]:
                raise fail[1]
            yield user


class FakeFactory:
    def __init__(self, world: FakeTelegram) -> None:
        self.world = world

    def create(self, session_string: str | None = None) -> FakeClient:
        return FakeClient(self.world, session_string)
