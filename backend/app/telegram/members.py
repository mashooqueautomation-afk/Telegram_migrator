"""Member discovery: returns only what Telegram's API legitimately provides.

No `aggressive=True` letter-by-letter searching (that is a workaround to enumerate
members Telegram deliberately doesn't list). If Telegram truncates the list, we
say so instead of trying to get around it.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from telethon.tl import types

from app.domain import ChannelKind
from app.telegram.channels import ChannelRef
from app.telegram.errors import FloodWait, translate_telethon_errors

log = logging.getLogger(__name__)

_ROLE_BY_PARTICIPANT = {
    "ChannelParticipantCreator": "creator", "ChatParticipantCreator": "creator",
    "ChannelParticipantAdmin": "admin", "ChatParticipantAdmin": "admin",
    "ChannelParticipant": "member", "ChannelParticipantSelf": "member", "ChatParticipant": "member",
}


@dataclass(frozen=True)
class RawMember:
    telegram_user_id: int
    access_hash: int | None
    username: str | None
    display_name: str
    is_bot: bool
    is_deleted: bool
    is_restricted: bool
    has_usable_hash: bool   # False for "min" user stubs Telegram can't let us reference
    role: str               # creator | admin | member | unknown


@dataclass
class MemberListing:
    members: list[RawMember] = field(default_factory=list)
    reported_total: int | None = None
    complete: bool = False
    incomplete_reason: str | None = None   # member_limit_reached | telegram_partial_list | rate_limited | total_unknown
    rate_limited_for: int | None = None


def raw_member_from_user(user) -> RawMember | None:
    if not isinstance(user, types.User):
        return None  # e.g. UserEmpty
    deleted = bool(user.deleted)
    name = " ".join(p for p in (user.first_name, user.last_name) if p).strip()
    if deleted:
        name = "Deleted account"
    elif not name:
        name = user.username or f"User {user.id}"
    participant = getattr(user, "participant", None)
    role = _ROLE_BY_PARTICIPANT.get(type(participant).__name__, "unknown") if participant is not None else "unknown"
    return RawMember(
        telegram_user_id=user.id,
        access_hash=user.access_hash,
        username=user.username,
        display_name=name[:255],
        is_bot=bool(user.bot),
        is_deleted=deleted,
        is_restricted=bool(user.restricted),
        has_usable_hash=user.access_hash is not None and not bool(user.min),
        role=role,
    )


async def fetch_members(client, ref: ChannelRef, *, limit: int,
                        kind: Literal["members", "banned"] = "members") -> MemberListing:
    """List members (or banned users) the account is allowed to see, up to `limit`.

    Partial results are kept when Telegram asks us to slow down; the listing is then
    flagged incomplete and `rate_limited_for` holds Telegram's requested cooldown.
    """
    if kind == "banned" and ref.kind is ChannelKind.GROUP:
        raise ValueError("basic groups have no separate banned list")

    peer = ref.to_input_peer()
    listing = MemberListing()
    iter_kwargs: dict = {"aggressive": False}
    if kind == "banned":
        iter_kwargs["filter"] = types.ChannelParticipantsKicked(q="")

    seen: set[int] = set()
    try:
        with translate_telethon_errors():
            if kind == "members":
                counted = await client.get_participants(peer, limit=0)
                listing.reported_total = getattr(counted, "total", None)
            async for user in client.iter_participants(peer, limit=limit, **iter_kwargs):
                raw = raw_member_from_user(user)
                if raw is None or raw.telegram_user_id in seen:
                    continue
                seen.add(raw.telegram_user_id)
                listing.members.append(raw)
    except FloodWait as fw:
        listing.rate_limited_for = fw.retry_after if fw.retry_after is not None else 0
        listing.complete = False
        listing.incomplete_reason = "rate_limited"
        log.info("Telegram FloodWait during member listing (%s s); returning partial results", fw.retry_after)
        return listing

    fetched = len(listing.members)
    if kind == "banned":
        # No reliable server-side total for the banned filter: complete == we exhausted the iterator under our limit.
        listing.complete = fetched < limit
        listing.incomplete_reason = None if listing.complete else "member_limit_reached"
    elif listing.reported_total is None:
        listing.complete = False
        listing.incomplete_reason = "total_unknown"
    elif fetched >= listing.reported_total:
        listing.complete = True
    elif fetched >= limit:
        listing.complete = False
        listing.incomplete_reason = "member_limit_reached"
    else:
        listing.complete = False
        listing.incomplete_reason = "telegram_partial_list"  # Telegram capped/hid part of the list
    return listing
