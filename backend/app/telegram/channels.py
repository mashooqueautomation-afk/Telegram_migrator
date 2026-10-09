"""Channel/group discovery. Only entities the connected account can legitimately see."""
from __future__ import annotations

from dataclasses import dataclass

from telethon.tl import types

from app.domain import ChannelKind
from app.telegram.errors import ChannelInaccessible, translate_telethon_errors


@dataclass(frozen=True)
class ChannelRef:
    kind: ChannelKind
    telegram_id: int
    access_hash: int | None

    def to_input_peer(self):
        if self.kind is ChannelKind.GROUP:
            return types.InputPeerChat(self.telegram_id)
        return types.InputPeerChannel(self.telegram_id, self.access_hash or 0)


@dataclass(frozen=True)
class ChannelInfo:
    telegram_id: int
    access_hash: int | None
    kind: ChannelKind
    title: str
    username: str | None
    participants_count: int | None
    is_creator: bool
    is_admin: bool
    can_invite_users: bool

    @property
    def ref(self) -> ChannelRef:
        return ChannelRef(self.kind, self.telegram_id, self.access_hash)


def channel_info_from_entity(entity) -> ChannelInfo | None:
    """Convert a Telethon entity to ChannelInfo; None for users, forbidden/deactivated/left chats."""
    if isinstance(entity, types.Channel):
        if getattr(entity, "left", False):
            return None
        kind = ChannelKind.SUPERGROUP if entity.megagroup else ChannelKind.CHANNEL
        username = entity.username
    elif isinstance(entity, types.Chat):
        if getattr(entity, "deactivated", False) or getattr(entity, "left", False) \
                or getattr(entity, "migrated_to", None) is not None:
            return None
        kind = ChannelKind.GROUP
        username = None
    else:
        return None

    rights = getattr(entity, "admin_rights", None)
    is_creator = bool(getattr(entity, "creator", False))
    is_admin = is_creator or rights is not None
    can_invite = is_creator or bool(rights is not None and getattr(rights, "invite_users", False))
    return ChannelInfo(
        telegram_id=entity.id,
        access_hash=getattr(entity, "access_hash", None),
        kind=kind,
        title=entity.title,
        username=username,
        participants_count=getattr(entity, "participants_count", None),
        is_creator=is_creator,
        is_admin=is_admin,
        can_invite_users=can_invite,
    )


async def list_channels(client, *, administered_only: bool = True, limit: int = 500) -> list[ChannelInfo]:
    found: dict[int, ChannelInfo] = {}
    with translate_telethon_errors():
        async for dialog in client.iter_dialogs(limit=limit):
            info = channel_info_from_entity(dialog.entity)
            if info is None or (administered_only and not info.is_admin):
                continue
            found[info.telegram_id] = info
    return sorted(found.values(), key=lambda c: c.title.lower())


async def fetch_channel(client, ref: ChannelRef) -> ChannelInfo:
    """Re-read one channel live from Telegram (authorisation can change after discovery)."""
    with translate_telethon_errors():
        entity = await client.get_entity(ref.to_input_peer())
    info = channel_info_from_entity(entity)
    if info is None:
        raise ChannelInaccessible()
    return info
