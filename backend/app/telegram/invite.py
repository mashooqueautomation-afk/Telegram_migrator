"""The ONE legitimate way Telegram lets an admin add someone: invite a single user the account can reference.

  * channels / supergroups -> channels.InviteToChannel
  * legacy basic groups    -> messages.AddChatUser

Nothing here tries to get around a refusal. Every failure is classified (errors.classify_invite_exception) and the caller
decides what to record. A call that raises nothing is NOT treated as success: `verify_member` asks Telegram afterwards.
"""
from __future__ import annotations

from dataclasses import dataclass

from telethon import errors as tl
from telethon.tl import types
from telethon.tl.functions import channels as fn_channels
from telethon.tl.functions import messages as fn_messages

from app.domain import ChannelKind, InviteOutcome
from app.telegram.channels import ChannelRef
from app.telegram.errors import InviteError, classify_invite_exception


@dataclass(frozen=True)
class UserRef:
    user_id: int
    access_hash: int | None


def _classified(exc: BaseException):
    translated = classify_invite_exception(exc)
    return translated if translated is not None else exc


async def is_member(client, dest: ChannelRef, user: UserRef) -> bool:
    """Ask Telegram whether the user is currently a participant of `dest` (a read; also subject to FloodWait)."""
    try:
        if dest.kind is ChannelKind.GROUP:
            full = await client(fn_messages.GetFullChatRequest(dest.telegram_id))
            participants = getattr(getattr(full.full_chat, "participants", None), "participants", []) or []
            return any(getattr(p, "user_id", None) == user.user_id for p in participants)
        await client(fn_channels.GetParticipantRequest(
            channel=types.InputChannel(dest.telegram_id, dest.access_hash or 0),
            participant=types.InputPeerUser(user.user_id, user.access_hash or 0)))
        return True
    except tl.UserNotParticipantError:
        return False
    except Exception as exc:  # noqa: BLE001
        raise _classified(exc) from exc


async def invite_user(client, dest: ChannelRef, user: UserRef) -> None:
    """Send ONE invitation. Raises FloodWait / InviteError on any refusal; returning only means 'no error raised'."""
    input_user = types.InputUser(user.user_id, user.access_hash or 0)
    try:
        if dest.kind is ChannelKind.GROUP:
            result = await client(fn_messages.AddChatUserRequest(chat_id=dest.telegram_id, user_id=input_user,
                                                                 fwd_limit=0))
        else:
            result = await client(fn_channels.InviteToChannelRequest(
                channel=types.InputChannel(dest.telegram_id, dest.access_hash or 0), users=[input_user]))
    except Exception as exc:  # noqa: BLE001
        raise _classified(exc) from exc

    # Newer layers report users that could NOT be added in `missing_invitees` instead of raising.
    if getattr(result, "missing_invitees", None):
        raise InviteError(InviteOutcome.PRIVACY_RESTRICTED, error_code="MISSING_INVITEE",
                          message="Telegram did not add the user: their privacy settings (or Premium requirements) "
                                  "prevented it.")
