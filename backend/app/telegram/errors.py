"""Our own Telegram exception hierarchy + translation from Telethon's RPC errors.

Nothing outside app/telegram needs to import Telethon exceptions.
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager

from telethon import errors as tl


class TelegramError(Exception):
    code = "telegram_error"
    default_message = "Telegram request failed."

    def __init__(self, message: str | None = None, *, retry_after: int | None = None) -> None:
        super().__init__(message or self.default_message)
        self.message = message or self.default_message
        self.retry_after = retry_after


class TelegramConfigError(TelegramError):
    code = "telegram_not_configured"
    default_message = "The server's Telegram API credentials (api_id/api_hash) were rejected by Telegram."


class PhoneNumberInvalid(TelegramError):
    code = "phone_number_invalid"
    default_message = "Telegram does not accept this phone number."


class PhoneNumberBanned(TelegramError):
    code = "phone_number_banned"
    default_message = "Telegram reports this phone number as banned."


class InvalidLoginCode(TelegramError):
    code = "invalid_login_code"
    default_message = "The login code is incorrect."


class LoginCodeExpired(TelegramError):
    code = "login_code_expired"
    default_message = "The login code has expired. Request a new one."


class TwoFactorRequired(TelegramError):
    """Signals that the account has a cloud password. Carries the updated temporary session."""

    code = "two_factor_required"
    default_message = "This account is protected by a two-step verification password."

    def __init__(self, temp_session: str) -> None:
        super().__init__()
        self.temp_session = temp_session


class InvalidTwoFactorPassword(TelegramError):
    code = "invalid_two_factor_password"
    default_message = "The two-step verification password is incorrect."


class TelegramSessionInvalid(TelegramError):
    code = "telegram_session_invalid"
    default_message = "The stored Telegram session is no longer valid. Reconnect the account."


class FloodWait(TelegramError):
    code = "telegram_flood_wait"
    default_message = "Telegram asked us to slow down."

    def __init__(self, retry_after: int | None) -> None:
        msg = (f"Telegram requested a cooldown of {retry_after} seconds."
               if retry_after is not None else self.default_message)
        super().__init__(msg, retry_after=retry_after)


class ChannelInaccessible(TelegramError):
    code = "channel_inaccessible"
    default_message = "This channel/group is private or not accessible with the connected account."


class AdminPermissionRequired(TelegramError):
    code = "admin_permission_required"
    default_message = "The connected account lacks the administrator permission needed for this operation."


class TelegramNetworkError(TelegramError):
    code = "telegram_network_error"
    default_message = "Could not reach Telegram. Try again shortly."


class TelegramApiError(TelegramError):
    code = "telegram_api_error"
    default_message = "Telegram returned an unexpected error."

    def __init__(self, message: str | None = None, *, rpc_name: str | None = None) -> None:
        super().__init__(message)
        self.rpc_name = rpc_name


_SESSION_DEAD = (
    tl.AuthKeyUnregisteredError, tl.SessionRevokedError, tl.SessionExpiredError,
    tl.UserDeactivatedError, tl.UserDeactivatedBanError, tl.AuthKeyDuplicatedError,
)


def translate_exception(exc: BaseException) -> TelegramError | None:
    """Map a Telethon/network exception to ours. Returns None if it isn't a Telegram-ish error."""
    if isinstance(exc, TelegramError):
        return exc
    # Order matters: FloodWaitError is a subclass of RPCError.
    if isinstance(exc, tl.FloodWaitError):
        return FloodWait(getattr(exc, "seconds", None))
    if isinstance(exc, tl.PhoneNumberFloodError):
        return FloodWait(None)
    if isinstance(exc, tl.ApiIdInvalidError):
        return TelegramConfigError()
    if isinstance(exc, (tl.PhoneNumberInvalidError, tl.PhoneNumberUnoccupiedError)):
        return PhoneNumberInvalid()
    if isinstance(exc, tl.PhoneNumberBannedError):
        return PhoneNumberBanned()
    if isinstance(exc, (tl.PhoneCodeInvalidError, tl.PhoneCodeEmptyError)):
        return InvalidLoginCode()
    if isinstance(exc, tl.PhoneCodeExpiredError):
        return LoginCodeExpired()
    if isinstance(exc, (tl.PasswordHashInvalidError, tl.PasswordEmptyError)):
        return InvalidTwoFactorPassword()
    if isinstance(exc, _SESSION_DEAD):
        return TelegramSessionInvalid()
    if isinstance(exc, (tl.ChannelPrivateError, tl.ChannelInvalidError, tl.ChannelBannedError,
                        tl.PeerIdInvalidError, tl.ChatIdInvalidError)):
        return ChannelInaccessible()
    if isinstance(exc, tl.ChatAdminRequiredError):
        return AdminPermissionRequired()
    # Transport-level failures: dropped/garbled connections, proxies answering with non-MTProto bytes, etc.
    if isinstance(exc, (tl.TimedOutError, asyncio.TimeoutError, ConnectionError, OSError, EOFError,
                        tl.InvalidBufferError, tl.InvalidChecksumError, tl.SecurityError, tl.ReadCancelledError)):
        return TelegramNetworkError()
    if isinstance(exc, (tl.TypeNotFoundError, tl.BadMessageError)):
        return TelegramApiError(rpc_name=type(exc).__name__)
    if isinstance(exc, tl.RPCError):
        return TelegramApiError(rpc_name=type(exc).__name__)
    return None


@contextmanager
def translate_telethon_errors() -> Iterator[None]:
    try:
        yield
    except tl.SessionPasswordNeededError:
        raise  # handled explicitly by the login code
    except Exception as exc:  # noqa: BLE001
        translated = translate_exception(exc)
        if translated is None:
            raise
        raise translated from exc


# =====================================================================================================
# Stage 2: classification of ONE member's invitation attempt.
#
# Policy (never bypass, only classify): every Telegram refusal is recorded exactly and the engine moves on.
#   * permanent refusals   -> member is skipped, never retried (privacy, invalid user, banned user ...)
#   * transient failures   -> retried with exponential backoff, then the member is marked FAILED
#   * FloodWait            -> the whole job pauses until the cooldown elapses
#   * fatal for the job    -> the job stops and reports it (admin rights lost, OUR account restricted, session dead)
# =====================================================================================================
from app.domain import InviteOutcome  # noqa: E402


class InviteError(TelegramError):
    code = "invite_error"
    default_message = "Telegram refused the invitation."

    def __init__(self, outcome: InviteOutcome, *, error_code: str, message: str, retryable: bool = False,
                 fatal: bool = False) -> None:
        super().__init__(message)
        self.outcome = outcome
        self.error_code = error_code
        self.retryable = retryable   # worth an automatic retry with backoff
        self.fatal = fatal           # stop the whole job


def _inv(outcome: InviteOutcome, code: str, message: str, *, retryable=False, fatal=False) -> InviteError:
    return InviteError(outcome, error_code=code, message=message, retryable=retryable, fatal=fatal)


# (Telethon error classes, outcome, RPC error code, human message, fatal?) — first match wins.
_INVITE_TABLE: list[tuple[tuple[type, ...], InviteOutcome, str, str, bool]] = [
    ((tl.UserPrivacyRestrictedError,), InviteOutcome.PRIVACY_RESTRICTED, "USER_PRIVACY_RESTRICTED",
     "The user's privacy settings do not allow them to be added to groups/channels.", False),
    ((tl.UserNotMutualContactError,), InviteOutcome.PRIVACY_RESTRICTED, "USER_NOT_MUTUAL_CONTACT",
     "The user only accepts invitations from mutual contacts.", False),
    ((tl.UserAlreadyParticipantError,), InviteOutcome.ALREADY_MEMBER, "USER_ALREADY_PARTICIPANT",
     "The user is already a member of the destination.", False),
    ((tl.UserChannelsTooMuchError,), InviteOutcome.USER_RESTRICTED, "USER_CHANNELS_TOO_MUCH",
     "The user has joined the maximum number of channels/groups Telegram allows.", False),
    ((tl.UserKickedError,), InviteOutcome.USER_RESTRICTED, "USER_KICKED",
     "The user was removed from the destination and an admin must re-admit them.", False),
    ((tl.UserBlockedError,), InviteOutcome.USER_RESTRICTED, "USER_BLOCKED",
     "The user is blocked in the destination.", False),
    ((tl.UserBotError, tl.BotGroupsBlockedError), InviteOutcome.INVALID_USER, "USER_BOT",
     "Bots cannot be invited as members.", False),
    ((tl.UserIdInvalidError, tl.InputUserDeactivatedError, tl.PeerIdInvalidError), InviteOutcome.INVALID_USER,
     "USER_ID_INVALID", "Telegram does not recognise this user (deleted or not resolvable).", False),
    ((tl.ChatAdminRequiredError, tl.ChatAdminInviteRequiredError), InviteOutcome.ADMIN_REQUIRED,
     "CHAT_ADMIN_REQUIRED", "The connected account is not (or no longer) allowed to invite users in the destination.",
     True),
    ((tl.ChannelPrivateError, tl.ChannelInvalidError, tl.ChatIdInvalidError, tl.ChatInvalidError,
      tl.ChannelBannedError), InviteOutcome.CHANNEL_RESTRICTED, "CHANNEL_PRIVATE",
     "The destination is private, invalid or no longer accessible to the connected account.", True),
    ((tl.UsersTooMuchError,), InviteOutcome.CHANNEL_RESTRICTED, "USERS_TOO_MUCH",
     "The destination has reached Telegram's member limit.", True),
    ((tl.PeerFloodError,), InviteOutcome.ACCOUNT_RESTRICTED, "PEER_FLOOD",
     "Telegram has limited this account for contacting too many users (anti-spam). Stop and check @SpamBot.", True),
    ((tl.UserBannedInChannelError,), InviteOutcome.ACCOUNT_RESTRICTED, "USER_BANNED_IN_CHANNEL",
     "Telegram has restricted this account from inviting/sending in groups and channels.", True),
    ((tl.ChannelsTooMuchError,), InviteOutcome.ACCOUNT_RESTRICTED, "CHANNELS_TOO_MUCH",
     "The connected account has joined the maximum number of channels/groups.", True),
    (_SESSION_DEAD, InviteOutcome.SESSION_INVALID, "SESSION_INVALID",
     "The stored Telegram session is no longer valid. Reconnect the account.", True),
]

_TRANSIENT = (tl.TimedOutError, tl.ServerError, asyncio.TimeoutError, ConnectionError, OSError, EOFError,
              tl.InvalidBufferError, tl.InvalidChecksumError, tl.SecurityError, tl.ReadCancelledError)


def classify_invite_exception(exc: BaseException) -> TelegramError | None:
    """Turn whatever an invitation attempt raised into FloodWait / InviteError. None = not a Telegram error."""
    if isinstance(exc, FloodWait):
        return exc
    if isinstance(exc, InviteError):
        return exc
    if isinstance(exc, tl.FloodWaitError):
        return FloodWait(getattr(exc, "seconds", None))
    for classes, outcome, code, message, fatal in _INVITE_TABLE:
        if isinstance(exc, classes):
            return _inv(outcome, code, message, fatal=fatal)
    if isinstance(exc, TelegramSessionInvalid):
        return _inv(InviteOutcome.SESSION_INVALID, "SESSION_INVALID", TelegramSessionInvalid.default_message,
                    fatal=True)
    if isinstance(exc, TelegramNetworkError) or isinstance(exc, _TRANSIENT):
        return _inv(InviteOutcome.TEMPORARY_ERROR, "NETWORK_ERROR", "Temporary network/Telegram server problem.",
                    retryable=True)
    if isinstance(exc, tl.RPCError):
        return _inv(InviteOutcome.UNKNOWN_ERROR, type(exc).__name__.removesuffix("Error").upper() or "UNKNOWN",
                    "Telegram returned an error we do not recognise; the member was not invited.")
    if isinstance(exc, TelegramError):
        return _inv(InviteOutcome.UNKNOWN_ERROR, exc.code.upper(), exc.message)
    return None
