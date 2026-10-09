"""MTProto login (phone -> code -> optional 2FA password) via Telethon.

Phone numbers, login codes and 2FA passwords are passed straight through to Telegram.
This module never persists or logs them; the caller stores only the resulting session
string, encrypted. Every operation has a hard deadline so a stalled connection can't hang a request.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from telethon import errors as tl

from app.telegram.client import deadline
from app.telegram.errors import TelegramApiError, TwoFactorRequired, translate_telethon_errors

log = logging.getLogger(__name__)

_PHONE_RE = re.compile(r"^\+\d{7,15}$")
DEFAULT_TIMEOUT = 30.0


def normalize_phone(raw: str) -> str:
    cleaned = re.sub(r"[\s\-().]", "", raw)
    if not _PHONE_RE.match(cleaned):
        raise ValueError("Phone number must be in international format, e.g. +14155550123")
    return cleaned


def normalize_code(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not 3 <= len(digits) <= 8:
        raise ValueError("Login code must be 3-8 digits")
    return digits


def mask_phone(phone: str) -> str:
    return "+" + "•" * max(len(phone) - 3, 0) + phone[-2:]


@dataclass(frozen=True)
class LoginChallenge:
    phone_code_hash: str
    temp_session: str   # unauthenticated StringSession carrying only the MTProto auth key
    delivery: str


@dataclass(frozen=True)
class LoginResult:
    session_string: str
    telegram_user_id: int
    username: str | None
    first_name: str | None


def _delivery(sent) -> str:
    name = type(getattr(sent, "type", None)).__name__.lower()
    for key, label in (("app", "telegram_app"), ("sms", "sms"), ("call", "call"), ("flash", "call"),
                       ("email", "email")):
        if key in name:
            return label
    return "other"


async def request_login_code(factory, phone: str, *, timeout: float | None = DEFAULT_TIMEOUT) -> LoginChallenge:
    client = factory.create(None)
    try:
        async with deadline(timeout):
            with translate_telethon_errors():
                await client.connect()
                sent = await client.send_code_request(phone)
                temp_session = client.session.save()
    finally:
        await _disconnect(client)
    code_hash = getattr(sent, "phone_code_hash", None)
    if not code_hash:
        raise TelegramApiError("Telegram did not return a login challenge.")
    return LoginChallenge(phone_code_hash=code_hash, temp_session=temp_session, delivery=_delivery(sent))


async def complete_login_with_code(factory, *, temp_session: str, phone: str, code: str, phone_code_hash: str,
                                   timeout: float | None = DEFAULT_TIMEOUT) -> LoginResult:
    client = factory.create(temp_session)
    try:
        async with deadline(timeout):
            with translate_telethon_errors():
                await client.connect()
                try:
                    await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
                except tl.SessionPasswordNeededError:
                    raise TwoFactorRequired(temp_session=client.session.save()) from None
                return await _finish(client)
    finally:
        await _disconnect(client)


async def complete_login_with_password(factory, *, temp_session: str, password: str,
                                       timeout: float | None = DEFAULT_TIMEOUT) -> LoginResult:
    client = factory.create(temp_session)
    try:
        async with deadline(timeout):
            with translate_telethon_errors():
                await client.connect()
                await client.sign_in(password=password)
                return await _finish(client)
    finally:
        await _disconnect(client)


async def revoke_session(factory, session_string: str, *, timeout: float | None = 15.0) -> bool:
    """Log the session out on Telegram's side. Returns False if it was already dead or unreachable."""
    client = factory.create(session_string)
    try:
        async with deadline(timeout):
            with translate_telethon_errors():
                await client.connect()
                await client.log_out()
        return True
    except Exception:  # noqa: BLE001 - best effort; local deletion proceeds regardless
        log.info("could not log the Telegram session out remotely (already invalid or unreachable)")
        return False
    finally:
        await _disconnect(client)


async def _finish(client) -> LoginResult:
    me = await client.get_me()
    if me is None:
        raise TelegramApiError("Login finished but Telegram returned no account information.")
    return LoginResult(session_string=client.session.save(), telegram_user_id=me.id,
                       username=getattr(me, "username", None), first_name=getattr(me, "first_name", None))


async def _disconnect(client) -> None:
    try:
        await client.disconnect()
    except Exception:  # noqa: BLE001
        log.debug("error while disconnecting Telegram client", exc_info=True)
