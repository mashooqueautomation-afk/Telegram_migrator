"""Telethon client construction. The ONLY place a TelegramClient is created."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from telethon import TelegramClient
from telethon.sessions import StringSession

from app.telegram.errors import TelegramNetworkError, TelegramSessionInvalid, translate_telethon_errors

log = logging.getLogger(__name__)


class TelegramClientFactory:
    def __init__(self, *, api_id: int, api_hash: str, device_name: str = "TG Migrator",
                 timeout: int = 20, connection_retries: int = 2) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._device_name = device_name
        self._timeout = timeout
        self._connection_retries = connection_retries

    def create(self, session_string: str | None = None) -> TelegramClient:
        return TelegramClient(
            StringSession(session_string or ""),
            self._api_id,
            self._api_hash,
            device_model=self._device_name,  # shows up under Telegram > Settings > Devices
            app_version="1.0",
            flood_sleep_threshold=0,         # never auto-sleep: FloodWait is raised so WE decide (and record it)
            connection_retries=self._connection_retries,
            request_retries=1,
            timeout=self._timeout,
            receive_updates=False,
        )


@asynccontextmanager
async def deadline(seconds: float | None) -> AsyncIterator[None]:
    """Bound a block of Telegram work; expiry becomes a clean TelegramNetworkError (HTTP 503)."""
    if seconds is None:
        yield
        return
    try:
        async with asyncio.timeout(seconds):
            yield
    except TimeoutError:
        raise TelegramNetworkError("Telegram did not respond in time. Try again shortly.") from None


async def _safe_disconnect(client) -> None:
    try:
        await client.disconnect()
    except Exception:  # noqa: BLE001
        log.debug("error while disconnecting Telegram client", exc_info=True)


@asynccontextmanager
async def authorized_client(factory, session_string: str) -> AsyncIterator:
    """Connect with a stored session and verify it is still authorised."""
    client = factory.create(session_string)
    try:
        with translate_telethon_errors():
            await client.connect()
            if not await client.is_user_authorized():
                raise TelegramSessionInvalid()
        yield client
    finally:
        await _safe_disconnect(client)
