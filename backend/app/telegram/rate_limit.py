"""FloodWait policy.

We never try to defeat or route around Telegram's limits. The rules are:
  * Wait only for the exact duration Telegram asks for, and only when it is tiny
    (<= max_inline_wait). Retry once. If it floods again, surface it.
  * Anything longer is surfaced to the caller, which records an account cooldown so
    we stop sending requests until it elapses.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from app.telegram.errors import FloodWait

log = logging.getLogger(__name__)
T = TypeVar("T")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def cooldown_remaining(until: datetime | None, now: datetime | None = None) -> int:
    """Whole seconds left on a cooldown (0 if none/elapsed)."""
    if until is None:
        return 0
    now = now or now_utc()
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    return max(0, int((until - now).total_seconds()) + (1 if (until - now).total_seconds() % 1 else 0))


def cooldown_until(retry_after: int | None, now: datetime | None = None) -> datetime | None:
    if retry_after is None:
        return None
    return (now or now_utc()) + timedelta(seconds=retry_after)


def ensure_cooldown_elapsed(until: datetime | None, now: datetime | None = None) -> None:
    """Raise FloodWait if a previously recorded cooldown is still running (no request is sent)."""
    remaining = cooldown_remaining(until, now)
    if remaining > 0:
        raise FloodWait(remaining)


async def call_with_flood_policy(
    fn: Callable[[], Awaitable[T]],
    *,
    max_inline_wait: int,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> T:
    sleep = sleep or asyncio.sleep  # resolved at call time so tests can patch it
    try:
        return await fn()
    except FloodWait as fw:
        if fw.retry_after is None or fw.retry_after > max_inline_wait:
            raise
        log.info("Telegram FloodWait of %ss is within the inline limit; waiting then retrying once", fw.retry_after)
        await sleep(fw.retry_after + 1)
        return await fn()  # a second FloodWait propagates to the caller
