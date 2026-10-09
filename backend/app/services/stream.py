"""Server-Sent Events: tails the persisted, sequenced migration_events table.

Why the database (not Redis pub/sub): events are never lost, the stream resumes exactly with `Last-Event-ID`, it works with
any number of API processes, and a reconnecting client replays what it missed. No DB connection is held between polls.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.models import MigrationEvent, MigrationJob
from app.services.events import EventType, event_to_payload, progress_payload, sse_frame
from app.services.job_state import TERMINAL

BATCH = 200


def _fingerprint(snapshot: dict) -> str:
    stable = {k: v for k, v in snapshot.items() if k != "rate_limit_remaining_seconds"}  # clients count down locally
    return json.dumps(stable, sort_keys=True, default=str)


async def event_stream(*, sessionmaker: async_sessionmaker[AsyncSession], settings: Settings, job_id: uuid.UUID,
                       after_seq: int = 0, is_disconnected: Callable[[], Awaitable[bool]] | None = None,
                       sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> AsyncIterator[str]:
    last_seq, last_fp, first = after_seq, None, True
    next_ping = time.monotonic() + settings.sse_heartbeat_seconds
    yield "retry: 3000\n\n"                                   # EventSource reconnect delay
    while True:
        if is_disconnected is not None and await is_disconnected():
            return
        async with sessionmaker() as db:
            job = await db.get(MigrationJob, job_id)
            if job is None:                                   # deleted while streaming
                yield sse_frame("migration.deleted", {"type": "migration.deleted", "migration_id": str(job_id)})
                return
            events = (await db.execute(select(MigrationEvent).where(MigrationEvent.job_id == job_id,
                                                                    MigrationEvent.seq > last_seq)
                                       .order_by(MigrationEvent.seq).limit(BATCH))).scalars().all()
            snapshot = progress_payload(job)
            terminal = job.status in TERMINAL
        for ev in events:
            yield sse_frame(ev.event_type, event_to_payload(ev), ev.seq)
            last_seq = ev.seq
        fp = _fingerprint(snapshot)
        if first or events or fp != last_fp:
            yield sse_frame(EventType.PROGRESS, snapshot)
            last_fp, first = fp, False
        if terminal and not events:
            return                                            # everything flushed; the migration is over
        if len(events) == BATCH:
            continue                                          # more backlog: no sleep
        if time.monotonic() >= next_ping:
            yield ": ping\n\n"
            next_ping = time.monotonic() + settings.sse_heartbeat_seconds
        await sleep(settings.sse_poll_interval_seconds)
