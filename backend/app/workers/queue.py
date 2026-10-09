"""Job queue abstraction. Production = arq on Redis; tests inject a recording queue."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Protocol

from app.core.errors import AppError

log = logging.getLogger(__name__)

RUN_MIGRATION = "run_migration"
ANALYZE_JOB = "analyze_job"


def run_job_id(migration_id, token: int | str | None = None) -> str:
    """Queue-level dedup key. The token (e.g. the resume timestamp or the current minute) keeps one orphaned arq entry
    from a crashed worker from blocking re-enqueueing for hours; the DB lease already prevents double processing."""
    return f"migrate:{migration_id}" + (f":{token}" if token is not None else "")


def analyze_job_id(migration_id, token: int | str | None = None) -> str:
    return f"analyze:{migration_id}" + (f":{token}" if token is not None else "")


def minute_token(now: datetime) -> int:
    return int(now.timestamp() // 60)


class QueueUnavailable(AppError):
    status_code = 503
    code = "queue_unavailable"

    def __init__(self, message: str = "The job queue (Redis) is not available. Is the worker stack running?") -> None:
        super().__init__(message, code="queue_unavailable")


class JobQueue(Protocol):
    async def enqueue(self, function: str, *args, job_id: str | None = None,
                      defer_until: datetime | None = None) -> bool:
        """True if newly queued; False if an identical job id is already queued/running (deduplicated)."""

    async def close(self) -> None: ...


class NullQueue:
    """Used when REDIS_URL is not configured."""

    async def enqueue(self, function, *args, job_id=None, defer_until=None) -> bool:
        raise QueueUnavailable("REDIS_URL is not configured, so jobs cannot be queued.")

    async def close(self) -> None:
        return None


class ArqQueue:
    def __init__(self, redis_url: str) -> None:
        from arq.connections import RedisSettings
        self._settings = RedisSettings.from_dsn(redis_url)
        self._pool = None

    async def _get_pool(self):
        if self._pool is None:
            from arq import create_pool
            self._pool = await create_pool(self._settings)
        return self._pool

    async def enqueue(self, function: str, *args, job_id: str | None = None,
                      defer_until: datetime | None = None) -> bool:
        try:
            pool = await self._get_pool()
            job = await pool.enqueue_job(function, *args, _job_id=job_id, _defer_until=defer_until)
        except Exception as exc:  # noqa: BLE001 - redis ConnectionError, OSError, timeouts ...
            log.warning("could not enqueue %s: %s", function, type(exc).__name__)
            self._pool = None
            raise QueueUnavailable() from exc
        return job is not None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None


def build_queue(redis_url: str | None) -> JobQueue:
    return ArqQueue(redis_url) if redis_url else NullQueue()
