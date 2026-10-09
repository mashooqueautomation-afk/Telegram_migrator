"""Validated migration-job state machine + compare-and-set transitions.

        PENDING ──start──▶ RUNNING ◀──resume── PAUSED
           │                │  │  ╲               │
           │             pause  │   ╲──fail──▶ FAILED
           │                ▼   │
           │             PAUSED  ├──complete──▶ COMPLETED
           │                     │
           └──cancel──▶ CANCELLED ◀──── CANCELLING ◀──cancel── RUNNING

COMPLETED, CANCELLED and FAILED are terminal. Re-running failures creates a NEW linked job (history is immutable).
"""
from __future__ import annotations

import uuid
from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import AppError
from app.db.base import utcnow
from app.domain import JobStatus
from app.models import MigrationJob

S = JobStatus

ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    S.PENDING: frozenset({S.RUNNING, S.CANCELLED}),
    S.RUNNING: frozenset({S.PAUSED, S.CANCELLING, S.CANCELLED, S.COMPLETED, S.FAILED}),
    S.PAUSED: frozenset({S.RUNNING, S.CANCELLED, S.FAILED}),
    S.CANCELLING: frozenset({S.CANCELLED, S.FAILED}),
    S.COMPLETED: frozenset(),
    S.CANCELLED: frozenset(),
    S.FAILED: frozenset(),
}
TERMINAL = frozenset({S.COMPLETED, S.CANCELLED, S.FAILED})
ACTIVE = frozenset({S.RUNNING, S.CANCELLING})   # a worker may be touching the job


class InvalidTransition(AppError):
    status_code = 409
    code = "invalid_transition"

    def __init__(self, current: JobStatus, target: JobStatus) -> None:
        super().__init__(f"A {current.value} migration cannot move to {target.value}.", code="invalid_transition")
        self.current, self.target = current, target


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED[current]


def assert_transition(current: JobStatus, target: JobStatus) -> None:
    if not can_transition(current, target):
        raise InvalidTransition(current, target)


async def cas_transition(db: AsyncSession, job_id: uuid.UUID, *, expected: JobStatus | Iterable[JobStatus],
                         to: JobStatus, now: datetime | None = None, **values) -> bool:
    """Atomically move a job `expected -> to` (UPDATE ... WHERE status IN expected). False = someone else got there first.

    Every (expected -> to) pair is validated against the state machine before touching the database.
    """
    expected_set = {expected} if isinstance(expected, JobStatus) else set(expected)
    for current in expected_set:
        assert_transition(current, to)
    now = now or utcnow()
    extra: dict = dict(values)
    if to in TERMINAL:
        extra.setdefault("completed_at", now)
        extra.setdefault("current_member_id", None)
        extra.setdefault("current_member_label", None)
    if to is S.RUNNING:
        extra["started_at"] = func.coalesce(MigrationJob.started_at, now)
    result = await db.execute(update(MigrationJob).where(MigrationJob.id == job_id,
                                                         MigrationJob.status.in_(expected_set))
                              .values(status=to, updated_at=now, **extra))
    return result.rowcount == 1
