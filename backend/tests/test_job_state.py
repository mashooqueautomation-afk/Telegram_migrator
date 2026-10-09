"""Migration state machine: validated transitions + compare-and-set."""
import uuid

import pytest

from app.domain import JobStatus as S
from app.models import MigrationJob, User
from app.services.job_state import ALLOWED, TERMINAL, InvalidTransition, assert_transition, can_transition, \
    cas_transition

VALID = [(S.PENDING, S.RUNNING), (S.RUNNING, S.PAUSED), (S.PAUSED, S.RUNNING), (S.RUNNING, S.CANCELLED),
         (S.RUNNING, S.COMPLETED), (S.RUNNING, S.CANCELLING), (S.CANCELLING, S.CANCELLED), (S.RUNNING, S.FAILED),
         (S.PENDING, S.CANCELLED), (S.PAUSED, S.CANCELLED)]
INVALID = [(S.COMPLETED, S.RUNNING), (S.COMPLETED, S.PAUSED), (S.COMPLETED, S.CANCELLED), (S.CANCELLED, S.RUNNING),
           (S.FAILED, S.RUNNING), (S.PENDING, S.COMPLETED), (S.PENDING, S.PAUSED), (S.PAUSED, S.COMPLETED),
           (S.CANCELLING, S.RUNNING), (S.CANCELLING, S.PAUSED), (S.RUNNING, S.PENDING), (S.RUNNING, S.RUNNING),
           (S.PAUSED, S.PENDING)]


@pytest.mark.parametrize("a,b", VALID)
def test_valid_transitions(a, b):
    assert can_transition(a, b)
    assert_transition(a, b)


@pytest.mark.parametrize("a,b", INVALID)
def test_invalid_transitions_are_rejected(a, b):
    assert not can_transition(a, b)
    with pytest.raises(InvalidTransition) as exc:
        assert_transition(a, b)
    assert exc.value.status_code == 409 and exc.value.code == "invalid_transition"


def test_terminal_states_have_no_way_out():
    for state in TERMINAL:
        assert ALLOWED[state] == frozenset()
    assert {S.COMPLETED, S.CANCELLED, S.FAILED} == set(TERMINAL)
    assert set(ALLOWED) == set(S)         # every status is described


async def _job(db) -> MigrationJob:
    user = User(email=f"{uuid.uuid4().hex[:6]}@x.test", password_hash="h")
    db.add(user)
    await db.flush()
    job = MigrationJob(user_id=user.id, source_title="s", destination_title="d", member_limit=10)
    db.add(job)
    await db.commit()
    return job


async def test_cas_transition_moves_and_stamps_timestamps(db):
    job = await _job(db)
    assert await cas_transition(db, job.id, expected=S.PENDING, to=S.RUNNING)
    await db.commit()
    await db.refresh(job)
    assert job.status is S.RUNNING and job.started_at is not None and job.completed_at is None
    started = job.started_at

    assert await cas_transition(db, job.id, expected=S.RUNNING, to=S.PAUSED)
    assert await cas_transition(db, job.id, expected=S.PAUSED, to=S.RUNNING)
    await db.commit()
    await db.refresh(job)
    assert job.started_at == started                     # started_at is set once, never overwritten

    job.current_member_label = "someone"
    await db.commit()
    assert await cas_transition(db, job.id, expected=S.RUNNING, to=S.COMPLETED)
    await db.commit()
    await db.refresh(job)
    assert job.status is S.COMPLETED and job.completed_at is not None and job.current_member_label is None


async def test_cas_loses_the_race_instead_of_clobbering(db):
    job = await _job(db)
    assert await cas_transition(db, job.id, expected=S.PENDING, to=S.RUNNING)
    assert not await cas_transition(db, job.id, expected=S.PENDING, to=S.RUNNING)     # already moved
    assert not await cas_transition(db, job.id, expected=(S.PAUSED, S.PENDING), to=S.CANCELLED)
    await db.commit()
    await db.refresh(job)
    assert job.status is S.RUNNING


async def test_cas_validates_before_touching_the_database(db):
    job = await _job(db)
    with pytest.raises(InvalidTransition):
        await cas_transition(db, job.id, expected=S.PENDING, to=S.COMPLETED)
    await db.refresh(job)
    assert job.status is S.PENDING


async def test_a_completed_job_can_never_be_made_running_again(db):
    job = await _job(db)
    for expected, to in [(S.PENDING, S.RUNNING), (S.RUNNING, S.COMPLETED)]:
        assert await cas_transition(db, job.id, expected=expected, to=to)
    with pytest.raises(InvalidTransition):
        await cas_transition(db, job.id, expected=S.COMPLETED, to=S.RUNNING)
