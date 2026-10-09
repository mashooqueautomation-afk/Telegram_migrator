import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.domain import Eligibility
from app.models import (AuditLog, MigrationEvent, MigrationJob, MigrationMember, TelegramAccount, TelegramChannel,
                        User)

BACKEND = Path(__file__).resolve().parent.parent


async def _user(db):
    u = User(email=f"{uuid.uuid4().hex[:6]}@x.test", password_hash="h")
    db.add(u)
    await db.flush()
    return u


async def _job(db):
    u = await _user(db)
    acct = TelegramAccount(user_id=u.id, telegram_user_id=1, session_encrypted="enc")
    db.add(acct)
    await db.flush()
    job = MigrationJob(user_id=u.id, account_id=acct.id, source_title="s", destination_title="d", member_limit=10)
    db.add(job)
    await db.flush()
    return u, acct, job


async def test_defaults_and_timestamps(db):
    u, acct, job = await _job(db)
    await db.commit()
    assert u.created_at.tzinfo is not None and job.status.value == "PENDING"
    assert job.analysis_state.value == "NOT_ANALYZED" and acct.status.value == "ACTIVE"
    assert isinstance(u.id, uuid.UUID)


async def test_member_unique_per_job_is_the_idempotency_anchor(db):
    _, _, job = await _job(db)
    db.add(MigrationMember(job_id=job.id, telegram_user_id=5, display_name="a", eligibility=Eligibility.ELIGIBLE))
    await db.flush()
    db.add(MigrationMember(job_id=job.id, telegram_user_id=5, display_name="dup", eligibility=Eligibility.BOT))
    with pytest.raises(IntegrityError):
        await db.flush()


async def test_unique_email_account_and_channel(db):
    u = await _user(db)
    db.add(User(email=u.email, password_hash="h"))
    with pytest.raises(IntegrityError):
        await db.flush()
    await db.rollback()

    u = await _user(db)
    a = TelegramAccount(user_id=u.id, telegram_user_id=9, session_encrypted="e")
    db.add(a)
    await db.flush()
    db.add(TelegramAccount(user_id=u.id, telegram_user_id=9, session_encrypted="e2"))
    with pytest.raises(IntegrityError):
        await db.flush()
    await db.rollback()


async def test_cascade_deletes(db):
    u, acct, job = await _job(db)
    ch = TelegramChannel(account_id=acct.id, telegram_id=1, kind="CHANNEL", title="c")
    db.add_all([ch, MigrationMember(job_id=job.id, telegram_user_id=1, display_name="m",
                                    eligibility=Eligibility.ELIGIBLE),
                MigrationEvent(job_id=job.id, seq=1, event_type="X", message="m", data={"k": 1}),
                AuditLog(user_id=u.id, action="test", details={"a": 1})])
    await db.commit()
    await db.delete(job)
    await db.commit()
    assert (await db.execute(select(MigrationMember))).first() is None
    assert (await db.execute(select(MigrationEvent))).first() is None
    await db.delete(acct)
    await db.commit()
    assert (await db.execute(select(TelegramChannel))).first() is None          # account -> channels cascade
    await db.delete(await db.get(User, u.id))
    await db.commit()
    audit = (await db.execute(select(AuditLog))).scalar_one()                   # audit trail survives the user
    assert audit.user_id is None and audit.details == {"a": 1}


async def test_deleting_an_account_keeps_job_history_with_titles(db):
    _, acct, job = await _job(db)
    await db.commit()
    await db.delete(acct)
    await db.commit()
    await db.refresh(job)
    assert job.account_id is None and job.source_title == "s"


async def test_bad_enum_value_is_rejected_by_the_database(db):
    from sqlalchemy import text
    _, acct, _ = await _job(db)
    await db.commit()
    with pytest.raises(IntegrityError):
        await db.execute(text("UPDATE telegram_accounts SET status='HACKED'"))


def test_alembic_upgrade_matches_models_and_downgrades(tmp_path):
    """Run the real migrations in a subprocess on a scratch SQLite DB, then `alembic check` for drift."""
    env = {**os.environ, "DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path}/mig.db"}

    def alembic(*args):
        return subprocess.run([sys.executable, "-m", "alembic", *args], cwd=BACKEND, env=env,
                              capture_output=True, text=True)

    up = alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr
    check = alembic("check")
    assert check.returncode == 0, check.stderr + check.stdout
    down = alembic("downgrade", "base")
    assert down.returncode == 0, down.stderr


def test_migration_0002_backfills_existing_stage1_data(tmp_path):
    """Real Stage 1 rows survive the upgrade: events get sequence numbers, eligible members become selectable."""
    import sqlite3
    db_file = tmp_path / "stage1.db"
    env = {**os.environ, "DATABASE_URL": f"sqlite+aiosqlite:///{db_file}"}

    def alembic(*args):
        return subprocess.run([sys.executable, "-m", "alembic", *args], cwd=BACKEND, env=env,
                              capture_output=True, text=True)

    assert alembic("upgrade", "0001").returncode == 0
    uid, jid = uuid.uuid4().hex, uuid.uuid4().hex
    con = sqlite3.connect(db_file)
    con.execute("INSERT INTO users (id, email, password_hash, is_active, created_at) VALUES (?,?,?,1,'2026-10-01 09:00:00')",
                (uid, "old@example.com", "h"))
    con.execute("""INSERT INTO migration_jobs (id, user_id, source_title, destination_title, status, analysis_state,
                   member_limit, source_listing_complete, destination_listing_complete, destination_banned_known,
                   created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (jid, uid, "Src", "Dst", "PENDING", "COMPLETED", 100, 1, 1, 0, "2026-10-01 10:00:00", "2026-10-01 10:00:00"))
    for i, (elig, state) in enumerate([("ELIGIBLE", "ELIGIBLE"), ("ELIGIBLE", "ELIGIBLE"), ("BOT", "SKIPPED"),
                                       ("UNKNOWN", "DISCOVERED")]):
        con.execute("""INSERT INTO migration_members (id, job_id, telegram_user_id, display_name, role, eligibility,
                       state, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (uuid.uuid4().hex, jid, 100 + i, f"M{i}", "member", elig, state, "2026-10-01 10:00:00",
                     "2026-10-01 10:00:00"))
    for i, etype in enumerate(["ANALYSIS_STARTED", "DISCOVERY_TRUNCATED", "ANALYSIS_COMPLETED"]):
        con.execute("INSERT INTO migration_events (id, job_id, event_type, message, created_at) VALUES (?,?,?,?,?)",
                    (uuid.uuid4().hex, jid, etype, etype, f"2026-10-01 10:00:0{i + 1}"))
    con.commit()
    con.close()

    up = alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr

    con = sqlite3.connect(db_file)
    assert [r[0] for r in con.execute("SELECT seq FROM migration_events WHERE job_id=? ORDER BY created_at", (jid,))] == [1, 2, 3]
    assert con.execute("SELECT event_seq, total_members, status, analysis_state FROM migration_jobs").fetchone() == \
        (3, 2, "PENDING", "COMPLETED")
    selected = dict(con.execute("SELECT eligibility, MAX(selected) FROM migration_members GROUP BY eligibility"))
    assert selected == {"ELIGIBLE": 1, "BOT": 0, "UNKNOWN": 0}
    assert con.execute("SELECT MIN(attempt_count), MAX(position) FROM migration_members").fetchone() == (0, 0)
    ddl = " ".join(r[0] for r in con.execute("SELECT sql FROM sqlite_master WHERE name LIKE 'migration_%'"))
    assert "CANCELLING" in ddl and "NOT_ANALYZED" in ddl and "ALREADY_MEMBER" in ddl and "RATE_LIMIT" in ddl
    with pytest.raises(sqlite3.IntegrityError):                       # (job_id, seq) is unique after the upgrade
        con.execute("INSERT INTO migration_events (id, job_id, seq, event_type, message, created_at) VALUES (?,?,?,?,?,?)",
                    (uuid.uuid4().hex, jid, 2, "X", "dup", "2026-10-01 11:00:00"))
    con.close()
    assert alembic("check").returncode == 0
