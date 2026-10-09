"""Stage 2: migration engine (job lifecycle/counters/leases, member processing fields, event sequencing)

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-05
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TS = sa.DateTime(timezone=True)

JOB_STATUS_OLD = ["PENDING", "RUNNING", "PAUSED", "COMPLETED", "CANCELLED", "FAILED"]
JOB_STATUS_NEW = ["PENDING", "RUNNING", "PAUSED", "CANCELLING", "COMPLETED", "CANCELLED", "FAILED"]
ANALYSIS_OLD = ["ANALYZING", "COMPLETED", "PARTIAL", "FAILED"]
ANALYSIS_NEW = ["NOT_ANALYZED", *ANALYSIS_OLD]
MEMBER_OLD = ["DISCOVERED", "ELIGIBLE", "PROCESSING", "SUCCESS", "SKIPPED", "FAILED", "RATE_LIMITED"]
MEMBER_NEW = ["DISCOVERED", "ELIGIBLE", "PROCESSING", "SUCCESS", "ALREADY_MEMBER", "SKIPPED", "FAILED", "RATE_LIMITED"]
PAUSE_REASONS = ["USER", "RATE_LIMIT", "ERRORS"]
OUTCOMES = ["SUCCESS", "ALREADY_MEMBER", "PRIVACY_RESTRICTED", "INVALID_USER", "USER_RESTRICTED", "ADMIN_REQUIRED",
            "CHANNEL_RESTRICTED", "ACCOUNT_RESTRICTED", "SESSION_INVALID", "FLOOD_WAIT", "TEMPORARY_ERROR",
            "NOT_CONFIRMED", "UNKNOWN_ERROR"]


def _in(column: str, values: list[str]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def upgrade() -> None:
    # ------------------------------------------------------------------ migration_jobs
    with op.batch_alter_table("migration_jobs") as b:
        b.add_column(sa.Column("retry_of_id", sa.Uuid(), nullable=True))
        b.add_column(sa.Column("pause_reason", sa.String(32), nullable=True))
        b.add_column(sa.Column("pause_requested", sa.Boolean(), nullable=False, server_default=sa.false()))
        b.add_column(sa.Column("cancellation_requested", sa.Boolean(), nullable=False, server_default=sa.false()))
        b.add_column(sa.Column("started_at", TS, nullable=True))
        b.add_column(sa.Column("completed_at", TS, nullable=True))
        b.add_column(sa.Column("error_message", sa.Text(), nullable=True))
        b.add_column(sa.Column("error_code", sa.String(48), nullable=True))
        for name in ("total_members", "processed_members", "successful_count", "skipped_count", "failed_count",
                     "rate_limited_count"):
            b.add_column(sa.Column(name, sa.Integer(), nullable=False, server_default=sa.text("0")))
        b.add_column(sa.Column("current_member_id", sa.Uuid(), nullable=True))
        b.add_column(sa.Column("current_member_label", sa.String(255), nullable=True))
        b.add_column(sa.Column("worker_id", sa.String(64), nullable=True))
        b.add_column(sa.Column("heartbeat_at", TS, nullable=True))
        b.add_column(sa.Column("event_seq", sa.BigInteger(), nullable=False, server_default=sa.text("0")))
        b.create_foreign_key(op.f("fk_migration_jobs_retry_of_id_migration_jobs"), "migration_jobs",
                             ["retry_of_id"], ["id"], ondelete="SET NULL")
        b.create_index("ix_migration_jobs_status", ["status"])
        b.drop_constraint(op.f("ck_migration_jobs_jobstatus"), type_="check")
        b.drop_constraint(op.f("ck_migration_jobs_analysisstate"), type_="check")
        b.create_check_constraint(op.f("ck_migration_jobs_jobstatus"), _in("status", JOB_STATUS_NEW))
        b.create_check_constraint(op.f("ck_migration_jobs_analysisstate"), _in("analysis_state", ANALYSIS_NEW))
        b.create_check_constraint(op.f("ck_migration_jobs_pausereason"),
                                  "pause_reason IS NULL OR " + _in("pause_reason", PAUSE_REASONS))

    # ------------------------------------------------------------------ migration_members
    with op.batch_alter_table("migration_members") as b:
        b.add_column(sa.Column("position", sa.Integer(), nullable=False, server_default=sa.text("0")))
        b.add_column(sa.Column("selected", sa.Boolean(), nullable=False, server_default=sa.false()))
        b.add_column(sa.Column("outcome", sa.String(32), nullable=True))
        b.add_column(sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")))
        b.add_column(sa.Column("last_attempt_at", TS, nullable=True))
        b.add_column(sa.Column("invite_attempted_at", TS, nullable=True))
        b.add_column(sa.Column("processed_at", TS, nullable=True))
        b.add_column(sa.Column("failure_reason", sa.Text(), nullable=True))
        b.add_column(sa.Column("telegram_error_code", sa.String(64), nullable=True))
        b.create_index("ix_migration_members_claim", ["job_id", "state", "position"])
        b.drop_constraint(op.f("ck_migration_members_memberstate"), type_="check")
        b.create_check_constraint(op.f("ck_migration_members_memberstate"), _in("state", MEMBER_NEW))
        b.create_check_constraint(op.f("ck_migration_members_inviteoutcome"),
                                  "outcome IS NULL OR " + _in("outcome", OUTCOMES))

    # ------------------------------------------------------------------ migration_events
    with op.batch_alter_table("migration_events") as b:
        b.add_column(sa.Column("seq", sa.BigInteger(), nullable=True))
        b.add_column(sa.Column("status", sa.String(32), nullable=True))
        b.add_column(sa.Column("reason", sa.Text(), nullable=True))
        b.add_column(sa.Column("error_code", sa.String(64), nullable=True))

    # ------------------------------------------------------------------ backfill Stage 1 rows
    op.execute("""
        UPDATE migration_events SET seq = (
            SELECT COUNT(*) FROM migration_events e2
            WHERE e2.job_id = migration_events.job_id
              AND (e2.created_at < migration_events.created_at
                   OR (e2.created_at = migration_events.created_at AND e2.id <= migration_events.id))
        )
    """)
    op.execute("""
        UPDATE migration_jobs SET event_seq = COALESCE(
            (SELECT MAX(seq) FROM migration_events e WHERE e.job_id = migration_jobs.id), 0)
    """)
    op.execute("UPDATE migration_members SET selected = TRUE WHERE eligibility = 'ELIGIBLE'"
               if op.get_bind().dialect.name == "postgresql"
               else "UPDATE migration_members SET selected = 1 WHERE eligibility = 'ELIGIBLE'")
    op.execute("""
        UPDATE migration_jobs SET total_members = (
            SELECT COUNT(*) FROM migration_members m WHERE m.job_id = migration_jobs.id AND m.eligibility = 'ELIGIBLE')
    """)

    with op.batch_alter_table("migration_events") as b:
        b.alter_column("seq", existing_type=sa.BigInteger(), nullable=False)
        b.create_unique_constraint("uq_migration_events_job_id", ["job_id", "seq"])


def downgrade() -> None:
    # Map values the old constraints don't know about before restoring them.
    op.execute("UPDATE migration_jobs SET status = 'CANCELLED' WHERE status = 'CANCELLING'")
    op.execute("UPDATE migration_jobs SET analysis_state = 'FAILED' WHERE analysis_state = 'NOT_ANALYZED'")
    op.execute("UPDATE migration_members SET state = 'SKIPPED' WHERE state = 'ALREADY_MEMBER'")

    with op.batch_alter_table("migration_events") as b:
        b.drop_constraint("uq_migration_events_job_id", type_="unique")
        for c in ("error_code", "reason", "status", "seq"):
            b.drop_column(c)

    with op.batch_alter_table("migration_members") as b:
        b.drop_constraint(op.f("ck_migration_members_inviteoutcome"), type_="check")
        b.drop_constraint(op.f("ck_migration_members_memberstate"), type_="check")
        b.drop_index("ix_migration_members_claim")
        for c in ("telegram_error_code", "failure_reason", "processed_at", "invite_attempted_at", "last_attempt_at",
                  "attempt_count", "outcome", "selected", "position"):
            b.drop_column(c)
        b.create_check_constraint(op.f("ck_migration_members_memberstate"), _in("state", MEMBER_OLD))

    with op.batch_alter_table("migration_jobs") as b:
        b.drop_constraint(op.f("ck_migration_jobs_pausereason"), type_="check")
        b.drop_constraint(op.f("ck_migration_jobs_analysisstate"), type_="check")
        b.drop_constraint(op.f("ck_migration_jobs_jobstatus"), type_="check")
        b.drop_index("ix_migration_jobs_status")
        b.drop_constraint("fk_migration_jobs_retry_of_id_migration_jobs", type_="foreignkey")
        for c in ("event_seq", "heartbeat_at", "worker_id", "current_member_label", "current_member_id",
                  "rate_limited_count", "failed_count", "skipped_count", "successful_count", "processed_members",
                  "total_members", "error_code", "error_message", "completed_at", "started_at",
                  "cancellation_requested", "pause_requested", "pause_reason", "retry_of_id"):
            b.drop_column(c)
        b.create_check_constraint(op.f("ck_migration_jobs_analysisstate"), _in("analysis_state", ANALYSIS_OLD))
        b.create_check_constraint(op.f("ck_migration_jobs_jobstatus"), _in("status", JOB_STATUS_OLD))
