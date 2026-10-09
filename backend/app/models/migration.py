from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, Enum, ForeignKey, Index, Integer, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, CreatedAt, JSONType, UTCDateTime, UUIDPk, utcnow
from app.domain import AnalysisState, Eligibility, InviteOutcome, JobStatus, MemberState, PauseReason


def _enum(e):
    return Enum(e, native_enum=False, length=32, create_constraint=True, validate_strings=True,
                values_callable=lambda x: [m.value for m in x])


class MigrationJob(Base, UUIDPk, CreatedAt):
    __tablename__ = "migration_jobs"
    __table_args__ = (
        Index("ix_migration_jobs_user_id_created_at", "user_id", "created_at"),
        Index("ix_migration_jobs_status", "status"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="SET NULL"))
    source_channel_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("telegram_channels.id", ondelete="SET NULL"))
    destination_channel_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("telegram_channels.id", ondelete="SET NULL"))
    retry_of_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("migration_jobs.id", ondelete="SET NULL"))
    # Snapshots so the record stays meaningful if a channel row is removed.
    source_title: Mapped[str] = mapped_column(String(255), nullable=False)
    destination_title: Mapped[str] = mapped_column(String(255), nullable=False)

    # ---- lifecycle
    status: Mapped[JobStatus] = mapped_column(_enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    pause_reason: Mapped[PauseReason | None] = mapped_column(_enum(PauseReason))
    pause_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    cancellation_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    error_message: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(String(48))

    # ---- progress counters (updated with SQL increments in the same transaction as the member result)
    total_members: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processed_members: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    successful_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rate_limited_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    current_member_id: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    current_member_label: Mapped[str | None] = mapped_column(String(255))
    rate_limited_until: Mapped[datetime | None] = mapped_column(UTCDateTime)

    # ---- worker coordination
    worker_id: Mapped[str | None] = mapped_column(String(64))
    heartbeat_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    event_seq: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)

    # ---- Stage 1 analysis
    analysis_state: Mapped[AnalysisState] = mapped_column(_enum(AnalysisState), default=AnalysisState.NOT_ANALYZED,
                                                          nullable=False)
    analysis_note: Mapped[str | None] = mapped_column(Text)
    member_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    source_reported_total: Mapped[int | None] = mapped_column(Integer)
    source_listing_complete: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    destination_listing_complete: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    destination_banned_known: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    analysis_finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False)

    members: Mapped[list["MigrationMember"]] = relationship(back_populates="job", cascade="all, delete-orphan",
                                                            passive_deletes=True)
    events: Mapped[list["MigrationEvent"]] = relationship(back_populates="job", cascade="all, delete-orphan",
                                                          passive_deletes=True)


class MigrationMember(Base, UUIDPk, CreatedAt):
    __tablename__ = "migration_members"
    __table_args__ = (
        UniqueConstraint("job_id", "telegram_user_id"),  # one row per Telegram user per job -> idempotency anchor
        Index("ix_migration_members_job_id_eligibility", "job_id", "eligibility"),
        Index("ix_migration_members_claim", "job_id", "state", "position"),
    )

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("migration_jobs.id", ondelete="CASCADE"), nullable=False)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    access_hash: Mapped[int | None] = mapped_column(BigInteger)  # internal; never returned by the API
    username: Mapped[str | None] = mapped_column(String(64))
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(16), default="unknown", nullable=False)
    position: Mapped[int] = mapped_column(Integer, default=0, nullable=False)   # processing order
    selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    eligibility: Mapped[Eligibility] = mapped_column(_enum(Eligibility), nullable=False)
    state: Mapped[MemberState] = mapped_column(_enum(MemberState), default=MemberState.DISCOVERED, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)                     # why the eligibility analysis said so

    outcome: Mapped[InviteOutcome | None] = mapped_column(_enum(InviteOutcome))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    invite_attempted_at: Mapped[datetime | None] = mapped_column(UTCDateTime)  # set BEFORE we ask Telegram to invite
    processed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    telegram_error_code: Mapped[str | None] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False)

    job: Mapped[MigrationJob] = relationship(back_populates="members")


class MigrationEvent(Base, UUIDPk, CreatedAt):
    __tablename__ = "migration_events"
    __table_args__ = (
        UniqueConstraint("job_id", "seq"),
        Index("ix_migration_events_job_id_created_at", "job_id", "created_at"),
    )

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("migration_jobs.id", ondelete="CASCADE"), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)          # per-job monotonic; SSE Last-Event-ID
    member_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("migration_members.id", ondelete="SET NULL"))
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    status: Mapped[str | None] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(String(64))
    data: Mapped[dict | None] = mapped_column(JSONType)                    # exposed as "metadata"; never credentials

    job: Mapped[MigrationJob] = relationship(back_populates="events")


class AuditLog(Base, UUIDPk, CreatedAt):
    """Append-only security/audit trail. Survives user deletion (user_id -> NULL)."""

    __tablename__ = "audit_logs"
    __table_args__ = (Index("ix_audit_logs_user_id_created_at", "user_id", "created_at"),)

    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    resource_type: Mapped[str | None] = mapped_column(String(48))
    resource_id: Mapped[str | None] = mapped_column(String(64))
    ip_address: Mapped[str | None] = mapped_column(String(45))
    details: Mapped[dict | None] = mapped_column(JSONType)
