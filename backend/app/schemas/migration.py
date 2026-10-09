from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.domain import AnalysisState, Eligibility, InviteOutcome, JobStatus, MemberState, PauseReason
from app.schemas.common import ORMModel


# ------------------------------------------------------------------ requests
class EligibilityIn(BaseModel):
    """Stage 1 one-shot: create a job AND analyse it inside the request (small jobs)."""
    account_id: uuid.UUID | None = None
    source_channel_id: uuid.UUID
    destination_channel_id: uuid.UUID
    member_limit: int | None = None


class MigrationCreateIn(BaseModel):
    account_id: uuid.UUID | None = None
    source_channel_id: uuid.UUID
    destination_channel_id: uuid.UUID
    member_limit: int | None = Field(None, ge=1, description="Max source members to read (server cap applies)")


class StartIn(BaseModel):
    acknowledge_partial: bool = Field(
        False, description="Required when the analysis is PARTIAL (a member list was truncated or rate-limited)")


class SelectionIn(BaseModel):
    telegram_user_ids: list[int] | None = Field(
        None, max_length=5000, description="null = every member whose eligibility is ELIGIBLE")
    selected: bool = True


class SelectionOut(BaseModel):
    updated: int
    ignored: int = Field(description="Requested members that are not ELIGIBLE or not in this job")
    selected_total: int


# ------------------------------------------------------------------ responses
class ChannelRefOut(BaseModel):
    id: uuid.UUID | None
    title: str


class JobSummary(BaseModel):
    total_analyzed: int
    by_eligibility: dict[str, int]


class CurrentMember(BaseModel):
    id: uuid.UUID
    label: str | None


class EventOut(ORMModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    seq: int
    event_type: str
    message: str
    status: str | None
    reason: str | None
    error_code: str | None
    metadata: dict | None = Field(None, validation_alias="data")
    member_id: uuid.UUID | None
    created_at: datetime


class JobOut(BaseModel):
    id: uuid.UUID
    status: JobStatus
    pause_reason: PauseReason | None
    pause_requested: bool
    cancellation_requested: bool
    retry_of_id: uuid.UUID | None
    source: ChannelRefOut
    destination: ChannelRefOut
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    # progress
    total_members: int
    processed_members: int
    successful_count: int
    skipped_count: int
    failed_count: int
    rate_limited_count: int
    remaining_members: int
    percent_complete: float
    current_member: CurrentMember | None
    error_message: str | None
    error_code: str | None

    # rate limiting / worker
    rate_limited_until: datetime | None
    rate_limit_remaining_seconds: int
    worker_active: bool
    allowed_actions: list[str]
    outcome_counts: dict[str, int]

    # Stage 1 analysis
    analysis_state: AnalysisState
    analysis_note: str | None
    member_limit: int
    source_reported_total: int | None
    source_listing_complete: bool
    destination_listing_complete: bool
    destination_banned_known: bool
    analysis_finished_at: datetime | None
    summary: JobSummary
    notes: list[str]
    recent_events: list[EventOut]


class JobListItem(BaseModel):
    id: uuid.UUID
    status: JobStatus
    pause_reason: PauseReason | None
    analysis_state: AnalysisState
    source_title: str
    destination_title: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    total_members: int
    processed_members: int
    successful_count: int
    skipped_count: int
    failed_count: int
    percent_complete: float
    rate_limited_until: datetime | None


class JobMemberOut(ORMModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    telegram_user_id: int
    username: str | None
    display_name: str
    role: str
    selected: bool
    eligibility: Eligibility
    eligibility_status: Eligibility = Field(validation_alias="eligibility")
    state: MemberState
    processing_status: MemberState = Field(validation_alias="state")
    reason: str | None
    outcome: InviteOutcome | None
    attempt_count: int
    last_attempt_at: datetime | None
    processed_at: datetime | None
    failure_reason: str | None
    telegram_error_code: str | None
