from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel

from app.domain import ChannelKind
from app.schemas.common import ORMModel


class ChannelOut(ORMModel):
    id: uuid.UUID
    telegram_id: int
    kind: ChannelKind
    title: str
    username: str | None
    participants_count: int | None
    is_creator: bool
    is_admin: bool
    can_invite_users: bool
    refreshed_at: datetime


class MemberOut(BaseModel):
    telegram_user_id: int
    username: str | None
    display_name: str
    role: str
    is_bot: bool
    is_deleted: bool
    is_restricted: bool


class MemberListingOut(BaseModel):
    channel_id: uuid.UUID
    members: list[MemberOut]
    returned: int
    reported_total: int | None
    complete: bool
    incomplete_reason: str | None
    rate_limited_for_seconds: int | None
    notes: list[str]
