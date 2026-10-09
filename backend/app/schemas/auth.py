from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, SecretStr, field_validator

from app.domain import AccountStatus
from app.schemas.common import ORMModel
from app.telegram.auth import normalize_code, normalize_phone


class RegisterIn(BaseModel):
    email: EmailStr
    password: SecretStr = Field(min_length=12, max_length=128)


class LoginIn(BaseModel):
    email: EmailStr
    password: SecretStr = Field(min_length=1, max_length=128)


class UserOut(ORMModel):
    id: uuid.UUID
    email: str
    created_at: datetime


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class TelegramStartIn(BaseModel):
    phone: SecretStr

    @field_validator("phone", mode="before")
    @classmethod
    def _phone(cls, v):
        raw = v.get_secret_value() if isinstance(v, SecretStr) else v
        return normalize_phone(str(raw))


class TelegramStartOut(BaseModel):
    login_id: uuid.UUID
    phone_masked: str
    delivery: str
    expires_in_seconds: int
    next_step: str = "verify"


class TelegramVerifyIn(BaseModel):
    login_id: uuid.UUID
    code: SecretStr

    @field_validator("code", mode="before")
    @classmethod
    def _code(cls, v):
        raw = v.get_secret_value() if isinstance(v, SecretStr) else v
        return normalize_code(str(raw))


class TelegramPasswordIn(BaseModel):
    login_id: uuid.UUID
    password: SecretStr = Field(min_length=1, max_length=256)


class AccountOut(ORMModel):
    id: uuid.UUID
    telegram_user_id: int
    username: str | None
    first_name: str | None
    status: AccountStatus
    created_at: datetime
    last_used_at: datetime | None
    flood_wait_until: datetime | None


class TelegramVerifyOut(BaseModel):
    status: str  # "connected" | "two_factor_required"
    login_id: uuid.UUID | None = None
    account: AccountOut | None = None
    next_step: str | None = None
