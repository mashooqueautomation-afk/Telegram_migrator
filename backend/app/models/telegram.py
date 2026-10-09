from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, CreatedAt, UTCDateTime, UUIDPk, utcnow
from app.domain import AccountStatus, ChannelKind, LoginState


def _enum(e):
    return Enum(e, native_enum=False, length=32, create_constraint=True, validate_strings=True,
                values_callable=lambda x: [m.value for m in x])


class TelegramAccount(Base, UUIDPk, CreatedAt):
    """An authorised Telegram user session. The session string is stored ONLY as Fernet ciphertext."""

    __tablename__ = "telegram_accounts"
    __table_args__ = (UniqueConstraint("user_id", "telegram_user_id"),)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str | None] = mapped_column(String(64))
    first_name: Mapped[str | None] = mapped_column(String(128))
    session_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[AccountStatus] = mapped_column(_enum(AccountStatus), default=AccountStatus.ACTIVE, nullable=False)
    flood_wait_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_used_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    channels: Mapped[list["TelegramChannel"]] = relationship(back_populates="account", cascade="all, delete-orphan",
                                                             passive_deletes=True)


class TelegramLoginAttempt(Base, UUIDPk, CreatedAt):
    """In-flight MTProto login. Holds the phone/code-hash/temporary session ENCRYPTED, never the code or 2FA password."""

    __tablename__ = "telegram_login_attempts"

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    state: Mapped[LoginState] = mapped_column(_enum(LoginState), default=LoginState.AWAITING_CODE, nullable=False)
    phone_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    phone_masked: Mapped[str] = mapped_column(String(32), nullable=False)
    phone_code_hash_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    temp_session_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    failed_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)


class TelegramChannel(Base, UUIDPk, CreatedAt):
    __tablename__ = "telegram_channels"
    __table_args__ = (UniqueConstraint("account_id", "telegram_id"),)

    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"),
                                                  index=True, nullable=False)
    telegram_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    access_hash: Mapped[int | None] = mapped_column(BigInteger)
    kind: Mapped[ChannelKind] = mapped_column(_enum(ChannelKind), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str | None] = mapped_column(String(64))
    participants_count: Mapped[int | None] = mapped_column(Integer)
    is_creator: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    can_invite_users: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    refreshed_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    account: Mapped[TelegramAccount] = relationship(back_populates="channels")
