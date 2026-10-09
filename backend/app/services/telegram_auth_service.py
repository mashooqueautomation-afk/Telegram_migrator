"""Orchestrates the Telegram login flow and its persistence. Telegram calls live in app/telegram/auth.py."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import ConflictError, NotFoundError, TooManyRequestsError
from app.core.security import SessionVault
from app.db.base import utcnow
from app.domain import AccountStatus, LoginState
from app.models import TelegramAccount, TelegramLoginAttempt, User
from app.services.audit import record_audit
from app.telegram import auth as tg_auth
from app.telegram.errors import (InvalidLoginCode, InvalidTwoFactorPassword, LoginCodeExpired, PhoneNumberBanned,
                                 PhoneNumberInvalid, TwoFactorRequired)


@dataclass
class VerifyOutcome:
    account: TelegramAccount | None = None
    two_factor_required: bool = False
    attempt: TelegramLoginAttempt | None = None


async def start_login(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory, user: User,
                      phone: str, ip: str | None) -> tuple[TelegramLoginAttempt, str]:
    challenge = await tg_auth.request_login_code(factory, phone, timeout=settings.telegram_login_timeout_seconds)  # may raise (flood, invalid phone, ...)
    await db.execute(delete(TelegramLoginAttempt).where(TelegramLoginAttempt.user_id == user.id))
    attempt = TelegramLoginAttempt(
        user_id=user.id,
        phone_encrypted=vault.encrypt(phone),
        phone_masked=tg_auth.mask_phone(phone),
        phone_code_hash_encrypted=vault.encrypt(challenge.phone_code_hash),
        temp_session_encrypted=vault.encrypt(challenge.temp_session),
        expires_at=utcnow() + timedelta(seconds=settings.login_attempt_ttl_seconds),
    )
    db.add(attempt)
    await db.flush()
    await record_audit(db, action="telegram.login_started", user_id=user.id, resource_type="login_attempt",
                       resource_id=attempt.id, ip=ip, details={"delivery": challenge.delivery})
    await db.commit()
    return attempt, challenge.delivery


async def _load_attempt(db: AsyncSession, user: User, login_id: uuid.UUID) -> TelegramLoginAttempt:
    attempt = (await db.execute(select(TelegramLoginAttempt).where(
        TelegramLoginAttempt.id == login_id, TelegramLoginAttempt.user_id == user.id))).scalar_one_or_none()
    if attempt is None:
        raise NotFoundError("No pending Telegram login. Start again.", code="login_not_found")
    if attempt.expires_at <= utcnow():
        await db.delete(attempt)
        await db.commit()
        raise ConflictError("This login attempt expired. Start again.", code="login_expired", status_code=410)
    return attempt


async def _fail_attempt(db: AsyncSession, attempt: TelegramLoginAttempt, *, limit: int, user: User,
                        ip: str | None, action: str) -> None:
    """Count a wrong code/password; drop the attempt once the limit is hit. Always commits."""
    attempt.failed_attempts += 1
    exhausted = attempt.failed_attempts >= limit
    await record_audit(db, action=action, user_id=user.id, resource_type="login_attempt",
                       resource_id=attempt.id, ip=ip, details={"failed_attempts": attempt.failed_attempts})
    if exhausted:
        await db.delete(attempt)
    await db.commit()
    if exhausted:
        raise TooManyRequestsError("Too many incorrect attempts. Start the Telegram login again.",
                                   code="too_many_login_attempts")


async def _finalize(db: AsyncSession, *, vault: SessionVault, user: User, attempt: TelegramLoginAttempt,
                    result: tg_auth.LoginResult, ip: str | None) -> TelegramAccount:
    account = (await db.execute(select(TelegramAccount).where(
        TelegramAccount.user_id == user.id,
        TelegramAccount.telegram_user_id == result.telegram_user_id))).scalar_one_or_none()
    encrypted = vault.encrypt(result.session_string)
    if account is None:
        account = TelegramAccount(user_id=user.id, telegram_user_id=result.telegram_user_id,
                                  session_encrypted=encrypted)
        db.add(account)
    account.session_encrypted = encrypted
    account.username = result.username
    account.first_name = result.first_name
    account.status = AccountStatus.ACTIVE
    account.flood_wait_until = None
    await db.delete(attempt)
    await db.flush()
    await record_audit(db, action="telegram.login_succeeded", user_id=user.id, resource_type="telegram_account",
                       resource_id=account.id, ip=ip, details={"telegram_user_id": result.telegram_user_id})
    await db.commit()
    return account


async def verify_code(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory, user: User,
                      login_id: uuid.UUID, code: str, ip: str | None) -> VerifyOutcome:
    attempt = await _load_attempt(db, user, login_id)
    if attempt.state is not LoginState.AWAITING_CODE:
        raise ConflictError("This login is waiting for the two-step verification password.",
                            code="login_wrong_step")
    try:
        result = await tg_auth.complete_login_with_code(
            factory,
            temp_session=vault.decrypt(attempt.temp_session_encrypted),
            phone=vault.decrypt(attempt.phone_encrypted),
            code=code,
            phone_code_hash=vault.decrypt(attempt.phone_code_hash_encrypted),
            timeout=settings.telegram_login_timeout_seconds,
        )
    except TwoFactorRequired as tf:
        attempt.state = LoginState.AWAITING_PASSWORD
        attempt.temp_session_encrypted = vault.encrypt(tf.temp_session)
        await record_audit(db, action="telegram.login_2fa_required", user_id=user.id,
                           resource_type="login_attempt", resource_id=attempt.id, ip=ip)
        await db.commit()
        return VerifyOutcome(two_factor_required=True, attempt=attempt)
    except InvalidLoginCode:
        await _fail_attempt(db, attempt, limit=settings.max_code_attempts, user=user, ip=ip,
                            action="telegram.login_code_failed")
        raise
    except (LoginCodeExpired, PhoneNumberBanned, PhoneNumberInvalid):
        await db.delete(attempt)
        await db.commit()
        raise
    account = await _finalize(db, vault=vault, user=user, attempt=attempt, result=result, ip=ip)
    return VerifyOutcome(account=account)


async def verify_password(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory, user: User,
                          login_id: uuid.UUID, password: str, ip: str | None) -> VerifyOutcome:
    attempt = await _load_attempt(db, user, login_id)
    if attempt.state is not LoginState.AWAITING_PASSWORD:
        raise ConflictError("Submit the login code first.", code="login_wrong_step")
    try:
        result = await tg_auth.complete_login_with_password(
            factory, temp_session=vault.decrypt(attempt.temp_session_encrypted), password=password,
            timeout=settings.telegram_login_timeout_seconds)
    except InvalidTwoFactorPassword:
        await _fail_attempt(db, attempt, limit=settings.max_password_attempts, user=user, ip=ip,
                            action="telegram.login_password_failed")
        raise
    account = await _finalize(db, vault=vault, user=user, attempt=attempt, result=result, ip=ip)
    return VerifyOutcome(account=account)


async def disconnect_account(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory, user: User,
                             account: TelegramAccount, ip: str | None) -> bool:
    """Log out remotely (best effort) and delete the stored session + discovered channels."""
    revoked = await tg_auth.revoke_session(factory, vault.decrypt(account.session_encrypted),
                                           timeout=settings.telegram_login_timeout_seconds)
    await record_audit(db, action="telegram.account_disconnected", user_id=user.id,
                       resource_type="telegram_account", resource_id=account.id, ip=ip,
                       details={"revoked_remotely": revoked})
    await db.delete(account)
    await db.commit()
    return revoked
