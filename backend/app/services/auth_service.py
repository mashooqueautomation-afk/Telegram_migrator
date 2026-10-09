from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, UnauthorizedError
from app.core.security import hash_password, password_needs_rehash, verify_password
from app.db.base import utcnow
from app.models import User
from app.services.audit import record_audit


async def register_user(db: AsyncSession, *, email: str, password: str, ip: str | None) -> User:
    email = email.strip().lower()
    user = User(email=email, password_hash=hash_password(password))
    db.add(user)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("An account with this e-mail already exists.", code="email_taken") from exc
    await record_audit(db, action="user.register", user_id=user.id, ip=ip)
    await db.commit()
    return user


async def authenticate(db: AsyncSession, *, email: str, password: str, ip: str | None) -> User:
    user = (await db.execute(select(User).where(User.email == email.strip().lower()))).scalar_one_or_none()
    ok = verify_password(password, user.password_hash if user else None)  # constant-ish cost either way
    if not user or not ok or not user.is_active:
        await record_audit(db, action="user.login_failed", user_id=user.id if user else None, ip=ip)
        await db.commit()
        raise UnauthorizedError("Incorrect e-mail or password.", code="invalid_credentials")
    if password_needs_rehash(user.password_hash):
        user.password_hash = hash_password(password)
    user.last_login_at = utcnow()
    await record_audit(db, action="user.login", user_id=user.id, ip=ip)
    await db.commit()
    return user
