from __future__ import annotations

from collections.abc import AsyncIterator, Callable

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import TooManyRequestsError, UnauthorizedError
from app.core.security import SessionVault, TokenError, decode_access_token
from app.models import User

_bearer = HTTPBearer(auto_error=False, description="JWT from POST /api/auth/login")


def get_settings_dep(request: Request) -> Settings:
    return request.app.state.settings


def get_vault(request: Request) -> SessionVault:
    return request.app.state.vault


def get_client_factory(request: Request):
    return request.app.state.client_factory


def get_queue(request: Request):
    return request.app.state.queue


def client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


async def get_current_user(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
                           db: AsyncSession = Depends(get_db),
                           settings: Settings = Depends(get_settings_dep)) -> User:
    if creds is None:
        raise UnauthorizedError("Authentication required.", code="not_authenticated")
    try:
        user_id = decode_access_token(creds.credentials, secret=settings.jwt_secret.get_secret_value(),
                                      algorithm=settings.jwt_algorithm)
    except TokenError as exc:
        raise UnauthorizedError("Invalid or expired token.", code="invalid_token") from exc
    user = await db.get(User, user_id)
    if user is None or not user.is_active:
        raise UnauthorizedError("Invalid or expired token.", code="invalid_token")
    return user


def auth_throttle(bucket: str) -> Callable:
    """Per-IP throttle for our own sensitive endpoints (login, Telegram login steps)."""

    async def dep(request: Request) -> None:
        wait = request.app.state.auth_limiter.check(f"{bucket}:{client_ip(request) or 'unknown'}")
        if wait:
            raise TooManyRequestsError("Too many requests. Slow down.", code="rate_limited", retry_after=wait)

    return dep


async def get_current_user_light(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
                                 settings: Settings = Depends(get_settings_dep)) -> User:
    """Same checks as get_current_user, but opens and closes its OWN session so long-lived responses (SSE) don't hold
    a pooled database connection for their whole lifetime."""
    if creds is None:
        raise UnauthorizedError("Authentication required.", code="not_authenticated")
    try:
        user_id = decode_access_token(creds.credentials, secret=settings.jwt_secret.get_secret_value(),
                                      algorithm=settings.jwt_algorithm)
    except TokenError as exc:
        raise UnauthorizedError("Invalid or expired token.", code="invalid_token") from exc
    async with request.app.state.sessionmaker() as db:
        user = await db.get(User, user_id)
    if user is None or not user.is_active:
        raise UnauthorizedError("Invalid or expired token.", code="invalid_token")
    return user
