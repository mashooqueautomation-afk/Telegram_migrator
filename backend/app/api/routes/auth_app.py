"""Sign-in for the web app itself (separate from connecting a Telegram account)."""
from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import auth_throttle, client_ip, get_current_user, get_db, get_settings_dep
from app.core.config import Settings
from app.core.errors import ForbiddenError
from app.core.security import create_access_token
from app.models import User
from app.schemas.auth import LoginIn, RegisterIn, TokenOut, UserOut
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=["app auth"])


@router.post("/register", response_model=UserOut, status_code=201, dependencies=[Depends(auth_throttle("register"))],
             summary="Create an app account (disable with ALLOW_REGISTRATION=false)")
async def register(body: RegisterIn, request: Request, db: AsyncSession = Depends(get_db),
                   settings: Settings = Depends(get_settings_dep)):
    if not settings.allow_registration:
        raise ForbiddenError("Registration is disabled on this server.", code="registration_disabled")
    return await auth_service.register_user(db, email=body.email, password=body.password.get_secret_value(),
                                            ip=client_ip(request))


@router.post("/login", response_model=TokenOut, dependencies=[Depends(auth_throttle("login"))],
             summary="Exchange e-mail + password for a bearer token")
async def login(body: LoginIn, request: Request, db: AsyncSession = Depends(get_db),
                settings: Settings = Depends(get_settings_dep)):
    user = await auth_service.authenticate(db, email=body.email, password=body.password.get_secret_value(),
                                           ip=client_ip(request))
    token, expires_in = create_access_token(user_id=user.id, secret=settings.jwt_secret.get_secret_value(),
                                            algorithm=settings.jwt_algorithm,
                                            ttl_minutes=settings.access_token_ttl_minutes)
    return TokenOut(access_token=token, expires_in=expires_in)


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(get_current_user)):
    return user
