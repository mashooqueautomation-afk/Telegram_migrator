"""Official MTProto login: phone -> code -> optional 2FA password.

The code and password are forwarded to Telegram and never stored or logged.
"""
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (auth_throttle, client_ip, get_client_factory, get_current_user, get_db, get_settings_dep,
                          get_vault)
from app.core.config import Settings
from app.core.security import SessionVault
from app.models import TelegramAccount, User
from app.schemas.auth import (AccountOut, TelegramPasswordIn, TelegramStartIn, TelegramStartOut, TelegramVerifyIn,
                              TelegramVerifyOut)
from app.services import telegram_auth_service as svc
from app.services.telegram_service import get_account

router = APIRouter(tags=["telegram auth"])


@router.post("/auth/telegram/start", response_model=TelegramStartOut,
             dependencies=[Depends(auth_throttle("tg-start"))],
             summary="Step 1: send a login code to the Telegram account's own devices")
async def telegram_start(body: TelegramStartIn, request: Request, user: User = Depends(get_current_user),
                         db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                         vault: SessionVault = Depends(get_vault), factory=Depends(get_client_factory)):
    attempt, delivery = await svc.start_login(db, settings=settings, vault=vault, factory=factory, user=user,
                                              phone=body.phone.get_secret_value(), ip=client_ip(request))
    return TelegramStartOut(login_id=attempt.id, phone_masked=attempt.phone_masked, delivery=delivery,
                            expires_in_seconds=settings.login_attempt_ttl_seconds)


@router.post("/auth/telegram/verify", response_model=TelegramVerifyOut,
             dependencies=[Depends(auth_throttle("tg-verify"))],
             summary="Step 2: submit the login code (may answer two_factor_required)")
async def telegram_verify(body: TelegramVerifyIn, request: Request, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                          vault: SessionVault = Depends(get_vault), factory=Depends(get_client_factory)):
    out = await svc.verify_code(db, settings=settings, vault=vault, factory=factory, user=user,
                                login_id=body.login_id, code=body.code.get_secret_value(), ip=client_ip(request))
    if out.two_factor_required:
        return TelegramVerifyOut(status="two_factor_required", login_id=body.login_id, next_step="2fa")
    return TelegramVerifyOut(status="connected", account=AccountOut.model_validate(out.account))


@router.post("/auth/telegram/2fa", response_model=TelegramVerifyOut,
             dependencies=[Depends(auth_throttle("tg-2fa"))],
             summary="Step 3 (only if required): submit the two-step verification password")
async def telegram_2fa(body: TelegramPasswordIn, request: Request, user: User = Depends(get_current_user),
                       db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                       vault: SessionVault = Depends(get_vault), factory=Depends(get_client_factory)):
    out = await svc.verify_password(db, settings=settings, vault=vault, factory=factory, user=user,
                                    login_id=body.login_id, password=body.password.get_secret_value(),
                                    ip=client_ip(request))
    return TelegramVerifyOut(status="connected", account=AccountOut.model_validate(out.account))


@router.get("/telegram/accounts", response_model=list[AccountOut], summary="Connected Telegram accounts")
async def list_accounts(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(TelegramAccount).where(TelegramAccount.user_id == user.id)
                             .order_by(TelegramAccount.created_at))).scalars().all()
    return rows


@router.delete("/telegram/accounts/{account_id}", status_code=204,
               summary="Disconnect: log the session out on Telegram and delete it here")
async def disconnect_account(account_id: str, request: Request, user: User = Depends(get_current_user),
                             db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                             vault: SessionVault = Depends(get_vault), factory=Depends(get_client_factory)):
    import uuid

    from app.core.errors import NotFoundError
    try:
        parsed = uuid.UUID(account_id)
    except ValueError as exc:
        raise NotFoundError("Telegram account not found.", code="account_not_found") from exc
    account = await get_account(db, user.id, parsed)
    await svc.disconnect_account(db, settings=settings, vault=vault, factory=factory, user=user, account=account, ip=client_ip(request))
