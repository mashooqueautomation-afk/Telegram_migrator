import uuid

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import client_ip, get_client_factory, get_current_user, get_db, get_settings_dep, get_vault
from app.core.config import Settings
from app.core.security import SessionVault
from app.models import User
from app.schemas.telegram import ChannelOut, MemberListingOut, MemberOut
from app.services import telegram_service as svc

router = APIRouter(prefix="/telegram", tags=["telegram data"])

_LISTING_NOTES = [
    "Only members Telegram's API returns to an administrator are listed; large channels may be truncated.",
    "Telegram does not reveal privacy settings, so whether an invitation will succeed is unknown at this stage.",
]


@router.get("/channels", response_model=list[ChannelOut],
            summary="Channels/groups the connected account can see (administered ones by default)")
async def list_channels(request: Request, account_id: uuid.UUID | None = None,
                        administered_only: bool = True,
                        refresh: bool = Query(True, description="false = return the last discovered list from the DB "
                                                                "without contacting Telegram"),
                        user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db),
                        settings: Settings = Depends(get_settings_dep), vault: SessionVault = Depends(get_vault),
                        factory=Depends(get_client_factory)):
    account = await svc.get_account(db, user.id, account_id)
    return await svc.discover_channels(db, settings=settings, vault=vault, factory=factory, account=account,
                                       administered_only=administered_only, refresh=refresh, ip=client_ip(request))


@router.get("/channels/{channel_id}/members", response_model=MemberListingOut,
            summary="Live member listing for a channel you administer (not persisted)")
async def list_members(channel_id: uuid.UUID, request: Request,
                       limit: int | None = Query(None, ge=1, description="Defaults to MEMBER_FETCH_DEFAULT_LIMIT"),
                       user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db),
                       settings: Settings = Depends(get_settings_dep), vault: SessionVault = Depends(get_vault),
                       factory=Depends(get_client_factory)):
    channel, listing = await svc.list_channel_members(
        db, settings=settings, vault=vault, factory=factory, user_id=user.id, channel_id=channel_id,
        limit=limit or settings.member_fetch_default_limit, ip=client_ip(request))
    notes = list(_LISTING_NOTES)
    if not listing.complete:
        notes.insert(0, f"This listing is incomplete ({listing.incomplete_reason}).")
    return MemberListingOut(
        channel_id=channel.id,
        members=[MemberOut(telegram_user_id=m.telegram_user_id, username=m.username, display_name=m.display_name,
                           role=m.role, is_bot=m.is_bot, is_deleted=m.is_deleted, is_restricted=m.is_restricted)
                 for m in listing.members],
        returned=len(listing.members), reported_total=listing.reported_total, complete=listing.complete,
        incomplete_reason=listing.incomplete_reason, rate_limited_for_seconds=listing.rate_limited_for, notes=notes)
