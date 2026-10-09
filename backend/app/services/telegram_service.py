"""Channel discovery & live member listing. Business rules here; Telegram calls in app/telegram/."""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.errors import AppError, ForbiddenError, NotFoundError
from app.core.security import DecryptionError, SessionVault
from app.db.base import utcnow
from app.domain import AccountStatus
from app.models import TelegramAccount, TelegramChannel
from app.services.audit import record_audit
from app.telegram.channels import ChannelInfo, fetch_channel, list_channels as tg_list_channels
from app.telegram.client import authorized_client, deadline
from app.telegram.errors import FloodWait, TelegramSessionInvalid
from app.telegram.members import MemberListing, fetch_members
from app.telegram.rate_limit import call_with_flood_policy, cooldown_until, ensure_cooldown_elapsed


# ------------------------------------------------------------------ accounts
async def get_account(db: AsyncSession, user_id: uuid.UUID, account_id: uuid.UUID | None = None) -> TelegramAccount:
    if account_id is not None:
        account = (await db.execute(select(TelegramAccount).where(
            TelegramAccount.id == account_id, TelegramAccount.user_id == user_id))).scalar_one_or_none()
        if account is None:
            raise NotFoundError("Telegram account not found.", code="account_not_found")
        return account
    accounts = (await db.execute(select(TelegramAccount).where(TelegramAccount.user_id == user_id))).scalars().all()
    if not accounts:
        raise NotFoundError("No Telegram account connected yet.", code="no_telegram_account")
    if len(accounts) > 1:
        raise AppError("Several Telegram accounts are connected; pass account_id.", code="account_ambiguous")
    return accounts[0]


def ensure_account_usable(account: TelegramAccount) -> None:
    if account.status is AccountStatus.NEEDS_REAUTH:
        raise TelegramSessionInvalid()
    ensure_cooldown_elapsed(account.flood_wait_until)  # raises FloodWait; nothing is sent to Telegram


def decrypt_session(vault: SessionVault, account: TelegramAccount) -> str:
    try:
        return vault.decrypt(account.session_encrypted)
    except DecryptionError as exc:
        raise AppError("The stored Telegram session cannot be decrypted with the configured keys.",
                       code="session_undecryptable", status_code=409) from exc


def record_flood(account: TelegramAccount, retry_after: int | None) -> None:
    until = cooldown_until(retry_after)
    if until is not None:
        account.flood_wait_until = until


@asynccontextmanager
async def account_guard(db: AsyncSession, account: TelegramAccount,
                        timeout: float | None = None) -> AsyncIterator[None]:
    """Persist account-level consequences of Telegram errors before they propagate to the HTTP layer.
    `timeout` is a hard overall deadline for the guarded block."""
    try:
        async with deadline(timeout):
            yield
    except TelegramSessionInvalid:
        account.status = AccountStatus.NEEDS_REAUTH
        await record_audit(db, action="telegram.session_invalid", user_id=account.user_id,
                           resource_type="telegram_account", resource_id=account.id)
        await db.commit()
        raise
    except FloodWait as fw:
        record_flood(account, fw.retry_after)
        await record_audit(db, action="telegram.flood_wait", user_id=account.user_id,
                           resource_type="telegram_account", resource_id=account.id,
                           details={"retry_after": fw.retry_after})
        await db.commit()
        raise


# ------------------------------------------------------------------ channels
async def get_owned_channel(db: AsyncSession, user_id: uuid.UUID, channel_id: uuid.UUID) -> TelegramChannel:
    channel = (await db.execute(
        select(TelegramChannel).join(TelegramAccount, TelegramAccount.id == TelegramChannel.account_id)
        .where(TelegramChannel.id == channel_id, TelegramAccount.user_id == user_id))).scalar_one_or_none()
    if channel is None:
        raise NotFoundError("Channel not found. List channels first.", code="channel_not_found")
    return channel


def apply_channel_info(row: TelegramChannel, info: ChannelInfo) -> None:
    row.access_hash = info.access_hash
    row.kind = info.kind
    row.title = info.title
    row.username = info.username
    if info.participants_count is not None:
        row.participants_count = info.participants_count
    row.is_creator = info.is_creator
    row.is_admin = info.is_admin
    row.can_invite_users = info.can_invite_users
    row.refreshed_at = utcnow()


async def refresh_channel_row(db: AsyncSession, client, row: TelegramChannel, settings: Settings) -> ChannelInfo:
    """Re-read a channel live; authorisation may have changed since discovery."""
    info = await call_with_flood_policy(lambda: fetch_channel(client, _ref(row)),
                                        max_inline_wait=settings.max_inline_flood_wait_seconds)
    apply_channel_info(row, info)
    return info


def _ref(row: TelegramChannel):
    from app.telegram.channels import ChannelRef
    return ChannelRef(row.kind, row.telegram_id, row.access_hash)


async def discover_channels(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory,
                            account: TelegramAccount, administered_only: bool, refresh: bool,
                            ip: str | None = None) -> list[TelegramChannel]:
    if not refresh:
        stmt = select(TelegramChannel).where(TelegramChannel.account_id == account.id)
        if administered_only:
            stmt = stmt.where(TelegramChannel.is_admin.is_(True))
        return list((await db.execute(stmt.order_by(TelegramChannel.title))).scalars().all())

    ensure_account_usable(account)
    session = decrypt_session(vault, account)
    async with account_guard(db, account, settings.telegram_operation_timeout_seconds):
        async with authorized_client(factory, session) as client:
            infos = await call_with_flood_policy(
                lambda: tg_list_channels(client, administered_only=administered_only, limit=settings.dialogs_limit),
                max_inline_wait=settings.max_inline_flood_wait_seconds)

    existing = {c.telegram_id: c for c in (await db.execute(
        select(TelegramChannel).where(TelegramChannel.account_id == account.id))).scalars().all()}
    rows: list[TelegramChannel] = []
    for info in infos:
        row = existing.get(info.telegram_id)
        if row is None:
            row = TelegramChannel(account_id=account.id, telegram_id=info.telegram_id, kind=info.kind,
                                  title=info.title)
            db.add(row)
        apply_channel_info(row, info)
        rows.append(row)
    account.last_used_at = utcnow()
    await record_audit(db, action="channels.discovered", user_id=account.user_id, resource_type="telegram_account",
                       resource_id=account.id, ip=ip, details={"count": len(rows)})
    await db.commit()
    return sorted(rows, key=lambda r: r.title.lower())


# ------------------------------------------------------------------ live member listing
async def list_channel_members(db: AsyncSession, *, settings: Settings, vault: SessionVault, factory,
                               user_id: uuid.UUID, channel_id: uuid.UUID, limit: int,
                               ip: str | None = None) -> tuple[TelegramChannel, MemberListing]:
    channel = await get_owned_channel(db, user_id, channel_id)
    account = await db.get(TelegramAccount, channel.account_id)
    ensure_account_usable(account)
    session = decrypt_session(vault, account)
    limit = max(1, min(limit, settings.member_fetch_max_limit))

    async with account_guard(db, account, settings.telegram_operation_timeout_seconds):
        async with authorized_client(factory, session) as client:
            info = await refresh_channel_row(db, client, channel, settings)
            if not info.is_admin:
                await db.commit()  # keep the refreshed (reduced) rights
                raise ForbiddenError("You are not an administrator of this channel/group anymore.",
                                     code="not_admin")
            listing = await fetch_members(client, info.ref, limit=limit)
        if listing.rate_limited_for is not None:
            record_flood(account, listing.rate_limited_for)

    account.last_used_at = utcnow()
    await record_audit(db, action="members.listed", user_id=user_id, resource_type="telegram_channel",
                       resource_id=channel.id, ip=ip,
                       details={"returned": len(listing.members), "complete": listing.complete,
                                "reason": listing.incomplete_reason})
    await db.commit()
    return channel, listing
