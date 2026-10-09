"""Give an existing app user a (fake) connected Telegram account + two administered channels, directly in the DB.
Usage:  python -m tests.live_seed you@example.com"""
import asyncio
import sys

from sqlalchemy import select

from app.core.config import get_settings
from app.core.security import SessionVault
from app.db.session import build_engine, build_sessionmaker
from app.domain import ChannelKind
from app.models import TelegramAccount, TelegramChannel, User


async def main(email: str) -> None:
    settings = get_settings()
    engine = build_engine(settings.database_url)
    vault = SessionVault(settings.session_encryption_keys.get_secret_value())
    async with build_sessionmaker(engine)() as db:
        user = (await db.execute(select(User).where(User.email == email))).scalar_one()
        acct = TelegramAccount(user_id=user.id, telegram_user_id=777, username="ada_admin", first_name="Ada",
                               session_encrypted=vault.encrypt("live-session-1"))
        db.add(acct)
        await db.flush()
        for tid, kind, title, creator in ((100, ChannelKind.CHANNEL, "Source Channel", True),
                                          (200, ChannelKind.SUPERGROUP, "Destination Group", False)):
            db.add(TelegramChannel(account_id=acct.id, telegram_id=tid, access_hash=1000 + tid, kind=kind, title=title,
                                   is_creator=creator, is_admin=True, can_invite_users=True))
        await db.commit()
    await engine.dispose()
    print("seeded", email)

asyncio.run(main(sys.argv[1]))
