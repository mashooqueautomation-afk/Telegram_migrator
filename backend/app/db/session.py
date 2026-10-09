from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def build_engine(database_url: str) -> AsyncEngine:
    kwargs: dict = {"pool_pre_ping": True}
    engine = create_async_engine(database_url, **kwargs)
    if database_url.startswith("sqlite"):
        @event.listens_for(engine.sync_engine, "connect")
        def _sqlite_fk_on(dbapi_conn, _record):  # enforce FKs / ON DELETE CASCADE like PostgreSQL
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()
    return engine


def build_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
