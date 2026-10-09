"""arq worker entry point.   Run:  arq app.workers.worker.WorkerSettings

Architecture:   FastAPI  -->  Redis (arq queue)  -->  this worker  -->  Telegram / PostgreSQL
The API only creates/controls jobs; this process does all Telegram work.
"""
from __future__ import annotations

import logging

from arq import cron
from arq.connections import RedisSettings

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.security import SessionVault
from app.db.session import build_engine, build_sessionmaker
from app.telegram.client import TelegramClientFactory
from app.workers.engine import MigrationEngine
from app.workers.queue import ArqQueue
from app.workers.tasks import analyze_job, run_migration, sweep

log = logging.getLogger(__name__)


def make_startup(settings: Settings, *, factory=None):
    """Returns arq's on_startup hook. `factory` lets tests/integration harnesses inject a Telegram fake."""

    async def on_startup(ctx: dict) -> None:
        configure_logging(secrets=[settings.telegram_api_hash.get_secret_value(),
                                   settings.jwt_secret.get_secret_value(),
                                   *settings.session_encryption_keys.get_secret_value().split(",")])
        db_engine = build_engine(settings.database_url)
        sessionmaker = build_sessionmaker(db_engine)
        vault = SessionVault(settings.session_encryption_keys.get_secret_value())
        tg_factory = factory or TelegramClientFactory(
            api_id=settings.telegram_api_id, api_hash=settings.telegram_api_hash.get_secret_value(),
            device_name=settings.telegram_device_name, timeout=settings.telegram_timeout_seconds,
            connection_retries=settings.telegram_connection_retries)
        queue = ArqQueue(settings.redis_url)
        ctx.update(settings=settings, db_engine=db_engine, sessionmaker=sessionmaker, vault=vault, factory=tg_factory,
                   queue=queue, engine=MigrationEngine(sessionmaker=sessionmaker, settings=settings, vault=vault,
                                                       factory=tg_factory, queue=queue))
        log.info("event=worker_started worker_id=%s concurrency=%s min_delay=%s", ctx["engine"].worker_id,
                 settings.migration_concurrency, settings.migration_min_delay_seconds)

    return on_startup


async def on_shutdown(ctx: dict) -> None:
    await ctx["queue"].close()
    await ctx["db_engine"].dispose()


def worker_config(settings: Settings, *, factory=None) -> dict:
    """Keyword arguments for arq's Worker / the WorkerSettings class."""
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL must be set to run the worker")
    return dict(
        functions=[run_migration, analyze_job],
        cron_jobs=[cron(sweep, second={0, 30}, run_at_startup=True, unique=True, timeout=120)],
        on_startup=make_startup(settings, factory=factory),
        on_shutdown=on_shutdown,
        redis_settings=RedisSettings.from_dsn(settings.redis_url),
        max_jobs=settings.worker_max_jobs,
        job_timeout=settings.migration_job_timeout_seconds,
        max_tries=1,                 # our engine is resumable; crash recovery is the sweeper's job, not arq retries
        keep_result=0,               # results are not needed and would block re-enqueueing the same job id
        health_check_interval=30,
        job_completion_wait=30,      # on SIGTERM give the in-flight member operation time to finish
    )


def __getattr__(name: str):
    """`arq app.workers.worker.WorkerSettings` resolves this lazily from the environment, so importing the module
    (tests, tooling) never needs a full configuration, while a misconfigured worker fails loudly with the real reason."""
    if name == "WorkerSettings":
        return type("WorkerSettings", (), worker_config(get_settings()))
    raise AttributeError(name)
