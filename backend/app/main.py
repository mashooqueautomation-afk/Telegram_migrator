from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

from app import __version__
from app.api.errors import register_exception_handlers
from app.api.routes import auth_app, health, migrations, telegram_auth, telegram_data
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.ratelimit import SlidingWindowLimiter
from app.core.security import SessionVault
from app.db.session import build_engine, build_sessionmaker
from app.telegram.client import TelegramClientFactory
from app.workers.queue import build_queue

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, *, client_factory=None, queue=None) -> FastAPI:
    """Application factory. Run with: uvicorn app.main:create_app --factory"""
    settings = settings or get_settings()
    configure_logging(secrets=[settings.telegram_api_hash.get_secret_value(),
                               settings.jwt_secret.get_secret_value(),
                               *settings.session_encryption_keys.get_secret_value().split(",")])
    vault = SessionVault(settings.session_encryption_keys.get_secret_value())  # fail fast on a bad key
    engine = build_engine(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await app.state.queue.close()
        await engine.dispose()

    app = FastAPI(
        title="Telegram Member Migration & Management API",
        version=__version__,
        description=(
            "Authenticate a Telegram account via MTProto, discover channels you administer, list the members "
            "Telegram legitimately exposes, analyse eligibility, then run a controlled, resumable migration in a background "
            "worker (invitations only where Telegram permits; every refusal is recorded, never bypassed)."
        ),
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )
    app.state.settings = settings
    app.state.vault = vault
    app.state.engine = engine
    app.state.sessionmaker = build_sessionmaker(engine)
    app.state.queue = queue if queue is not None else build_queue(settings.redis_url)
    app.state.auth_limiter = SlidingWindowLimiter(settings.auth_rate_limit_per_minute, 60)
    app.state.client_factory = client_factory or TelegramClientFactory(
        api_id=settings.telegram_api_id, api_hash=settings.telegram_api_hash.get_secret_value(),
        device_name=settings.telegram_device_name, timeout=settings.telegram_timeout_seconds,
        connection_retries=settings.telegram_connection_retries)

    if settings.cors_origin_list:
        app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_credentials=False,
                           allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
                           allow_headers=["Authorization", "Content-Type"])

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        if request.url.path.startswith("/api"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    register_exception_handlers(app)
    app.include_router(health.router, prefix="/api")
    app.include_router(auth_app.router, prefix="/api")
    app.include_router(telegram_auth.router, prefix="/api")
    app.include_router(telegram_data.router, prefix="/api")
    app.include_router(migrations.router, prefix="/api")
    return app
