from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.errors import AppError
from app.core.security import DecryptionError
from app.telegram import errors as tge

log = logging.getLogger(__name__)

_STATUS: list[tuple[type[tge.TelegramError], int]] = [
    (tge.FloodWait, 429),
    (tge.InvalidLoginCode, 400),
    (tge.LoginCodeExpired, 400),
    (tge.InvalidTwoFactorPassword, 400),
    (tge.PhoneNumberInvalid, 400),
    (tge.PhoneNumberBanned, 403),
    (tge.TelegramSessionInvalid, 409),
    (tge.ChannelInaccessible, 403),
    (tge.AdminPermissionRequired, 403),
    (tge.TelegramConfigError, 500),
    (tge.TelegramNetworkError, 503),
    (tge.TelegramApiError, 502),
]


def _body(code: str, message: str, retry_after: int | None = None) -> dict:
    err: dict = {"code": code, "message": message}
    if retry_after is not None:
        err["retry_after"] = retry_after
    return {"error": err}


def _response(status: int, code: str, message: str, retry_after: int | None = None) -> JSONResponse:
    headers = {"Retry-After": str(retry_after)} if retry_after else None
    if status == 401:
        headers = {**(headers or {}), "WWW-Authenticate": "Bearer"}
    return JSONResponse(status_code=status, content=_body(code, message, retry_after), headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError):
        return _response(exc.status_code, exc.code, exc.message, exc.retry_after)

    @app.exception_handler(tge.TelegramError)
    async def _telegram_error(_: Request, exc: tge.TelegramError):
        status = next((s for cls, s in _STATUS if isinstance(exc, cls)), 502)
        return _response(status, exc.code, exc.message, exc.retry_after)

    @app.exception_handler(DecryptionError)
    async def _decrypt_error(_: Request, exc: DecryptionError):
        log.error("stored secret could not be decrypted")
        return _response(409, "session_undecryptable",
                         "A stored secret could not be decrypted with the configured keys.")

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        # Deliberately omit the submitted `input` so codes/passwords are never echoed back or logged.
        details = [{"loc": list(e.get("loc", [])), "message": str(e.get("msg", "invalid"))} for e in exc.errors()]
        return JSONResponse(status_code=422, content={"error": {"code": "validation_error",
                                                                  "message": "Request validation failed.",
                                                                  "details": details}})

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception):
        log.exception("unhandled error")  # traceback goes to logs (redacted); the client gets a generic body
        return _response(500, "internal_error", "Internal server error.")
