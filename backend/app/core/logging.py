"""Logging setup with redaction of phone numbers, session strings and configured secrets."""
from __future__ import annotations

import logging
import re
from collections.abc import Iterable

_PHONE = re.compile(r"\+\d{7,15}")
# Telethon StringSession strings are '1' + ~350 url-safe base64 characters.
_SESSION = re.compile(r"\b1[A-Za-z0-9_\-=]{200,}")


class RedactingFilter(logging.Filter):
    def __init__(self, secrets: Iterable[str] = ()) -> None:
        super().__init__()
        self._secrets = [s for s in secrets if s and len(s) >= 6]

    def redact(self, text: str) -> str:
        for secret in self._secrets:
            text = text.replace(secret, "[REDACTED]")
        text = _SESSION.sub("[REDACTED-SESSION]", text)
        return _PHONE.sub("[REDACTED-PHONE]", text)

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redact(record.getMessage())
        record.args = ()
        return True


def configure_logging(secrets: Iterable[str] = (), level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    flt = RedactingFilter(secrets)
    for handler in root.handlers:
        if not any(isinstance(f, RedactingFilter) for f in handler.filters):
            handler.addFilter(flt)
    # Telethon can be chatty about connection details.
    logging.getLogger("telethon").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------------- structured logging (Stage 2)
_SENSITIVE_KEYS = ("phone", "code_hash", "password", "session", "secret", "token", "api_hash", "otp", "hash")


def redact_mapping(fields: dict) -> dict:
    return {k: ("[REDACTED]" if any(s in k.lower() for s in _SENSITIVE_KEYS) else v) for k, v in fields.items()}


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields) -> None:
    """One structured line: `event=member_success migration_id=... member_id=... status=... error_code=...`.

    Credentials can never be logged: sensitive-looking keys are masked and the RedactingFilter scrubs values.
    """
    safe = redact_mapping({k: v for k, v in fields.items() if v is not None})
    logger.log(level, "event=%s %s", event, " ".join(f"{k}={v}" for k, v in safe.items()))
