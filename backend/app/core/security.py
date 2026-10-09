"""Password hashing, JWTs and authenticated encryption for Telegram sessions."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from cryptography.fernet import Fernet, InvalidToken, MultiFernet

_hasher = PasswordHasher()
# Verified against when the e-mail is unknown so both failure paths cost the same.
_DUMMY_HASH = _hasher.hash("not-a-real-password")


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str | None) -> bool:
    try:
        ok = _hasher.verify(password_hash or _DUMMY_HASH, password)
    except (VerificationError, InvalidHashError):
        return False
    return ok and password_hash is not None


def password_needs_rehash(password_hash: str) -> bool:
    return _hasher.check_needs_rehash(password_hash)


# ---------------------------------------------------------------- JWT
class TokenError(Exception):
    pass


def create_access_token(*, user_id: uuid.UUID, secret: str, algorithm: str, ttl_minutes: int) -> tuple[str, int]:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=ttl_minutes)
    payload = {"sub": str(user_id), "iat": int(now.timestamp()), "exp": int(exp.timestamp()), "typ": "access"}
    return jwt.encode(payload, secret, algorithm=algorithm), ttl_minutes * 60


def decode_access_token(token: str, *, secret: str, algorithm: str) -> uuid.UUID:
    try:
        payload = jwt.decode(token, secret, algorithms=[algorithm], options={"require": ["exp", "sub"]})
        if payload.get("typ") != "access":
            raise TokenError("wrong token type")
        return uuid.UUID(payload["sub"])
    except (jwt.PyJWTError, ValueError) as exc:
        raise TokenError("invalid token") from exc


# ---------------------------------------------------------------- Encryption
class DecryptionError(Exception):
    pass


class SessionVault:
    """Fernet (AES-128-CBC + HMAC-SHA256) with key rotation via MultiFernet."""

    def __init__(self, keys_csv: str) -> None:
        keys = [k.strip() for k in keys_csv.split(",") if k.strip()]
        if not keys:
            raise ValueError("SESSION_ENCRYPTION_KEYS must contain at least one Fernet key")
        try:
            self._fernet = MultiFernet([Fernet(k.encode()) for k in keys])
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "SESSION_ENCRYPTION_KEYS contains an invalid Fernet key "
                "(generate one with scripts/generate_secrets.py)"
            ) from exc

    def encrypt(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode()).decode()

    def decrypt(self, token: str) -> str:
        try:
            return self._fernet.decrypt(token.encode()).decode()
        except (InvalidToken, ValueError) as exc:
            raise DecryptionError("stored value could not be decrypted (wrong or rotated-out key?)") from exc

    def rotate(self, token: str) -> str:
        """Re-encrypt with the primary key."""
        try:
            return self._fernet.rotate(token.encode()).decode()
        except InvalidToken as exc:
            raise DecryptionError("stored value could not be decrypted") from exc
