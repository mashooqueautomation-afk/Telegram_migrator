import logging
import uuid

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from app.core.logging import RedactingFilter
from app.core.ratelimit import SlidingWindowLimiter
from app.core.security import (DecryptionError, SessionVault, TokenError, create_access_token, decode_access_token,
                               hash_password, verify_password)
from app.services.audit import sanitize
from tests.conftest import make_settings

SESSION = "1BAAOMTQ5LjE1NC4xNjcuNTEBu" + "A" * 300  # shaped like a Telethon StringSession


def test_vault_roundtrip_and_ciphertext_hides_plaintext():
    vault = SessionVault(Fernet.generate_key().decode())
    token = vault.encrypt(SESSION)
    assert SESSION not in token and "BAAOMTQ5" not in token
    assert vault.decrypt(token) == SESSION
    assert vault.encrypt(SESSION) != token  # random IV: ciphertexts differ


def test_vault_detects_tampering_and_wrong_key():
    vault = SessionVault(Fernet.generate_key().decode())
    token = vault.encrypt("secret")
    tampered = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    with pytest.raises(DecryptionError):
        vault.decrypt(tampered)
    with pytest.raises(DecryptionError):
        SessionVault(Fernet.generate_key().decode()).decrypt(token)


def test_vault_key_rotation():
    old, new = Fernet.generate_key().decode(), Fernet.generate_key().decode()
    old_token = SessionVault(old).encrypt("secret")
    rotated_vault = SessionVault(f"{new},{old}")      # new key first = primary; old still decrypts
    assert rotated_vault.decrypt(old_token) == "secret"
    re_encrypted = rotated_vault.rotate(old_token)
    assert SessionVault(new).decrypt(re_encrypted) == "secret"   # now readable with the new key alone


def test_vault_rejects_invalid_key():
    with pytest.raises(ValueError):
        SessionVault("not-a-fernet-key")
    with pytest.raises(ValueError):
        SessionVault("  ,  ")


def test_password_hashing():
    h = hash_password("a-long-passphrase")
    assert h != "a-long-passphrase" and h.startswith("$argon2")
    assert verify_password("a-long-passphrase", h)
    assert not verify_password("wrong", h)
    assert not verify_password("anything", None)   # unknown user path


def test_jwt_roundtrip_expiry_and_tamper():
    uid = uuid.uuid4()
    secret = "s" * 40
    token, ttl = create_access_token(user_id=uid, secret=secret, algorithm="HS256", ttl_minutes=5)
    assert ttl == 300
    assert decode_access_token(token, secret=secret, algorithm="HS256") == uid
    with pytest.raises(TokenError):
        decode_access_token(token, secret="other" * 10, algorithm="HS256")
    expired, _ = create_access_token(user_id=uid, secret=secret, algorithm="HS256", ttl_minutes=-1)
    with pytest.raises(TokenError):
        decode_access_token(expired, secret=secret, algorithm="HS256")
    with pytest.raises(TokenError):
        decode_access_token("garbage", secret=secret, algorithm="HS256")


def test_redacting_filter_masks_phone_session_and_secrets(caplog):
    flt = RedactingFilter(secrets=["my-api-hash-0123456789"])
    rec = logging.LogRecord("x", logging.INFO, __file__, 1,
                            "login for +14155550123 hash=my-api-hash-0123456789 session=%s", (SESSION,), None)
    flt.filter(rec)
    out = rec.getMessage()
    assert "+14155550123" not in out and "my-api-hash" not in out and SESSION not in out
    assert "[REDACTED" in out


def test_audit_sanitize_drops_credentials():
    cleaned = sanitize({"telegram_user_id": 7, "phone": "+1", "login_code": "1", "nested": {"password": "x", "ok": 1},
                        "session_string": "s", "api_hash": "h", "count": 3})
    assert cleaned == {"telegram_user_id": 7, "nested": {"ok": 1}, "count": 3}


def test_sliding_window_limiter():
    lim = SlidingWindowLimiter(limit=2, window_seconds=60)
    assert lim.check("k", now=0) == 0
    assert lim.check("k", now=1) == 0
    assert lim.check("k", now=2) > 0            # third hit blocked
    assert lim.check("other", now=2) == 0       # separate key unaffected
    assert lim.check("k", now=61) == 0          # window slid


def test_settings_validation(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/x.db"
    with pytest.raises(ValidationError):
        make_settings(url, jwt_secret="short")
    with pytest.raises(ValidationError):
        make_settings(url, telegram_api_id=0)
    with pytest.raises(ValidationError):   # production refuses placeholders
        make_settings(url, app_env="production", jwt_secret="change-me-" + "x" * 40)
    assert make_settings(url, cors_origins="http://a.test, http://b.test").cors_origin_list == \
        ["http://a.test", "http://b.test"]
