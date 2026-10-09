from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PLACEHOLDERS = ("change-me", "changeme", "replace-me", "your-", "xxxx")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: Literal["development", "production", "test"] = "development"
    database_url: str

    # Telegram application credentials from https://my.telegram.org (server-side only).
    telegram_api_id: int
    telegram_api_hash: SecretStr
    telegram_device_name: str = "TG Migrator"
    telegram_timeout_seconds: int = 20
    telegram_connection_retries: int = 2
    # Hard overall deadlines (seconds). A half-open connection must never hang an HTTP request.
    telegram_login_timeout_seconds: float = 30
    telegram_operation_timeout_seconds: float = 120

    # Comma-separated Fernet keys. The FIRST key encrypts; all keys can decrypt (rotation).
    session_encryption_keys: SecretStr

    jwt_secret: SecretStr
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 60
    allow_registration: bool = True
    cors_origins: str = ""
    docs_enabled: bool = True

    # Telegram login flow
    login_attempt_ttl_seconds: int = 600
    max_code_attempts: int = 5
    max_password_attempts: int = 5

    # Discovery / analysis bounds (Stage 1 runs analysis inside the request; Stage 2 moves it to a worker)
    dialogs_limit: int = 500
    member_fetch_default_limit: int = 200
    member_fetch_max_limit: int = 5000
    eligibility_default_limit: int = 500
    eligibility_max_limit: int = 5000
    destination_listing_limit: int = 5000

    # FloodWait policy: we never sleep longer than this inline; anything longer is surfaced to the caller.
    max_inline_flood_wait_seconds: int = 5

    # In-process throttle for our own auth endpoints
    auth_rate_limit_per_minute: int = 10

    # ---------------------------------------------------------------- Stage 2: migration engine
    # Redis is the job queue (arq). Without it the API still works but cannot start/analyse jobs.
    redis_url: str | None = None
    # Application-level throttling. These NEVER override Telegram: its limits and FloodWaits stay authoritative.
    migration_concurrency: int = Field(1, ge=1, le=5)              # members processed at once (keep at 1 unless sure)
    migration_min_delay_seconds: float = Field(5.0, ge=0)          # min gap between the starts of Telegram operations
    migration_max_retries: int = Field(4, ge=0, le=10)             # retries for TRANSIENT errors only
    migration_retry_base_seconds: float = Field(1.0, ge=0)         # backoff: 1, 2, 4, 8 ... capped below
    migration_retry_max_seconds: float = Field(30.0, ge=0)
    migration_max_consecutive_failures: int = Field(5, ge=1)       # circuit breaker -> pause the job
    migration_lease_seconds: int = Field(120, ge=10)               # a job whose heartbeat is older is considered orphaned
    migration_job_timeout_seconds: int = 86400                     # arq hard limit for one run; recovery continues after
    default_flood_wait_seconds: int = 300                          # used only if Telegram gives no duration
    flood_wait_margin_seconds: int = 1                             # extra second on top of what Telegram asked
    worker_max_jobs: int = Field(2, ge=1, le=10)                   # migrations a single worker process runs at once
    sse_poll_interval_seconds: float = Field(1.0, gt=0)
    sse_heartbeat_seconds: float = Field(15.0, gt=0)

    @field_validator("telegram_api_id")
    @classmethod
    def _api_id_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("TELEGRAM_API_ID must be a positive integer")
        return v

    @field_validator("jwt_secret")
    @classmethod
    def _jwt_strong(cls, v: SecretStr) -> SecretStr:
        if len(v.get_secret_value()) < 32:
            raise ValueError("JWT_SECRET must be at least 32 characters")
        return v

    @model_validator(mode="after")
    def _no_placeholders_in_production(self) -> "Settings":
        if self.app_env == "production":
            for name in ("jwt_secret", "session_encryption_keys", "telegram_api_hash"):
                value = getattr(self, name).get_secret_value().lower()
                if any(p in value for p in _PLACEHOLDERS):
                    raise ValueError(f"{name.upper()} still contains a placeholder value")
        return self

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
