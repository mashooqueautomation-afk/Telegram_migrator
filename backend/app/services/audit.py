from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AuditLog

_SENSITIVE = ("phone", "code", "password", "passwd", "secret", "session", "token", "hash", "api_key", "otp")


def sanitize(details: Any) -> Any:
    """Drop anything that looks like a credential before it can reach the audit table."""
    if isinstance(details, dict):
        return {k: sanitize(v) for k, v in details.items() if not any(s in k.lower() for s in _SENSITIVE)}
    if isinstance(details, (list, tuple)):
        return [sanitize(v) for v in details]
    return details


async def record_audit(db: AsyncSession, *, action: str, user_id: uuid.UUID | None = None,
                       resource_type: str | None = None, resource_id: object | None = None,
                       ip: str | None = None, details: dict | None = None) -> None:
    db.add(AuditLog(user_id=user_id, action=action, resource_type=resource_type,
                    resource_id=str(resource_id) if resource_id is not None else None,
                    ip_address=ip, details=sanitize(details) if details else None))
    await db.flush()  # caller owns the commit
