from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness probe")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/health/ready", summary="Readiness probe (checks the database)")
async def ready(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"status": "database_unavailable"})
    return {"status": "ready"}
