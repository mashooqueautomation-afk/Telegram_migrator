"""arq task functions. Thin: all logic lives in the engine / services so it can be tested without Redis."""
from __future__ import annotations

import logging
import uuid

from app.services import analysis_service
from app.workers.recovery import recover

log = logging.getLogger(__name__)


async def run_migration(ctx: dict, job_id: str) -> str:
    result = await ctx["engine"].run(uuid.UUID(job_id))
    log.info("event=run_migration_finished migration_id=%s result=%s", job_id, result)
    return result


async def analyze_job(ctx: dict, job_id: str) -> None:
    async with ctx["sessionmaker"]() as db:
        await analysis_service.analyze_existing_job(db, settings=ctx["settings"], vault=ctx["vault"],
                                                    factory=ctx["factory"], job_id=uuid.UUID(job_id))


async def sweep(ctx: dict) -> dict:
    return await recover(sessionmaker=ctx["sessionmaker"], settings=ctx["settings"], queue=ctx["queue"])
