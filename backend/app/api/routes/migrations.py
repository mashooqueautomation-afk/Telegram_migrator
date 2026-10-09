import uuid

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (client_ip, get_client_factory, get_current_user, get_current_user_light, get_db, get_queue,
                          get_settings_dep, get_vault)
from app.core.config import Settings
from app.core.security import SessionVault
from app.domain import Eligibility, InviteOutcome, JobStatus, MemberState
from app.models import MigrationEvent, MigrationJob, MigrationMember, User
from app.schemas.common import Page
from app.schemas.migration import (EligibilityIn, EventOut, JobListItem, JobMemberOut, JobOut, MigrationCreateIn,
                                   SelectionIn, SelectionOut, StartIn)
from app.services import analysis_service as analysis
from app.services import migration_service as svc
from app.services.job_view import build_job_out, to_list_item
from app.services.stream import event_stream

router = APIRouter(prefix="/migrations", tags=["migrations"])

SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}


async def _out(db: AsyncSession, job: MigrationJob, settings: Settings) -> JobOut:
    return await build_job_out(db, job, settings)


# ============================================================================================ create / analyse
@router.post("", response_model=JobOut, status_code=201,
             summary="Create a migration (PENDING). Next: POST /{id}/eligibility, then /start")
async def create_migration(body: MigrationCreateIn, request: Request, user: User = Depends(get_current_user),
                           db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep)):
    job = await svc.create_job(db, settings=settings, user_id=user.id, account_id=body.account_id,
                               source_channel_id=body.source_channel_id,
                               destination_channel_id=body.destination_channel_id, member_limit=body.member_limit,
                               ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/eligibility", response_model=JobOut, status_code=201,
             summary="Stage 1 one-shot: create AND analyse inside the request (small jobs; sends nothing)")
async def create_eligibility(body: EligibilityIn, request: Request, user: User = Depends(get_current_user),
                             db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                             vault: SessionVault = Depends(get_vault), factory=Depends(get_client_factory)):
    job = await analysis.create_eligibility_job(
        db, settings=settings, vault=vault, factory=factory, user_id=user.id, account_id=body.account_id,
        source_channel_id=body.source_channel_id, destination_channel_id=body.destination_channel_id,
        member_limit=body.member_limit, ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/eligibility", response_model=JobOut, status_code=202,
             summary="Queue the eligibility analysis for a PENDING migration (runs in the worker)")
async def request_eligibility(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                              db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                              queue=Depends(get_queue)):
    job = await svc.request_analysis(db, queue=queue, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/selection", response_model=SelectionOut,
             summary="Choose which ELIGIBLE members to migrate (before start). Default: all eligible are selected")
async def set_selection(migration_id: uuid.UUID, body: SelectionIn, request: Request,
                        user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    updated, ignored, total = await svc.set_selection(db, user_id=user.id, job_id=migration_id,
                                                      telegram_user_ids=body.telegram_user_ids,
                                                      selected=body.selected, ip=client_ip(request))
    return SelectionOut(updated=updated, ignored=ignored, selected_total=total)


# ============================================================================================ control
@router.post("/{migration_id}/start", response_model=JobOut, summary="Start: PENDING -> RUNNING (queues the worker)")
async def start_migration(migration_id: uuid.UUID, request: Request, body: StartIn | None = None,
                          user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db),
                          settings: Settings = Depends(get_settings_dep), queue=Depends(get_queue)):
    job = await svc.start_job(db, queue=queue, user_id=user.id, job_id=migration_id,
                              acknowledge_partial=bool(body and body.acknowledge_partial), ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/pause", response_model=JobOut,
             summary="Cooperative pause: takes effect after the current member finishes")
async def pause_migration(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep)):
    job = await svc.pause_job(db, settings=settings, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/resume", response_model=JobOut,
             summary="Resume a paused migration (refused with 429 + Retry-After while Telegram's cooldown runs)")
async def resume_migration(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                           db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep),
                           queue=Depends(get_queue)):
    job = await svc.resume_job(db, queue=queue, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/cancel", response_model=JobOut,
             summary="Cancel: stop new members, let the current one finish, keep all results")
async def cancel_migration(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                           db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep)):
    job = await svc.cancel_job(db, settings=settings, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return await _out(db, job, settings)


@router.post("/{migration_id}/retry", response_model=JobOut, status_code=201,
             summary="Create a NEW linked job for the members that did not reach a final result")
async def retry_migration(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings_dep)):
    child = await svc.create_retry_job(db, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return await _out(db, child, settings)


# ============================================================================================ reads
@router.get("", response_model=Page[JobListItem], summary="Your migrations, newest first")
async def list_jobs(status: JobStatus | None = None, limit: int = Query(20, ge=1, le=100),
                    offset: int = Query(0, ge=0), user: User = Depends(get_current_user),
                    db: AsyncSession = Depends(get_db)):
    cond = [MigrationJob.user_id == user.id] + ([MigrationJob.status == status] if status else [])
    total = (await db.execute(select(func.count()).select_from(MigrationJob).where(*cond))).scalar_one()
    rows = (await db.execute(select(MigrationJob).where(*cond).order_by(MigrationJob.created_at.desc())
                             .limit(limit).offset(offset))).scalars().all()
    return Page[JobListItem](items=[to_list_item(j) for j in rows], total=total, limit=limit, offset=offset)


@router.get("/{migration_id}", response_model=JobOut, summary="Full status for the dashboard")
async def get_job(migration_id: uuid.UUID, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db),
                  settings: Settings = Depends(get_settings_dep)):
    return await _out(db, await analysis.get_job(db, user.id, migration_id), settings)


@router.get("/{migration_id}/members", response_model=Page[JobMemberOut],
            summary="Per-member results (paginated, filterable)")
async def list_job_members(migration_id: uuid.UUID, eligibility: Eligibility | None = None,
                           processing_status: MemberState | None = None, outcome: InviteOutcome | None = None,
                           selected: bool | None = None, search: str | None = Query(None, max_length=64),
                           limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
                           user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    job = await analysis.get_job(db, user.id, migration_id)
    conds = [MigrationMember.job_id == job.id]
    if eligibility is not None:
        conds.append(MigrationMember.eligibility == eligibility)
    if processing_status is not None:
        conds.append(MigrationMember.state == processing_status)
    if outcome is not None:
        conds.append(MigrationMember.outcome == outcome)
    if selected is not None:
        conds.append(MigrationMember.selected.is_(selected))
    if search:
        like = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        conds.append(or_(MigrationMember.display_name.ilike(like, escape="\\"),
                         MigrationMember.username.ilike(like, escape="\\")))
    total = (await db.execute(select(func.count()).select_from(MigrationMember).where(*conds))).scalar_one()
    rows = (await db.execute(select(MigrationMember).where(*conds)
                             .order_by(MigrationMember.position, MigrationMember.display_name,
                                       MigrationMember.telegram_user_id).limit(limit).offset(offset))).scalars().all()
    return Page[JobMemberOut](items=[JobMemberOut.model_validate(r) for r in rows], total=total, limit=limit,
                              offset=offset)


# ============================================================================================ events / SSE
def _last_event_id(request: Request, after_seq: int) -> int:
    raw = request.headers.get("last-event-id", "")
    return int(raw) if raw.isdigit() else after_seq


def _sse_response(request: Request, settings: Settings, job_id: uuid.UUID, after_seq: int) -> StreamingResponse:
    gen = event_stream(sessionmaker=request.app.state.sessionmaker, settings=settings, job_id=job_id,
                       after_seq=_last_event_id(request, after_seq), is_disconnected=request.is_disconnected)
    return StreamingResponse(gen, media_type="text/event-stream", headers=SSE_HEADERS)


@router.get("/{migration_id}/events", response_model=list[EventOut],
            responses={200: {"content": {"text/event-stream": {}}, "description":
                             "JSON list of persisted events, or a live SSE stream when `Accept: text/event-stream`"}},
            summary="Event log. JSON by default; Server-Sent Events with Accept: text/event-stream")
async def migration_events(migration_id: uuid.UUID, request: Request, after_seq: int = Query(0, ge=0),
                           limit: int = Query(100, ge=1, le=500), user: User = Depends(get_current_user_light),
                           settings: Settings = Depends(get_settings_dep)):
    sm = request.app.state.sessionmaker
    async with sm() as db:
        job = await analysis.get_job(db, user.id, migration_id)    # ownership is verified server-side
    if "text/event-stream" in request.headers.get("accept", ""):
        return _sse_response(request, settings, job.id, after_seq)
    async with sm() as db:
        rows = (await db.execute(select(MigrationEvent).where(MigrationEvent.job_id == job.id,
                                                              MigrationEvent.seq > after_seq)
                                 .order_by(MigrationEvent.seq).limit(limit))).scalars().all()
    return [EventOut.model_validate(r) for r in rows]


@router.get("/{migration_id}/events/stream", response_class=StreamingResponse,
            summary="Always Server-Sent Events (same as /events with the SSE Accept header)")
async def migration_event_stream(migration_id: uuid.UUID, request: Request, after_seq: int = Query(0, ge=0),
                                 user: User = Depends(get_current_user_light),
                                 settings: Settings = Depends(get_settings_dep)):
    async with request.app.state.sessionmaker() as db:
        job = await analysis.get_job(db, user.id, migration_id)
    return _sse_response(request, settings, job.id, after_seq)


# ============================================================================================ delete
@router.delete("/{migration_id}", status_code=204,
               summary="Delete a (non-running) job and every member/event row it holds (privacy: right to erase)")
async def delete_job(migration_id: uuid.UUID, request: Request, user: User = Depends(get_current_user),
                     db: AsyncSession = Depends(get_db)):
    await svc.delete_job(db, user_id=user.id, job_id=migration_id, ip=client_ip(request))
    return Response(status_code=204)
