# Telegram Member Migration & Management — Stage 1 + Stage 2

```
sign in → connect a Telegram account (official MTProto login) → discover channels you administer
  → create a migration → eligibility analysis (worker) → choose members
  → START: a background worker invites them one at a time, only where Telegram permits it
  → live progress (SSE) · pause / resume / cancel · FloodWait pause with countdown · retry · full audit trail
```

**Stage 1** (backend foundation), **Stage 2** (migration engine), and a **Stage 3 frontend implementation** are included in this project.
The UI is integrated against the existing API contracts; it still requires a local dependency install and complete runtime verification before production use.

## What Telegram actually allows (and what this project never does)

- There is **no API that moves members between channels.** The only legitimate mechanism is to **invite individual users the
  account can reference** (`channels.InviteToChannel`; legacy groups: `messages.AddChatUser`), and Telegram refuses many of them.
- **Privacy settings are invisible in advance.** `PRIVACY_RESTRICTED` is recorded only after Telegram actually refuses an invite.
  `ELIGIBLE` means "no blocking condition found" — **never** "the invite will work".
- **An invite call that raises nothing is not success.** After inviting, the worker asks Telegram whether the user is now a
  participant; only a confirmed membership becomes `SUCCESS`. Otherwise the member is `FAILED / NOT_CONFIRMED`.
- **Telegram stays in charge.** FloodWait pauses the whole job until the cooldown has elapsed; refusals are recorded and skipped;
  if Telegram restricts *our account* (`PEER_FLOOD`, spam ban) or revokes our rights, the job **stops and reports**.
  `MIGRATION_MIN_DELAY_SECONDS`, `MIGRATION_CONCURRENCY` and `MIGRATION_MAX_RETRIES` are application-level pacing only; they
  do not and cannot override Telegram's limits.
- **Never implemented, by design:** account rotation, proxy/IP rotation, fake user agents, privacy bypass, scraping private data,
  unsolicited messaging, ban evasion, retrying refusals, shortening a FloodWait.

## Architecture

```
 HTTP client ──▶ FastAPI ──enqueue──▶ Redis (arq) ──▶ Worker (MigrationEngine) ──▶ Telegram (Telethon, MTProto)
      ▲             │  ▲                                    │
      │ SSE         ▼  └───────── PostgreSQL ◀──────────────┘   (jobs, members, events, audit; the single source of truth)
      └──────── tails migration_events
```

- **The API never processes members and never blocks on Telegram** for migrations. It validates, moves state through the
  validated state machine, writes events + audit rows, and queues a task.
- **The worker** (`arq app.workers.worker.WorkerSettings`) runs `run_migration` (the engine) and `analyze_job`. A cron sweeper
  (`recover`) runs at start-up and every 30 s.
- **PostgreSQL is authoritative.** Redis only carries "please run job X" messages; losing Redis loses no data (the sweeper
  re-queues work from the database).
- Code layout: `app/telegram/` (the only code touching Telethon: `client auth channels members invite rate_limit errors`),
  `app/services/` (business rules), `app/workers/` (`engine queue recovery tasks worker`), `app/api/` (thin HTTP layer).

### Job state machine (`app/services/job_state.py`)

```
PENDING ──start──▶ RUNNING ⇄ PAUSED                 COMPLETED / CANCELLED / FAILED are terminal.
   │                 │ │ ╲                           Invalid transitions are rejected (HTTP 409 invalid_transition),
   │                 │ │  ╲──▶ FAILED                e.g. COMPLETED → RUNNING, PENDING → PAUSED.
   └──▶ CANCELLED ◀──┘ └──▶ COMPLETED                Transitions are atomic compare-and-set updates, so two writers
                  ▲   CANCELLING                     can never both win.
                  └─────────┘
```

`PAUSED` carries a `pause_reason`: `USER` (never auto-resumes) · `RATE_LIMIT` (Telegram cooldown; **auto-resumes**) ·
`ERRORS` (circuit breaker: N members in a row failed transiently).

### Member processing (idempotent, resumable)

1. **Claim** — `UPDATE … SET state='PROCESSING' WHERE id=? AND state IN ('ELIGIBLE','RATE_LIMITED')`; only one worker can win.
   Finished members (`SUCCESS`, `ALREADY_MEMBER`, `SKIPPED`, `FAILED`) are never claimable again. `UNIQUE(job_id, telegram_user_id)`.
2. **Check live** whether the user is already in the destination → `ALREADY_MEMBER` (nothing is sent).
3. **Stamp `invite_attempted_at` in the database *before* asking Telegram to invite** (crash safety).
4. **Invite**, then **verify membership**. Result + counters + event are written in **one transaction**.
5. If a worker dies mid-member, recovery returns that member to `ELIGIBLE`; if `invite_attempted_at` is set the engine *verifies*
   instead of blindly re-inviting, so a landed invite is recorded as `SUCCESS` and never sent twice.

| Telegram answer | Member becomes | Retried? |
|---|---|---|
| invite + membership confirmed | `SUCCESS` | – |
| already a participant | `ALREADY_MEMBER` | – |
| `USER_PRIVACY_RESTRICTED`, `USER_NOT_MUTUAL_CONTACT`, `missing_invitees` | `SKIPPED` (eligibility → `PRIVACY_RESTRICTED`) | **never** |
| `USER_KICKED`, `USER_CHANNELS_TOO_MUCH` | `SKIPPED` (eligibility → `RESTRICTED`) | never |
| invalid/deleted user, bot | `SKIPPED` | never |
| network / server hiccup | exponential backoff 1 s, 2 s, 4 s, 8 s (capped), then `FAILED / TEMPORARY_ERROR` | bounded by `MIGRATION_MAX_RETRIES` |
| accepted but not listed as a member | `FAILED / NOT_CONFIRMED` | no |
| unrecognised Telegram error | `FAILED / UNKNOWN_ERROR` | no (manual retry only) |
| **FloodWait** | member `RATE_LIMITED`, **job pauses** until cooldown | after the cooldown |
| `CHAT_ADMIN_REQUIRED`, `CHANNEL_PRIVATE`, `USERS_TOO_MUCH` | job **FAILED** (member untouched) | via a retry job |
| `PEER_FLOOD`, `USER_BANNED_IN_CHANNEL` (our account restricted), dead session | job **FAILED**, reported | via a retry job |

### FloodWait

Telegram's duration is read, **+1 s margin**, and persisted (`migration_jobs.rate_limited_until`, `telegram_accounts.flood_wait_until`).
The job becomes `PAUSED/RATE_LIMIT` (events `migration.rate_limited` + `migration.paused`), the worker **returns** (it does not sleep),
and a **deferred arq task** is scheduled for the exact end of the cooldown; the sweeper is the safety net. While it runs:
`POST /resume` → **429 `cooldown_active` + `Retry-After`**, no Telegram calls are made, other operations on the same account
(channel discovery, new analyses) refuse too. The API exposes `rate_limited_until` and `rate_limit_remaining_seconds` for the countdown.

### Pause / cancel (cooperative)

`pause` sets `pause_requested`; `cancel` moves `RUNNING → CANCELLING` and sets `cancellation_requested`. The worker checks the
flags **between members**; the in-flight member always finishes and is recorded, then the job becomes `PAUSED` / `CANCELLED`.
Cancelling never deletes anything. If no live worker exists, the API finalises immediately.

### Retry = a new linked job

`POST /{id}/retry` on a finished job creates a **new** `PENDING` job (`retry_of_id`) holding only members that did not reach a
final result (failed, rate-limited, never processed). Permanent refusals and successes are excluded. History stays immutable and
`COMPLETED → RUNNING` stays impossible. Membership is re-checked live before every invite.

## Setup

Prerequisites: a Telegram **api_id/api_hash** (<https://my.telegram.org> → *API development tools*), and Docker **or** Python 3.12 +
PostgreSQL 14+ + Redis 6+.

### A — Docker Compose (db + redis + backend + worker)

```bash
cp .env.example .env
python3 backend/scripts/generate_secrets.py          # POSTGRES_PASSWORD, SESSION_ENCRYPTION_KEYS, JWT_SECRET
$EDITOR .env                                         # paste them, set TELEGRAM_API_ID / TELEGRAM_API_HASH,
                                                     # make the password in DATABASE_URL match POSTGRES_PASSWORD
docker compose up --build
```

Start order: `db` + `redis` healthy → `backend` (applies migrations, must become healthy) → `worker`. The API is on
<http://localhost:8000> (docs at `/docs`), bound to localhost; db and redis are not published. `docker compose stop` gives the worker
45 s to finish the in-flight member.

### B — local Python

```bash
sudo -u postgres psql -c "CREATE USER tgmigrate WITH PASSWORD 'choose-a-password';"
sudo -u postgres psql -c "CREATE DATABASE tgmigrate OWNER tgmigrate;"
redis-server --daemonize yes                         # or any Redis 6+

python3 -m venv backend/.venv && source backend/.venv/bin/activate
pip install -r backend/requirements-dev.txt
cp .env.example .env && python backend/scripts/generate_secrets.py   # fill .env (DATABASE_URL host = localhost)
set -a; . ./.env; set +a

cd backend
alembic upgrade head
uvicorn app.main:create_app --factory --reload       # terminal 1 — the API
arq app.workers.worker.WorkerSettings                # terminal 2 — the worker (does ALL Telegram work)
```

### Try the engine WITHOUT a real Telegram account

`backend/tests/live_harness.py` runs the **real worker** against a fake Telegram (no risk to a real account):

```bash
cd backend
# after registering an app user through the API (see below), give it a fake connected account + channels:
python -m tests.live_seed you@example.com
FAKE_MEMBERS=24 FAKE_FLOOD_AT=8 FAKE_FLOOD_SECONDS=5 FAKE_PRIVACY_EVERY=7 FAKE_STATE=/tmp/fake.log \
  arq tests.live_harness.WorkerSettings              # use this instead of the normal worker
```

## Using it

```bash
API=http://localhost:8000/api
# Telegram login (see Stage 1 flow): POST /auth/register, /auth/login, then /auth/telegram/start -> /verify -> (/2fa)
curl -s $API/telegram/channels -H "$AUTH"                                     # note the source + destination ids

curl -s $API/migrations -H "$AUTH" -H 'content-type: application/json' \
  -d '{"source_channel_id":"<src>","destination_channel_id":"<dst>","member_limit":200}'   # -> PENDING job
curl -s -X POST $API/migrations/<id>/eligibility -H "$AUTH"                   # 202; the worker analyses
curl -s $API/migrations/<id> -H "$AUTH"                                       # poll until analysis_state=COMPLETED
curl -s -X POST $API/migrations/<id>/selection -H "$AUTH" -H 'content-type: application/json' \
  -d '{"telegram_user_ids":[111,222],"selected":false}'                       # optional: deselect members
curl -s -X POST $API/migrations/<id>/start -H "$AUTH"                         # RUNNING; the worker invites, slowly
curl -N $API/migrations/<id>/events -H "$AUTH" -H 'Accept: text/event-stream' # live progress
curl -s -X POST $API/migrations/<id>/pause|resume|cancel|retry -H "$AUTH"
```

**Start small.** Telegram publishes no invite quotas and new/low-reputation accounts hit `PEER_FLOOD` quickly. Try 10–20 members
first, keep `MIGRATION_MIN_DELAY_SECONDS` ≥ 5, and stop when Telegram pushes back.

## API

All under `/api`, bearer-token auth. Errors: `{"error": {"code", "message", "retry_after"?}}`; 429/409-cooldown carry `Retry-After`.
Every migration endpoint resolves the job **through the caller's own user id** (other users get 404); the worker re-verifies that
the job's Telegram account belongs to the job's owner on every run.

| Method | Path | Purpose |
|---|---|---|
| POST | `/migrations` | create (PENDING) from two channels you administer |
| POST | `/migrations/eligibility` | *Stage 1 one-shot*: create **and** analyse inside the request (small jobs) |
| POST | `/migrations/{id}/eligibility` | **202** — queue the analysis (worker); re-runnable while PENDING |
| POST | `/migrations/{id}/selection` | choose which ELIGIBLE members to migrate |
| POST | `/migrations/{id}/start` | PENDING → RUNNING + queue (`{"acknowledge_partial":true}` needed after a PARTIAL analysis) |
| POST | `/migrations/{id}/pause` · `/resume` · `/cancel` | cooperative control (resume refused during a cooldown) |
| POST | `/migrations/{id}/retry` | new linked job for unfinished members |
| GET | `/migrations` · `/migrations/{id}` | list (filter `status`) · full dashboard status |
| GET | `/migrations/{id}/members` | filters: `eligibility`, `processing_status`, `outcome`, `selected`, `search`; paginated |
| GET | `/migrations/{id}/events` | **JSON** event log (`after_seq`, `limit`) — or **SSE** with `Accept: text/event-stream` |
| GET | `/migrations/{id}/events/stream` | always SSE |
| DELETE | `/migrations/{id}` | erase job + members + events (refused while RUNNING/CANCELLING) |
| … | `/auth/*`, `/telegram/*`, `/health*` | unchanged from Stage 1 |

`GET /migrations/{id}` returns what a dashboard needs in one call: `status`, `pause_reason`, counters (`total_members`,
`processed_members`, `successful_count`, `skipped_count`, `failed_count`, `rate_limited_count`, `remaining_members`,
`percent_complete`), `current_member`, `rate_limited_until` + `rate_limit_remaining_seconds`, `worker_active`,
`allowed_actions` (which buttons to show), `outcome_counts`, `error_message`/`error_code`, `notes`, and the last 10 `recent_events`.

### SSE

Events: `migration.created|started|progress|member_success|member_skipped|member_failed|rate_limited|pause_requested|paused|resumed|cancel_requested|cancelled|completed|failed|recovered|retry_created`
(plus Stage 1's `ANALYSIS_*`). Stored events carry `id: <seq>` — reconnect with `Last-Event-ID` (or `?after_seq=`) and nothing is lost
or repeated. `migration.progress` is a computed snapshot (not stored), sent on connect and whenever anything changes:

```json
{"type":"migration.progress","migration_id":"…","status":"RUNNING","processed":150,"total":1000,"successful":110,
 "skipped":30,"failed":10,"rate_limited":0,"percent":15.0,"rate_limit_remaining_seconds":0,"current_member":{"id":"…","label":"@jane"}}
```

The stream tails the database (works with any number of API processes), sends `: ping` heartbeats, and ends by itself when the
job reaches a terminal state. **Browsers' `EventSource` cannot send an `Authorization` header**: the Stage 3 frontend should use a
fetch-based SSE client (e.g. `@microsoft/fetch-event-source`). No tokens in URLs.

## Configuration (all in `.env.example`)

`REDIS_URL` · `MIGRATION_CONCURRENCY` (1–5, default **1**) · `MIGRATION_MIN_DELAY_SECONDS` (default 5) · `MIGRATION_MAX_RETRIES` (4) ·
`MIGRATION_RETRY_BASE_SECONDS`/`_MAX_SECONDS` · `MIGRATION_MAX_CONSECUTIVE_FAILURES` (5) · `MIGRATION_LEASE_SECONDS` (120) ·
`WORKER_MAX_JOBS` (2) · `SSE_*` — plus the Stage 1 settings. With concurrency > 1 the min delay is still enforced between
operation *starts* across all coroutines of a job, so it is not a way to go faster.

## Logging, audit, privacy

- Structured lines: `event=member_result migration_id=… member_id=… status=SKIPPED error_code=USER_PRIVACY_RESTRICTED`.
  A redaction filter masks phone numbers, session strings and configured secrets; sensitive-looking keys are masked.
- `migration_events` (sequenced, per job) holds lifecycle + per-member events; `audit_logs` records `MIGRATION_CREATED`,
  `_ANALYSIS_REQUESTED`, `_SELECTION_CHANGED`, `_STARTED`, `_PAUSE_REQUESTED`, `_PAUSED`, `_RESUMED`, `_RATE_LIMITED`,
  `_CANCEL_REQUESTED`, `_CANCELLED`, `_COMPLETED`, `_FAILED`, `_RECOVERED`, `_RETRY_CREATED` with the user, resource and safe
  metadata only (credentials are stripped before storage; tests scan events/audit/logs for the session, phone, API hash).
- Data kept per member: Telegram user id, username, display name, role, eligibility, processing result. `access_hash` is stored
  (needed to reference the user) but never returned. `DELETE /migrations/{id}` erases it all.

## Tests

```bash
cd backend && source .venv/bin/activate
pytest -q                  # 225 tests; SQLite by default; the 5 arq tests need a local Redis and skip without one
TEST_DATABASE_URL=postgresql+asyncpg://USER:PASS@localhost:5432/scratch_db pytest -q   # same suite on PostgreSQL (tables are dropped!)
```

| File | Tests | Covers |
|---|---:|---|
| `test_job_state.py` | 28 | every valid/invalid transition (incl. `COMPLETED → RUNNING`), compare-and-set races, timestamps |
| `test_engine.py` | 52 | happy path, **confirmation before success**, idempotency, two workers, privacy/permanent refusals, already-member, backoff 1-2-4-8, bounded retries, circuit breaker, **FloodWait** (pause, persisted cooldown, nothing processed, auto-resume, account-wide cooldown), fatal errors, cooperative pause/cancel, **crash recovery**, cancelled task, concurrency, throttling, DB retry, logs/audit hygiene |
| `test_migration_api.py` | 34 | create/analysis in the worker, start validation, invalid transitions over HTTP, **ownership on every endpoint**, queue outages, retry (incl. >500-member batches), delete guard, filters, **SSE** (ordering, `Last-Event-ID`, live follow, countdown, heartbeat, deletion) |
| `test_arq_integration.py` | 5 | **real Redis + real arq worker**: API→Redis→worker→Telegram fake, deferred FloodWait resume, dedupe, unreachable Redis → 503 |
| `test_models_and_migrations.py` | 8 | constraints, cascades, Alembic upgrade/check/downgrade, **0002 backfill of real Stage 1 rows** |
| Stage 1 files | 98 | auth, Telegram login, channel/member discovery, eligibility, timeouts, security |

Telegram is replaced by a fake that raises **real Telethon error classes** and builds **real TL types**; no credentials are needed.

## Known limits / honest notes

- **Not yet run against real Telegram.** Error names, `missing_invitees` behaviour, exact FloodWait payloads and quotas can differ
  from what the fake models. Treat your first real run as an integration test: tiny batch, watch the events.
- Each member costs **3 Telegram requests** (membership check, invite, confirmation). That is deliberate (no false successes)
  and counts toward Telegram's limits.
- Legacy basic groups use `AddChatUser` and are verified via `GetFullChat`; this path is tested only against the fake.
- `docker compose up` has not been executed in the build sandbox (no Docker); the compose file parses and the same processes were run
  directly (see below).
- The auth throttle is per-process. Analysis via `POST /migrations/eligibility` (Stage 1 one-shot) still runs inside the request; use
  `POST /migrations` + `/{id}/eligibility` for anything large.
- arq job ids carry a time token so an orphaned queue entry from a crashed worker cannot block re-queueing; duplicates are harmless
  (the database lease allows one runner per job).

## Stage 3 — Mobile-first frontend (PWA)

The Stage 3 frontend lives in `frontend/` and uses React + TypeScript + Vite. It calls the existing Stage 1/2 FastAPI endpoints; it does not replace the migration worker or simulate the migration API. The UI includes:

- App account registration/sign-in (`/api/auth/*`)
- Telegram MTProto phone → code → optional 2FA connection (`/api/auth/telegram/*`)
- Connected account management and authorized channel discovery
- Mobile-first source/destination selection and eligibility-analysis wizard
- Filterable/paginated member results and per-member selection
- Migration list, detail view, live progress, SSE event timeline, pause/resume/cancel/retry, and deletion of non-running history
- Light/dark theme, responsive layouts, installable PWA manifest, and an offline shell fallback (Telegram operations require connectivity)

### Run the complete stack with Docker

1. Copy `.env.example` to `.env` and generate real secrets:

   ```bash
   python -m pip install cryptography
   python backend/scripts/generate_secrets.py
   ```

   Copy the generated `POSTGRES_PASSWORD`, `SESSION_ENCRYPTION_KEYS`, and `JWT_SECRET` into `.env`. Update the `DATABASE_URL` line so its password matches `POSTGRES_PASSWORD`. Set `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` from your own Telegram application at `https://my.telegram.org`.

2. Build and start the services from the project root:

   ```bash
   docker compose build
   docker compose up -d
   docker compose ps
   ```

3. Open the UI at `http://localhost:3000`. API docs are at `http://localhost:3000/docs`; the backend is also exposed locally at `http://localhost:8000/docs` for development.

4. View service logs if something does not start:

   ```bash
   docker compose logs -f backend worker frontend
   ```

5. Stop the stack:

   ```bash
   docker compose down
   ```

Persistent PostgreSQL and Redis volumes remain unless removed explicitly. Do not publish this setup to the public internet as-is: put TLS, appropriate access controls, and production secrets in place first.

### Run the frontend for development

Run the backend separately at `http://127.0.0.1:8000`, then:

```bash
cd frontend
npm install
npm run dev
```

Vite serves the UI at `http://localhost:5173` and proxies `/api` calls to the local backend. To validate the frontend:

```bash
npm test
npm run build
```

### Frontend integration notes

- Access tokens are held in `sessionStorage` (tab/session scoped), not `localStorage`.
- The SSE client uses `fetch-event-source` because native browser `EventSource` cannot attach the bearer `Authorization` header.
- The member eligibility label is not a promise that an invite will work. The UI uses “confirmed invited” only for the backend-confirmed outcome.
- The offline service worker caches only the application shell/static assets; it does not cache or fake API results and cannot process Telegram jobs while offline.
- The PWA SVG icon is a scalable single-file icon. For app-store submission or maximum iOS compatibility, provide dedicated raster icon sizes as a later polish step.

### Stage 3 verification status

The source files have been added and TypeScript/TSX syntax parsing completed without syntax diagnostics in the build sandbox. Full `npm install`, `npm test`, `npm run build`, and end-to-end Docker verification still need to be run on a machine with package-registry access and Docker available. Do not treat source creation or syntax parsing as proof that the complete runtime has passed.
