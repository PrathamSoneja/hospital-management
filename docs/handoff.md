# Project Handoff

Updated: 2026-10-29

## Latest Local Milestone (2026-10-29)

- **LOCAL-VERIFIED:** PostgreSQL 16 Compose service is healthy; original `hospital_allocation` remains at `20261029_01`, with its named volume preserved and no application rows. It was not migrated or reset during this milestone.
- **LOCAL-VERIFIED:** New empty disposable `hospital_allocation_bootstrap_test` was initialized with Alembic-only revisions `20261029_00` (base model tables) and `20261029_01` (active admission index). PostgreSQL showed all four service tables and the partial unique predicate.
- **LOCAL-VERIFIED:** Allocation app `/healthz` returned 200 and a DB-backed missing-allocation read returned 404 after migration. The app no longer calls `Base.metadata.create_all`; run Alembic before app startup.
- **LOCAL-VERIFIED:** `.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py -q` with `TEST_POSTGRES_URL` targeting the disposable DB -> **14 passed**.
- **MOCK-TESTED:** Allocation collaborators in those workflow tests remain fakes. Concurrent same-key/same-input ran one workflow but the competing call received 409 instead of replay; durable idempotency and recovery require an approved schema/workflow contract.
- **GENERATED-UNDEPLOYED:** Compose, `.env.example`, and Alembic default URLs specify `postgresql+psycopg2://`; Dockerfile includes Alembic configuration and scripts. Docker image was not built and no app container was restarted.
- **CLOUD-UNVERIFIED:** No GCP resources or deployed services were inspected or changed. Triage fencing and queue transaction behavior, event publishers, Firestore, Cloud SQL and HPA remain unverified here.

## Scope and Worktree

Member B owns Bed Allocation, Triage, HPA, k6, Prometheus/Grafana, and structured logging. No README, prior handoff, or workspace agent instructions were present during this review.

The service baseline and approved uniqueness work were pushed to `origin/master` in commits `da915bd` and `8a2220b`. The worktree was clean before this handoff update.

## Implemented and Checked

- Bed Allocation REST/domain code, PostgreSQL SQLAlchemy models, dependency clients/fakes, and outbox dispatcher are present under `services/bed_allocation/`.
- Triage REST/gRPC, Firestore and in-memory repositories, priority ordering, and fencing validation abstractions are present under `services/triage/`.
- HPA manifests use `autoscaling/v2`, Allocation 2–10 replicas, Triage 2–6, and 60% CPU targets. Both deployments set CPU requests. Deployment comments note ArgoCD should ignore `/spec/replicas`.
- Prometheus config, Grafana dashboard, alert rules, structured JSON formatter, and health/metrics middleware are present.
- `docs/contracts.md`, OpenAPI, protobuf, and event schemas provide the current contract sources. `scripts/compile_proto.py` exists.
- Workspace virtualenv baseline: `.venv\Scripts\python.exe -m pytest -q` -> **23 passed**.
- System interpreter baseline: `pytest -q` -> collection failed because `sqlalchemy`, `pydantic`, and `jsonschema` are not installed there. This is an environment mismatch; the workspace virtualenv has the test dependencies.

## Incomplete or Conflicting

- Allocation active predicate is now `status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')`, matching the partial unique index `uq_allocations_active_admission`. The service maps that index race to 409 `ACTIVE_ALLOCATION_EXISTS`; same-key completed replay remains checked first.
- An Alembic index migration reports existing duplicate active allocation IDs and does not delete rows. Alembic is declared in `requirements.txt`; the migration was applied to the local Compose PostgreSQL database and its index definition verified.
- Tests cover partial-index behavior on SQLite, rejection while `RECOVERING`, same-key replay, and a two-request PostgreSQL concurrency test using separate pooled connections and a temporary schema. The PostgreSQL test skips unless `TEST_POSTGRES_URL` points to local PostgreSQL.
- Triage production construction still uses `MockFencingValidator`; the real lock validator is not selected. Unknown/non-Firestore storage modes fall through to memory, and the app's default backend is memory.
- Historical audit note: Triage still does not wire `RemoteLockValidator`. Allocation Bed/Staff mutation RPC exceptions now raise retryable unknown-outcome errors; Allocation reuses stable operation IDs for recovery. Downstream deduplication remains unverified against live services.
- The non-fake allocation path still selects `FakeEventPublisher`; no production Pub/Sub/Kafka publisher or running outbox worker is wired.
- Historical audit note superseded by Step 2 below: Allocation now persists release before promotion and records retryable promotion actions. Unknown Triage queue-removal outcome after a lost response still lacks a durable idempotent remote operation result.
- No k6 scripts were found. Metrics and dashboard assets exist, but no k6 contention/scaling evidence has been produced.
- Structured JSON logging is configured for entrypoint messages, but service workflow and outbox logs are not consistently routed through that formatter.
- Triage REST `PUT` has no `Idempotency-Key` handling, and Triage REST endpoints do not explicitly propagate `X-Request-ID`. These conflict with the shared request/idempotency conventions; the needed persistence semantics must be agreed before implementation.

## Approved Decisions and Pending Implementation

- Approved urgency update, failure/recovery, public 202 recovery contract, active-admission uniqueness, Alembic, and Firestore triage fencing/idempotency decisions are recorded in `docs/decisions.md` and `docs/contracts.md`.
- Step 2 implementation status is recorded in the latest section below. Runtime recovery is covered locally with PostgreSQL and fakes; Firestore transaction/fencing and live downstream behavior remain unverified.

## Commands

- Full local suite with local PostgreSQL enabled: `.venv\Scripts\python.exe -m pytest -q` -> **26 passed** (2026-10-29).
- Focused uniqueness/recovery/replay checks: `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py -k "recovery or replay or partial_unique" -q` -> 2 passed, 1 skipped, 11 deselected.
- Allocation tests: `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py -q` -> 13 passed, 1 skipped.
- Triage and event schema tests: `.venv\Scripts\python.exe -m pytest tests\contracts\test_event_schemas.py tests\test_triage_service.py -q` -> 12 passed.
- Alembic revision discovery and offline SQL generation passed: `.venv\Scripts\python.exe -m alembic heads` and `.venv\Scripts\python.exe -m alembic upgrade head --sql`.
- Changed Python files passed Pylance diagnostics and `py_compile`; allocation OpenAPI YAML parsed successfully.
- PostgreSQL test: `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py::test_postgres_concurrent_requests_use_partial_unique_index -q` -> **1 passed** against the local Compose database.
- PostgreSQL migration applied locally with `.venv\Scripts\python.exe -m alembic upgrade head`; verified `uq_allocations_active_admission` on the local database. No cloud database was accessed.
- System interpreter suite: `pytest -q` (collection errors due to missing dependencies).
- No cloud resources were written, and no cluster deployment or billable action was run. Git commits `da915bd` and `8a2220b` were pushed to `origin/master` at the user's request.

## Exact Next Task

Complete the approved Allocation Step 2: durable release/cancel claims and recovery, release-before-promotion ordering, failure and crash tests, then commit coherent tested functionality slices. Do not begin Triage until these gaps close. Do not start cloud work until tomorrow's approved GCP window.

## Step 2 Update (2026-10-29, latest)

- **LOCAL-VERIFIED:** `20261029_02` reached head by Alembic from a fresh `hospital_allocation_step2_empty_test`; a separate `hospital_allocation_step2_upgrade_test` upgraded from `20261029_01` to `20261029_02`. The earlier `hospital_allocation` shared/original DB was not migrated. No volume was deleted.
- **LOCAL-VERIFIED:** Allocation + Triage + event-schema regression command with `TEST_POSTGRES_URL` on `hospital_allocation_bootstrap_test` -> **38 passed**. The Allocation-only suite is **26 passed**. Its PostgreSQL test uses separate pooled connections and creates/drops a temporary schema; PostgreSQL coverage includes same-key release contention/202, mismatch, target isolation, concurrent recovery workers, bed-released/staff-uncertain recovery, and release-before-failed-promotion with queue preservation. The remaining Allocation tests use SQLite fixtures and collaborator fakes; this is not distributed-service proof.
- **MOCK-TESTED:** Fake-backed tests cover cancel claim-before-remove and replay, release replay/mismatch, promotion compensation failure/retry, a lost bed-release acknowledgement, and a simulated crash after the release commit. The RemoveEntry durability gap was closed in the latest local change below.
- **LOCAL-VERIFIED:** `docker compose config --quiet` passed. `hospital-bed-allocation:local-step2` built. A disposable container against `hospital_allocation_step2_empty_test` returned `/healthz` 200 and a DB-backed missing-allocation 404, then only that smoke-test container was stopped/removed. The Compose PostgreSQL service was not restarted. Docker CMD now uses the valid `services.bed_allocation` Python module path.
- **LOCAL-VERIFIED:** Current Allocation recovery worker uses PostgreSQL `FOR UPDATE SKIP LOCKED` claims and resumes create, release, cancel, and promotion actions. `ENABLE_RECOVERY_WORKER=true` remains an explicit runtime setting. No live Bed Registry, Staff Scheduler, Lock Coordinator, or Triage gRPC server was used.
- **MOCK-TESTED:** Bed/Staff mutation RPC transport errors raise retryable 503/504 outcomes, allowing Allocation recovery rather than treating unknown results as capacity shortage. Remote operation deduplication and fencing still require the actual downstream contracts.
- **UNVERIFIED:** No GCP resources or Cloud Logging were inspected. Queue-fencing and event-publishing defects are not fixed by these PostgreSQL checks. `USE_FAKES=true` selects in-memory Patient/Triage/lock/bed/staff clients and a fake event publisher; setting it false selects gRPC/HTTP clients but still selects `FakeEventPublisher`.
- **UNVERIFIED / NEXT:** Broader Triage, event publisher, observability, and cloud milestones remain out of scope until Pratham confirms continuation. Queue fencing still validates external lock ownership before the Firestore transaction, so lease validation and transaction commit are not atomic.
- Step 2 functionality commits are local only: `8b2cf45` (`feat(allocation): claim idempotent create requests`), `bbff127` (`fix(allocation): preserve unknown RPC outcomes`), and `7dd3904` (`feat(recovery): resume allocation releases and cancellations`). No push occurred. The pre-existing staged `.gitignore` remains staged and excluded from all three commits.

### Latest runnable checks

- `$env:TEST_POSTGRES_URL='postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_bootstrap_test'; .venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_triage_service.py tests\contracts\test_event_schemas.py -q` -> **38 passed**.
- Empty DB: `$env:DATABASE_URL='postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_step2_empty_test'; .venv\Scripts\python.exe -m alembic upgrade head` -> `20261029_02`.
- Upgrade DB: target `hospital_allocation_step2_upgrade_test`, run `alembic upgrade 20261029_01` then `alembic upgrade 20261029_02` -> `20261029_02`.

## RemoveEntry Acceptance Gap Closure (2026-10-29)

- **LOCAL-VERIFIED:** Firestore now atomically writes the queue operation result and pool fence with queue insertion/removal. `queue_operation_results/{sha256(operation_id)}` records action, target IDs (including `entry_id` for removal), stable fingerprint, original context, result, `recorded_at`, and nullable `completed_at`. Fence tokens are canonical decimal strings and compared numerically. The existing RPC fields and `waitlist_entries/{allocation_id}` layout are unchanged.
- Matching completed enqueue/remove retries replay their stored result without fence advancement or queue mutation; operation ID reuse across enqueue/remove, changed removal reason/target, or changed entry identity conflicts. Missing entry without a removal result persists `OUTCOME_UNKNOWN`; it remains unresolved and is never treated as a successful delete or automatically re-executed if the entry later appears.
- **LOCAL-VERIFIED:** Firestore emulator tests cover commit/lost-response replay after repository recreation with a fresh lock, duplicate concurrent removes, conflicting reuse, distinct IDs, stale fencing, and unresolved missing-entry replay. PostgreSQL + Firestore emulator test simulates a lost remove response after Firestore commit and confirms Allocation recovery replays the durable result and promotes once. One recovery test exposed a missing `BedDto` import; the import is fixed.
- **LOCAL-VERIFIED:** `.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_allocation_firestore_recovery.py tests\test_triage_service.py tests\test_triage_firestore_operations.py tests\contracts\test_event_schemas.py -q` with `TEST_POSTGRES_URL` on disposable `hospital_allocation_pratham_test` and `FIRESTORE_EMULATOR_HOST=localhost:8080` -> **50 passed**.
- **MOCK-TESTED:** Allocation's downstream bed/staff/lock adapters remain fakes in the combined test. Firestore behavior itself used the local emulator. `MockFencingValidator` validates the adapter call; Firestore transaction enforces persisted pool fencing. The production service wiring still selects mock fencing and needs a separate approved Triage milestone.
- Existing external ownership validation occurs before the Firestore transaction and can race with lease expiry before commit; this limitation is documented and not solved here. No cloud writes, original database writes, volume deletion, or pushes occurred. No broader Triage or publisher work was started.
