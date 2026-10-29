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
- `RemoteLockValidator` and RPC deadlines exist, but Triage does not wire the remote validator. Bed/staff mutation RPC exceptions are returned as ordinary failure tuples, which can turn an unknown remote outcome into a guessed failure; no stable-operation reconciliation path covers these cases.
- The non-fake allocation path still selects `FakeEventPublisher`; no production Pub/Sub/Kafka publisher or running outbox worker is wired.
- Allocation release and promotion mutate remote services before durable outcome/outbox persistence. Unknown downstream outcomes and failures between those steps have no complete reconciliation flow.
- No k6 scripts were found. Metrics and dashboard assets exist, but no k6 contention/scaling evidence has been produced.
- Structured JSON logging is configured for entrypoint messages, but service workflow and outbox logs are not consistently routed through that formatter.
- Triage REST `PUT` has no `Idempotency-Key` handling, and Triage REST endpoints do not explicitly propagate `X-Request-ID`. These conflict with the shared request/idempotency conventions; the needed persistence semantics must be agreed before implementation.

## Approved Decisions and Pending Implementation

- Approved urgency update, failure/recovery, public 202 recovery contract, active-admission uniqueness, Alembic, and Firestore triage fencing/idempotency decisions are recorded in `docs/decisions.md` and `docs/contracts.md`.
- This task implements only active-admission uniqueness and recovery-blocking coverage. Runtime recovery/reconciliation, urgency mutation, and Firestore transactional fencing/idempotency remain pending; do not infer they are implemented from the contract updates.

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

## Step 2 Update (2026-10-29)

- **LOCAL-VERIFIED:** Approved Allocation Step 2 is in progress. Forward migration `20261029_02` extends idempotency scope and stores workflow/recovery fields. It was applied to the disposable bootstrap DB; the original `hospital_allocation` DB was not changed.
- **LOCAL-VERIFIED:** Focused allocation suite against `TEST_POSTGRES_URL=postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_bootstrap_test` -> **17 passed**. The PostgreSQL contention test creates and drops an isolated schema inside that disposable database, uses separate pooled connections, and checks same-key replay and different-key active-admission conflict. This is local integration evidence with collaborator fakes, not distributed-service proof.
- **LOCAL-VERIFIED:** `docker compose config --quiet` passed. The Allocation image built as `hospital-bed-allocation:local-step2`; no application container was started or restarted.
- **MOCK-TESTED:** Create has a durable scoped claim before remote mutations, matching processing retries return the same allocation as 202 with `Location`, payload mismatch is 409, and uncertain staff assignment retains owned capacity in `RECOVERING`. Fake operation IDs deduplicate replay; crash-before-final-commit recovery was exercised.
- **UNVERIFIED / INCOMPLETE:** Recovery currently replays create workflows only. Release and cancellation still perform remote effects before their durable state/idempotency outcome; release/promotion ordering is not yet corrected. The recovery worker is opt-in (`ENABLE_RECOVERY_WORKER=true`) and cannot safely claim release/cancel work yet. Required release/cancel, failed-compensation, promotion-failure, crash/restart, concurrent-worker and actual downstream side-effect tests remain.
- **UNVERIFIED:** Migration `20261029_02` was applied to a DB already at `20261029_01`; a separately created empty database was bootstrapped through `20261029_00` and `20261029_01` earlier. No original/shared database migration was run.
- **UNVERIFIED:** Compose was validated, but services were not restarted. `USE_FAKES=true` selects in-memory patient/Triage/lock/bed/staff clients and a fake event publisher; setting it false selects the gRPC/HTTP clients but still uses `FakeEventPublisher`. Do not disable blindly; runtime wiring and recovery coverage are not yet production-ready.
- **TEAMMATE-BLOCKED:** Distributed lock/fencing correctness depends on Nisarg's Lock Coordinator contract; Bed Registry/Staff Scheduler RPC operation deduplication and Kafka/Flink ownership belong to teammates. Current local fake tests do not verify those remote semantics.
- Current Step 2 work has **not** been committed as a functionality slice because release/cancel durability remains unfinished. Preserve the pre-existing staged `.gitignore` change; never include it in later commits. Step 1 commit is `985e18dcfda3a0f513d5a74c670009d58de502d2` (`build(db): bootstrap allocation schema with alembic`).
