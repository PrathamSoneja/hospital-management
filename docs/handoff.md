# Project Handoff

Updated: 2026-10-29

## Scope and Worktree

Member B owns Bed Allocation, Triage, HPA, k6, Prometheus/Grafana, and structured logging. No README, prior handoff, or workspace agent instructions were present during this review.

The worktree contains existing user/agent work that must be preserved: `docs/decisions.md` is modified; the former `services/bed-allocation/` files are deleted; `services/bed_allocation/`, `services/triage/Dockerfile`, `compose.yaml`, `k8s/`, `monitoring/`, `tests/contracts/`, and `tests/test_allocation_service.py` are untracked. Do not restore or discard these changes.

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
- An Alembic index migration reports existing duplicate active allocation IDs and does not delete rows. Alembic is declared in `requirements.txt` and installed in `.venv` for this workspace. Migration execution against a database and the real PostgreSQL race remain unverified.
- Tests now cover partial-index behavior on SQLite, rejection while `RECOVERING`, existing same-key replay, and an opt-in two-request PostgreSQL concurrency test using separate pooled connections and a temporary schema. The PostgreSQL test skips unless `TEST_POSTGRES_URL` points to local PostgreSQL.
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

- Full local suite: `.venv\Scripts\python.exe -m pytest -q` -> **25 passed, 1 skipped** (2026-10-29). The skip is the opt-in PostgreSQL concurrency test.
- Focused uniqueness/recovery/replay checks: `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py -k "recovery or replay or partial_unique" -q` -> 2 passed, 1 skipped, 11 deselected.
- Allocation tests: `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py -q` -> 13 passed, 1 skipped.
- Triage and event schema tests: `.venv\Scripts\python.exe -m pytest tests\contracts\test_event_schemas.py tests\test_triage_service.py -q` -> 12 passed.
- Alembic revision discovery and offline SQL generation passed: `.venv\Scripts\python.exe -m alembic heads` and `.venv\Scripts\python.exe -m alembic upgrade head --sql`.
- Changed Python files passed Pylance diagnostics and `py_compile`; allocation OpenAPI YAML parsed successfully.
- PostgreSQL availability: no local PostgreSQL service or binaries, Docker daemon unavailable, and `localhost:5432` connection check failed. The PostgreSQL concurrency test was not run.
- System interpreter suite: `pytest -q` (collection errors due to missing dependencies).
- No cloud writes, remote pushes, cluster deployment, or billable action was run.

## Exact Next Task

Start an isolated local PostgreSQL database, set both `DATABASE_URL` and `TEST_POSTGRES_URL` to that local database, run `.venv\Scripts\python.exe -m alembic upgrade head`, then run `.venv\Scripts\python.exe -m pytest tests\test_allocation_service.py::test_postgres_concurrent_requests_use_partial_unique_index -q`. Inspect and resolve any duplicate report explicitly; do not run the migration against cloud resources or claim PostgreSQL concurrency correctness until the real test passes. Next, implement the approved recovery lifecycle.