# Project Handoff

Updated: 2026-10-31

## Local Completion Milestone (2026-10-31, latest)

Supersedes every older statement below where they conflict. Branch `feature/outbox-delivery`, local only, not pushed.
Commits after `6ea9f85`: `7c20453` nullable-ward notifications, `5d896cd` cloud-profile config validation,
`7ea4653` 50-way PostgreSQL contention test + exclusive lock double, `fcbac79` CI services/strict skips,
`4610398` Allocation claim-race fix, `3801634` k6 scripts, `05dc2c4` readiness/secret files/non-root,
`6c25142` + `c4f6b1c` GKE/ArgoCD manifests, `6a88e60` scripts; this handoff/runbook commit follows.

- **LOCAL-VERIFIED (bug fixed):** the local k6 contention run exposed a real Allocation defect: an accepted create row had no recovery claim, so the recovery worker could start a second run sharing the request's Coordinator lease; one lock release then failed and the row looped forever in `RELEASE_POOL_LOCK` (66 attempts observed). Fixed by claiming at insert, claim-owner checks at workflow start, and resuming `RELEASE_POOL_LOCK` from durable outcomes. Regression tests fail on the old code and pass now.
- **LOCAL-VERIFIED:** `REQUIRE_INTEGRATION=1` full suite with disposable PostgreSQL (`hospital_allocation_pratham_test`) + Firestore emulator: **115 passed, 0 skipped** (Patient Registry tests included now that packages installed). Without `TEST_POSTGRES_URL` the run aborts instead of skipping.
- **LOCAL-VERIFIED:** OpenAPI (3 specs) valid; proto regenerates without diff; migration empty-bootstrap/upgrade/downgrade verified earlier and `alembic check` clean; Allocation image runs as uid 10001 and smoke passes (`healthz=200 readyz=200 missing_allocation=404`); Triage and Patient images build; promtool: 3 rules valid; `kubectl kustomize` renders; kubeconform strict (K8s 1.30): 16 valid, 2 CRDs skipped.
- **MOCK-TESTED (k6, grafana/k6:0.54.0 container):** contention 50 distinct admissions / 10 beds → settled `ALLOCATED=10 WAITLISTED=40 distinct_beds=10`, 0 violations. Sustained 3 pools, 20 VUs, 2.5 min: 12,400 requests, 0 failed, p95 884 ms; afterwards 0 duplicate active beds, every outbox stream gap-free, no pending actions. Lock/Bed/Staff/Patient were stateful doubles; Allocation used in-process Triage. Not distributed or HPA evidence.
- **LOCAL-VERIFIED (observability):** HTTP metric labels are route templates; 0 label values contain IDs after load; request ID echoed and present in structured JSON logs (`severity`, `request_id`).
- **GENERATED-UNDEPLOYED:** `k8s/base`, `k8s/overlays/staging` (placeholders only, digest-pinned images), migration Job (manual ArgoCD sync, separate schema-owner credential), ArgoCD Applications ignoring `/spec/replicas`, optional Managed Prometheus component, PowerShell cloud scripts (guarded, **UNEXECUTED**), `docs/gcp-integration-runbook.md`.
- **BLOCKED / teammate:** Terraform provisions Confluent Cloud Kafka (conflicts with Kafka-on-GKE); no Allocation DB users; Allocation GSA lacks Pub/Sub publisher + Secret Manager accessor; Secret Manager CSI, network-policy enforcement, Managed Prometheus not enabled; no Pub/Sub/Eventarc/Cloud Run, Kafka topic, or teammate endpoints; `stream_sequence` + nullable `ward` contract acceptance pending (Nisarg, Jay). Details and owners: runbook section 0.
- **Pending decision (unchanged):** staged deletion of `.env.example` (replaced by `.env.gcp.example`/`.env.local.example`); `infra/terraform/outputs.tf` edit left uncommitted for Ruchir.

### Required environment values (all still unset)

Project ID, region, AR repository, namespace, name prefix, Cloud SQL connection name, the four Secret Manager secrets, Lock/Bed/Staff/Patient endpoints, Pub/Sub topic, Kafka bootstrap/topic/protocol/mechanism/CA, ArgoCD project and repo URL/branch, billing account/budget. Source table: runbook "Variable sources".

### Next three cloud actions (each needs explicit approval)

1. Ruchir: resolve the Kafka/Confluent conflict, add Allocation DB users, GSA roles, Secret Manager secrets, and cluster add-ons; produce a reviewed Terraform plan (`Invoke-TerraformPlan.ps1`).
2. Pratham: push digest-pinned images (`Push-Images.ps1`), fill the overlay, run the approved `20261031_03` migration via the manual ArgoCD Application.
3. With teammate endpoints live: staging event/recovery tests and HPA load evidence (`Invoke-PostDeployChecks.ps1 -WatchHpa`).

## Outbox Milestone (2026-10-31)

- **Branch:** local `feature/outbox-delivery` (committing on `master` was blocked by review). Local commits: `0bc54ac` runtime/test-support split, `388bdbe` outbox sequencing+claims, `29426b6` broker adapters+worker, `d2fee50` observability labels/queries, `7a23330` Patient admission contract, `3717df0` k6+CI (unrun). Not pushed.
- **LOCAL-VERIFIED (outbox core):** migration `20261031_03` adds `outbox_stream_counters(destination, pool_id)`, `pool_id`, `stream_sequence`, `attempt_count`, `available_at`, claim owner/token/expiry, bounded `last_error`, and unique `(destination, pool_id, stream_sequence)`. Sequences come from the counter row incremented inside the business transaction (rollback consumes none; counters taken in fixed kafka→pubsub order). Workers claim only the head of each stream with `FOR UPDATE SKIP LOCKED`, commit, publish outside any transaction, then ACK/retry only if the claim token still matches. Backoff is exponential and capped. `POISONED` blocks only its stream. Operator actions: `retry_poisoned(event_id)` and `resolve(event_id, note)` (sets `RESOLVED`, row kept for audit).
- **LOCAL-VERIFIED (payloads):** emitted payloads validate against both schemas. Ward comes from the Bed Registry's bed record and patient ID from the persisted admission lookup; `ICU-A`/`pat-<id>` fabrications are removed. Occupancy: +1 allocated (including promotion, which previously emitted no event), −1 released, 0 waitlisted. `lock_wait_ms` is the Coordinator-reported wait observed once per acquisition (promotion sharing the release's lock reports null). `allocation_wait_ms` is acceptance→allocation, null otherwise.
- **DECISION NEEDED:** notification schema requires `ward`, but a waitlisted allocation has no bed. Until decided, the waitlisted Pub/Sub notification is **not emitted** (Kafka event still is). No ward is derived from the pool.
- **LOCAL-VERIFIED (migration):** fresh `hospital_outbox_empty_test` → `20261031_03`; `hospital_outbox_upgrade_test` at `20261029_02` with 3 legacy rows → upgrade (pool copied only from exact payload `pool_id`; `FAILED`→`POISONED`; sequence left NULL; counts logged) → downgrade → upgrade. `alembic check` reports no model drift. Shared/original DB untouched.
- **MOCK-TESTED:** Pub/Sub adapter waits on the publish future with a deadline; Kafka adapter uses `acks=all`, idempotent producer, key=`pool_id`, and requires a delivery report. Tested only with client doubles. `google-cloud-pubsub`/`confluent-kafka` are declared but not installed here (no package network). **Real broker publish NOT VERIFIED.**
- **At-least-once only:** publish-then-crash-before-ACK redelivers the same `event_id`/`stream_sequence` (tested). Claim tokens fence DB updates, not delayed broker writes; no exactly-once or strict broker-order claim under claim expiry. Consumers dedupe on `event_id` and detect gaps with `stream_sequence`; they must not drop lower sequences automatically.
- **TEAMMATE-BLOCKED:** `stream_sequence` acceptance by Nisarg (Notification) and Jay (Resource Analytics/Flink) is PENDING. Kafka bootstrap/topic/partition/security details and actual Pub/Sub topic access are needed; no cloud resource was queried or created.

### Legacy/poison operator procedure

1. Report: `OutboxDispatcher(...).stream_report()` and `.legacy_blockers()` / `.poison_rows()` (event IDs, destinations, pools — no payloads).
2. Poison: fix the cause, then `retry_poisoned(event_id)` (fresh attempt budget).
3. Legacy unsequenced rows (or poison that must not be delivered): confirm with the consumer owner, deliver out-of-band if needed, then `resolve(event_id, note)`. Nothing is skipped, deleted, or backfilled automatically. A legacy row with NULL `pool_id` blocks its whole destination.

### Outbox checks

- `$env:TEST_POSTGRES_URL='postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_pratham_test'; $env:FIRESTORE_EMULATOR_HOST='localhost:8080'; .venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_allocation_firestore_recovery.py tests\test_allocation_runtime_config.py tests\test_triage_service.py tests\test_triage_firestore_operations.py tests\test_triage_put_firestore.py tests\test_triage_lock_coordinator.py tests\test_patient_registry_client.py tests\test_observability_middleware.py tests\contracts\test_event_schemas.py tests\test_outbox_postgres.py tests\test_outbox_publishers.py -q` → **89 passed, 11 warnings, 0 skipped**.
- Mutation check: removing the per-stream predecessor guard fails 3 outbox tests.
- Without PostgreSQL/emulator (CI-like): `pytest -q --ignore=tests\test_patient_registry.py` → 65 passed, 24 skipped. `tests\test_patient_registry.py` still fails collection (`google.cloud.storage` missing). The CI workflow has no PostgreSQL/emulator services, so it would skip the outbox and Firestore tests.

### Next three actions

1. Decide the waitlisted-notification `ward` question; get Nisarg/Jay acceptance of `stream_sequence`.
2. With package network: install declared deps and run Patient tests; with approved broker details: one real Pub/Sub and Kafka publish against approved non-production targets.
3. Add disposable PostgreSQL (and the Firestore emulator) to CI if approved; then review k6 scripts and HPA manifests.

## Current Workspace Update (2026-10-31)

This section supersedes older status statements below where they conflict.

- **LOCAL-VERIFIED:** Focused regression command with `TEST_POSTGRES_URL` targeting `hospital_allocation_pratham_test` and `FIRESTORE_EMULATOR_HOST=localhost:8080`: `tests\test_allocation_service.py`, `test_allocation_firestore_recovery.py`, `test_allocation_runtime_config.py`, `test_triage_service.py`, `test_triage_firestore_operations.py`, `test_triage_put_firestore.py`, `test_triage_lock_coordinator.py`, `test_patient_registry_client.py`, `test_observability_middleware.py`, and `tests\contracts\test_event_schemas.py` -> **68 passed, 11 warnings, 0 skipped**.
- **LOCAL-VERIFIED:** `docker compose config --quiet`; Terraform format check; local YAML/JSON and OpenAPI reference checks passed. Prometheus readiness and alert rule group verified. Grafana health, Prometheus datasource, and provisioned dashboard verified through its local API.
- **GENERATED-UNDEPLOYED:** Allocation, Triage, and Patient Registry Docker image builds completed with local-check tags. These builds do not imply running/deployed services.
- **TEAMMATE-BLOCKED:** Patient Registry tests could not collect in the workspace virtualenv: `google.cloud.storage` is absent. Installing the declared `google-cloud-storage` dependency failed because package index DNS/network is unavailable. The HTTP admission-client contract tests passed independently. No cloud service was called.
- **LOCAL-VERIFIED:** Allocation's Patient Registry adapter now requires matching `admission_id`, nonempty `patient_id`, and nonempty `status`; it no longer fabricates values. Focused OpenAPI describes GET/POST admissions, and Terraform declares the Cloud SQL database-name output. No Terraform state is present; applied outputs/status are unknown and were not queried.
- **LOCAL-VERIFIED:** Explicit local Allocation demo launcher injects ignored stubs; production construction requires real dependency configuration and does not import `dev-local/` or `tests/support/`. Triage fake fencing and fake event publisher were removed from production modules. `tests/support/` remains tracked-capable and reusable. Compose retains PostgreSQL as actual local storage; app services are opt-in profile services.
- **UNVERIFIED:** Outbox publisher/worker and durable delivery workflow remain unimplemented. The approved claim/backoff/poison behavior still needs the approved durable per-stream sequence source; no event schema or database migration was added pending that decision. Pub/Sub and Kafka are not locally available. At-least-once duplicates remain possible; claim tokens alone cannot fence delayed broker writes.
- **LOCAL-VERIFIED:** k6 contention and sustained multi-pool scripts and CI workflow are present. k6 is not installed, scripts were not run, and no HPA scaling was observed. Cloud Logging is cloud-unverified.
- **TEAMMATE-BLOCKED:** All eight microservices are undeployed. A Patient Registry HTTP adapter contract is defined; Lock Coordinator is owned by Nisarg (gRPC/ZooKeeper), Bed Registry by Jay (gRPC), Staff Scheduler by Ruchi (gRPC), Notification by Nisarg (Pub/Sub/Eventarc), Resource Analytics by Jay (Kafka consumer/Flink), and Patient Registry by Ruchi (REST). Expected endpoints/RPCs are in `docs/contracts.md` and `contracts/proto/`; live endpoint configuration and deployed service availability are absent. Local Triage tests use explicit test collaborators; live Coordinator validation is not established.
- Tracked local-only paths staged for exact removal, with copies preserved locally: `.env.example` and `services/bed_allocation/clients/fakes.py`. No secrets are included in tracked configuration. `.env` and `dev-local/` are ignored; `tests/support/`, scripts, Compose, and real data models remain trackable.
- **UNCOMMITTED:** The Git review blocked committing on the current default branch and identified `.gitignore` staging as outside the retained approval. `.gitignore` is now unstaged; the dev-local/runtime slice remains staged, and contract/monitoring/k6/handoff changes remain unstaged. No commit was created.
- **CLOUD-UNVERIFIED:** No cloud queries, writes, deployments, pushes, original/shared database migration, reset, or volume deletion occurred. No HPA runtime scaling evidence. This work does not address Allocation queue-fencing or event-publishing defects.

### Current runnable checks

- `docker compose config --quiet`
- `$env:TEST_POSTGRES_URL='postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_pratham_test'; $env:FIRESTORE_EMULATOR_HOST='localhost:8080'; .venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_allocation_firestore_recovery.py tests\test_allocation_runtime_config.py tests\test_triage_service.py tests\test_triage_firestore_operations.py tests\test_triage_put_firestore.py tests\test_triage_lock_coordinator.py tests\test_patient_registry_client.py tests\test_observability_middleware.py tests\contracts\test_event_schemas.py -q`
- `terraform fmt -check -recursive infra\terraform`
- `k6 run k6\contention.js` and `k6 run k6\sustained-multi-pool.js` only against a disposable local target after installing k6; not yet run.

### Next three local actions

1. Finish image build and validate generated Patient Registry image/runtime contract; resolve missing local Python dependency when package access is available.
2. Resume outbox migration only after confirming the durable per-pool/per-destination sequence source and legacy-row handling; then test claims, lease expiry, stale ACKs, retry/backoff, poison blocking, and independent streams against disposable PostgreSQL and local brokers/emulators.
3. Re-run CI-equivalent checks and k6 on disposable targets, then commit reviewed functionality slices. No push or cloud work without separate approval.

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

## Previous Next Task (Superseded 2026-10-30)

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
- Step 2 functionality commits are local only: `8b2cf45` (`feat(allocation): claim idempotent create requests`), `bbff127` (`fix(allocation): preserve unknown RPC outcomes`), `7dd3904` (`feat(recovery): resume allocation releases and cancellations`), and `347a0f1` (`fix(triage): persist idempotent queue mutations`). No push occurred. The pre-existing staged `.gitignore` remains staged and excluded from all four commits.

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

## Broader Triage Milestone (2026-10-30)

- **LOCAL-VERIFIED:** Firestore REST PUT requires a validated `Idempotency-Key`; a canonical JSON scope is hashed for the ledger document ID, and the raw key is not stored or logged. Fingerprint mismatch returns 409. A completed matching replay returns the stored response before lock acquisition. New writes recheck the claim, immutable pool, active queue target, and decimal-string fencing state in one Firestore transaction; triage, any existing queue urgency, completed claim, and fence commit together. The transaction callback has no external effects. No queue row is created by urgency changes.
- **LOCAL-VERIFIED:** Same-pool urgency updates are restricted to levels 1–5, preserve `entry_id` and `queued_at`, and update triage plus an existing queue row atomically. Enqueue rechecks triage inside its transaction, serializing it against a concurrent first PUT. `pool_id` is immutable after first creation; concurrent initial writes to different pools allow one winner and return `POOL_CHANGE_NOT_SUPPORTED` for the loser without mutating the loser’s fence. Completed ledger replay works without a fresh lease.
- **LOCAL-VERIFIED:** GET triage missing-resource response, waitlist sort/pagination/cursor errors, request-ID response header, health/ready configuration, and Firestore dependency 503/504 mappings were checked. `healthz` is process-only; `readyz` checks bounded Firestore and Coordinator readiness. Unknown storage/fencing modes fail configuration. Memory storage and fake fencing are allowed only under `ENVIRONMENT=test`. `.env.example` leaves the Coordinator address empty; Compose does not invent a teammate service.
- **MOCK-TESTED:** `GrpcLockCoordinatorClient` Acquire/Validate/Release, request-ID metadata, deadlines, and unavailable handling were tested against a local gRPC test server implementing the existing protobuf. This verifies the adapter contract only.
- **TEAMMATE-BLOCKED:** No live teammate-owned Lock Coordinator service/endpoint is available locally. Therefore live ownership validation, lease semantics, and the external-validation-to-Firestore-commit race are not verified. The race is documented and not claimed fixed.
- **LOCAL-VERIFIED:** `.venv\Scripts\python.exe scripts\compile_proto.py` completed without generated-file diffs; Python `py_compile` passed for changed Triage/observability modules; OpenAPI YAML parsed and required paths were present; `docker compose config --quiet` passed. Existing PostgreSQL 16 and Firestore emulator containers were already running and were not restarted.
- **LOCAL-VERIFIED:** `TEST_POSTGRES_URL=postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_pratham_test` and `FIRESTORE_EMULATOR_HOST=localhost:8080` with `.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_allocation_firestore_recovery.py tests\test_triage_service.py tests\test_triage_firestore_operations.py tests\test_triage_put_firestore.py tests\test_triage_lock_coordinator.py tests\contracts\test_event_schemas.py -q` -> **59 passed, 0 skipped**. This combines PostgreSQL Allocation tests and emulator-backed Triage tests; Lock Coordinator is a local contract fake, not a live service.
- **CLOUD-UNVERIFIED:** No cloud services were contacted or changed. Queue-fencing’s external validation race, distributed lock behavior, Cloud Logging, and actual production Firestore remain unverified. Allocation downstream clients remain fake in the recovery regression; PostgreSQL verification does not fix queue fencing or event-publishing defects.
- **UNVERIFIED:** OpenAPI was parsed as YAML and structurally spot-checked; a dedicated OpenAPI schema validator was not available/run. Container image build/start was not part of this milestone. No tests were skipped in the regression command.
- Triage implementation commit: `3908c52` (`feat(triage): persist idempotent triage updates`). The pre-existing staged `.gitignore` remains staged and excluded. No push, cloud action, volume deletion, or original/shared database migration occurred.

### Next local milestone

Implement and locally verify real Pub/Sub and Kafka adapters plus durable outbox claims, per-destination delivery state, retry/backoff, and crash-safe concurrent workers. Do not deploy to cloud. Resolve teammate integration contracts before live broker wiring; no publisher correctness is implied by this Triage milestone.
