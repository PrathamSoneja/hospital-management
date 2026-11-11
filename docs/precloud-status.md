# Pre-GCP Status: Allocation, Triage, Load, Observability (Pratham)

Updated 2026-11-11. Branch `feature/precloud-renewal` from merged `master` (`4c12fd2`). Local commits only;
nothing pushed. Current results were produced at `ba2461d` (see "Revision"). Results from 2026-11-06 are kept
below as labelled history. `docs/handoff.md` sections dated before 2026-11-06 are historical.

**Status:** the Pratham-owned pre-GCP work is implemented and locally verified, with these exceptions:
- **Contention latency:** the 50-way contention scenario misses its local 8 s latency bound on this machine today. Correctness holds, and the identical 2026-11-06 code shows the same latency, so the cause is environmental.
- **GitHub CI:** a run has **not been observed** (no `gh`, and the private repository's API returns 403 from here).
- **Not covered here:** anything needing GCP or teammate services is listed under External blockers. Nothing here is GKE, HPA, ZooKeeper, or cloud evidence.

## What changed (and why)

| Area | Change |
|---|---|
| Sandbox | Tracked, test-only `tests/sandbox/` + `tests/support/sandbox_dependencies.py`. One shared, stateful process serves the Lock Coordinator, Bed Registry, and Staff Scheduler over their real gRPC contracts, plus the Patient Registry over HTTP. Its mutations are idempotent by `operation_id`, fenced by the current lease, and fault-injectable (`unavailable`, `lost_ack`, `delay`). Allocation (2 processes) and Triage run through their **production entrypoints**, configured only by environment variables. There is no adapter injection and no fake fallback. Infrastructure is the isolated Compose project `hospital-sandbox` (`tests/sandbox/compose.yaml`): PostgreSQL, the Firestore and Pub/Sub emulators, single-node KRaft Kafka, Prometheus, and Grafana. It uses tmpfs storage and ports 43000–49092. |
| Safety rails | Only `*_test` databases on localhost can be created or dropped. Only processes the sandbox started are stopped. Child processes get no `GOOGLE_APPLICATION_CREDENTIALS`, an empty gcloud config, and an unreachable metadata server, and this is tested. `infra-down` removes only the `hospital-sandbox` project. |
| Allocation fixes (found by sandbox load) | (1) A release no longer promotes a queue head whose own create is unresolved (enqueue committed, ACK lost); the durable `PROMOTE` is kept and retried. (2) Promotion completion is a guarded transition: WAITLISTED → ALLOCATED, and only while the release still owns `PROMOTE`. Recovery skips rows whose claim lapsed during a long batch. Before this, a stale runner could resurrect a released candidate, leaving one bed with two owners. (3) Create finalization never overwrites an already-settled row. (4) A pool-lock timeout during recovery used to reset the workflow to `CHECK_PRIORITY`, forgetting a committed enqueue whose ACK was lost. The retry then allocated a bed and orphaned the waitlist entry, which later blocked every promotion in the pool. The action in progress is now kept, and promotion removes a head entry whose allocation has moved past waitlisting (using a stable operation ID). All regression tests fail on the previous code (`tests/test_allocation_workflow_claims.py`). |
| Configuration | `RECOVERY_CLAIM_SECONDS` (5–600, default 60) replaces the fixed 60 s workflow and recovery claim. It must exceed the longest workflow step. It is added to `.env.gcp.example` and the staging overlay. No API, event, or schema contract changed. |
| Tests | Cross-process workflows, event delivery to real local brokers, dependency contract acceptance (sandbox or real endpoints), migrations, release gate, rendered-manifest consistency, sandbox isolation, and promtool alert unit tests (`monitoring/rules/alerts.test.yml`). |
| Load | `k6/contention.js` and `k6/sustained-multi-pool.js` now spread load across allocators. The sustained run discharges existing occupants, so waitlists and promotions happen under load. It retries 503/504 with the same Idempotency-Key, and its latency bound is an explicit parameter (staging default 3 s; local run 8 s). |
| Release readiness | `scripts/check_release_config.py` is the pre-sync gate. It rejects `PLACEHOLDER_*`, non-digest or all-zero images, `:latest`, and bad namespaces. The templates stay schema-valid with their placeholders. `scripts/smoke_image.py` checks health, readiness, and graceful SIGTERM shutdown for both images. |
| Terraform lock files (2026-11-11) | The lock files held only `windows_amd64` checksums, so `init -lockfile=readonly` + `validate` failed on Linux (reproduced in `hashicorp/terraform:1.15.8`). `linux_amd64` hashes were added for the same versions (google 6.50.0, confluent 2.93.0); nothing else in Ruchir's Terraform changed. |
| Migrations and roles (2026-11-11) | Head is `20261031_04`, which grants DML to `allocation_app`. Local bootstrap now creates `allocation_migrator`/`allocation_app` the way the runbook prepares Cloud SQL: migrations run as the migrator, and the sandbox services, audits, and image smoke run as `allocation_app`. Tests cover the 04 head, 04 -> 02 -> head, DML allowed, DDL refused (`42501`), default privileges on future migrator tables, and DML revoked after rollback. |
| Lease renewal (2026-11-11) | `renew()` on the lock client uses the existing `Renew` RPC with the client's bounded deadline. `LeaseRenewer` runs one thread per held lease and renews three times per lease period. Its deadline is measured on the monotonic clock from before each request. It is stopped before release, and holders sharing a `lock_id` share it (the last holder releases). Every mutation context passes an ownership check. After a refused renewal or actual expiry, no new mutation is sent and the workflow goes RECOVERING with stable operation IDs. Renewal does not cancel in-flight calls or fix the Firestore lease-validation race. Verified only against the tracked Coordinator double; **real Coordinator pending (Nisarg)**. |
| Demo controls (2026-11-11) | `POST /api/v1/allocations?pause_ms=0..30000&lock=false` is accepted only with `ALLOCATION_DEMO_MODE=true`; otherwise it returns 400 `DEMO_CONTROLS_DISABLED`. Demo mode is refused in production, and `LOCK_RENEWAL_ENABLED=false` is refused outside demo mode. `lock=false` skips only the pool lease: fenced Bed/Staff/Triage still refuse its mutations, and recovery completes later under a real lease. Expiry-demo profile: 5 s lease, renewal off (`up --profile expiry-demo`). Staging keeps its 10 s lease with renewal. |
| Service boundaries (2026-11-11) | `tests/test_service_boundaries.py` drives Ruchir's merged Staff Scheduler (gRPC + Firestore emulator) and Patient Registry (HTTP) with Allocation's real adapters. The Patient boundary passes. The Staff boundary exposed a defect (see blockers): it is a strict xfail, with a verified one-line fix in `docs/proposed-fixes/`. |
| CI | Pins Terraform 1.15.8 and kubectl v1.36.1 and validates Terraform offline. It renders and validates manifests with kubeconform and asserts the release gate rejects the templates. It runs promtool rule tests, then the **same sandbox CLI** as local runs (`verify`, `load`), builds and smoke-tests the images, and uploads JUnit, evidence, and rendered manifests. `REQUIRE_INTEGRATION=1` now also requires Kafka and the Pub/Sub emulator. |

## Commands (PowerShell, repository root)

```powershell
py -3.11 -m venv .venv; .\.venv\Scripts\python.exe -m pip install -r requirements.txt openapi-spec-validator
.\scripts\local\Sandbox.ps1 infra-up          # start/wait: Postgres, Firestore, Pub/Sub, Kafka, Prometheus, Grafana
.\scripts\local\Sandbox.ps1 verify            # full pytest suite (JUnit -> .sandbox\pytest\); any skip fails
.\scripts\local\Sandbox.ps1 load -Duration 2m # reset DB + Firestore project -> seed -> readiness -> k6 -> audit -> evidence
.\scripts\local\Sandbox.ps1 up                # interactive sandbox (fixed ports below) until Ctrl+C
.\.venv\Scripts\python.exe -m tests.sandbox.cli up --profile expiry-demo   # 5 s lease, no renewal, demo controls on
.\scripts\local\Sandbox.ps1 infra-down        # remove only the hospital-sandbox Compose project
```

The same commands run on Linux/CI as `python -m tests.sandbox.cli <command>`. Fixed interactive ports:

| Service | Port |
|---|---|
| Allocation REST | 47201, 47202 |
| Triage REST / gRPC | 47101 / 47151 |
| Shared stand-ins (gRPC / HTTP) | 47051 / 47080 (`/admin/state`) |
| Prometheus | 49090 |
| Grafana | 43000 (user `admin`, password `sandbox-only`) |

Other checks:

```powershell
.\.venv\Scripts\python.exe scripts\check_release_config.py        # exits 1 until every placeholder/digest is resolved
docker run --rm --entrypoint promtool -v "${PWD}\monitoring\rules:/r:ro" prom/prometheus:v2.50.0 test rules /r/alerts.test.yml
.\.venv\Scripts\python.exe scripts\smoke_image.py hospital-bed-allocation:<tag> --database-url <local *_test URL at head>
.\.venv\Scripts\python.exe scripts\smoke_image.py hospital-triage:<tag> --service triage --firestore localhost:48080 --port 18003
```

## Verification matrix (current, 2026-11-11 at `ba2461d`)

Main working tree (Ruchir's uncommitted `outputs.tf` edit present but untouched). `F` = `.sandbox\final-ba2461d\`.

| Check | Command | Result | Evidence | Status |
|---|---|---|---|---|
| Full suite, integration required | `python -m tests.sandbox.cli verify` (`REQUIRE_INTEGRATION=1`) | **167 passed, 1 xfailed (strict, documented Staff defect), 0 failed** | `F\junit.xml`, `F\verify.log` | VERIFIED |
| Migrations and runtime privileges | `tests/test_migrations_postgres.py` | 3 passed: empty -> `04` without drift; 02 -> `04` with legacy rows; `04` -> 02 -> `04`; `allocation_app` DML ok, DDL refused, default privileges, revoke on rollback | same | VERIFIED |
| Lease renewal | `tests/test_lease_renewal.py` | 8 passed: renew beyond expiry; refusal; unreachable until actual expiry; renewal disabled; gRPC `Renew`; workflow longer than the lease; lost lease -> no mutation, then recovery with the stable operation ID; shared lease. A mutation test (guard removed) fails | same | VERIFIED (Coordinator double) |
| Demo controls | `tests/integration/test_sandbox_demo_controls.py`, `tests/test_runtime_config.py` | rejected outside demo mode (400) and in production (startup); `lock=false` refused by fenced stand-ins then recovered; paused non-renewing holder loses its 5 s lease while another holder proceeds; invariants hold | same | VERIFIED |
| Patient Registry boundary | `tests/test_service_boundaries.py` | Allocation's HTTP adapter against Ruchir's app: OPEN admission, 404 -> not found | same | VERIFIED (GCS replaced by his test storage) |
| Staff Scheduler boundary | same | **Defect.** `AssignStaff` fails INTERNAL on Firestore (`transaction.get(CollectionReference)`). With the proposed one-line fix: boundary + his 5 tests pass | `docs/proposed-fixes/staff-scheduler-transaction-get.patch` | FAILED (teammate code; strict xfail) |
| Python sources | compile all tracked `.py` | 109 files, 0 errors | - | VERIFIED |
| Terraform | `fmt -check`; `init -backend=false -lockfile=readonly`; `validate`, both roots | Linux container and Windows: all pass after the lock-file fix (Linux failed before) | - | VERIFIED (no plan, state, or apply) |
| OpenAPI / protobuf / Compose | `openapi_spec_validator` x4; `compile_proto.py` + `git diff`; `docker compose config` x2 | all OK; no diff | - | VERIFIED |
| Manifests / release gate | kubeconform (K8s 1.30 strict); `check_release_config.py` | 16 valid, 2 CRDs skipped; gate exits 1 on templates (expected) | - | VERIFIED |
| Alert rules | `promtool check rules`; `promtool test rules` | 3 rules; SUCCESS | - | VERIFIED |
| Images | `docker build` x4; `smoke_image.py` | Allocation as `allocation_app`: healthz 200, readyz 200, 404, SIGTERM exit 0 (0.7 s). Triage: healthz 200, readyz 503 without Coordinator, exit 0 | tags `:ba2461d` | VERIFIED |
| Load: contention (correctness) | `python -m tests.sandbox.cli load --duration 2m` | 50 admissions, 10 beds, 2 allocators -> **10 ALLOCATED / 40 WAITLISTED / 10 distinct beds**, 0 violations | `F\load-evidence\audit-contention.json` | VERIFIED (MOCK dependencies) |
| Load: contention (latency) | same | p95 **12.6 s > 8 s bound** (k6 exit 99). k6 shows the time is server-side waiting (connect ~40 ms) and serialized under the pool lock. The 2026-11-06 code `2032c31` gives p95 12.8 s on this machine today (it was 4.9 s on 2026-11-06), so the cause is the environment, not today's changes. Bound not relaxed | `F\latency-investigation-20261111\` | FAILED (environment) |
| Load: sustained multi-pool | same | 2,877 requests, checks 99.87% (3 Triage PUT 504 after retries), p95 5.0 s; audit: 30 ALLOCATED / 246 WAITLISTED / 275 RELEASED, 0 unsettled, 6 outbox streams gap-free and drained | `F\load-evidence\` | VERIFIED (MOCK dependencies) |
| Observability | same (`observability.json`) | 3/3 targets up; Grafana healthy; Grafana -> Prometheus proxy returns data; endpoint labels are route templates; 4,550 JSON log lines carry k6 request IDs | `F\load-evidence\observability.json` | VERIFIED (local) |
| GitHub Actions CI | `.github/workflows/ci.yml` | **NOT OBSERVED.** No `gh`; the API returns 403 for the private repository. The reported failing step (Terraform) was reproduced and fixed locally | - | NOT OBSERVED |
| Lock Coordinator / Bed Registry (Nisarg / Jay) | - | Branches `feature/lock-coordinator` and `feature/bed-registry-and-analytics` are **not merged**; compatibility not run | - | PENDING |
| GKE, HPA, Cloud Logging, real brokers | - | - | - | EXTERNAL BLOCKER |

### Historical: 2026-11-06 at `2032c31` (superseded where rows overlap)

Evidence lives under the ignored `.sandbox\` directory on the machine that ran it. The load directories contain `manifest.json` (revision, dirty flag, commands, fixtures, limitations), k6 logs and summaries, audits, `observability.json`, and process logs.

All rows are local results at `2032c31` from a **fresh clone with a new virtualenv**, unless marked otherwise.
`R` = `.sandbox\rehearsal-2032c31\` (copied from the clone).

| Check | Command | Result | Evidence | Status |
|---|---|---|---|---|
| Full suite, integration required | `Sandbox.ps1 verify` (`REQUIRE_INTEGRATION=1`) | **147 passed, 0 skipped, 0 failed** (JUnit) | `R\junit.xml`, `R
ehearsal.log` | VERIFIED |
| Cross-process workflows | `tests/integration/test_sandbox_workflows.py` (in the suite) | 7 passed. Covers: waitlist, urgency update with stable queue identity, release/promotion, cancel; idempotent replay, mismatch, and concurrent retries; lost ACK with a stable operation ID; unavailable dependencies; allocator crash recovered after restart; stale fencing (Triage and Bed); independent pools plus 50×2-allocator contention | same | VERIFIED |
| Outbox → local brokers | `tests/integration/test_sandbox_events.py` | 3 passed. Covers: schema, keys and sequences on Kafka and Pub/Sub; Kafka outage with backoff while Pub/Sub continues, then drain; publish-then-crash redelivers the same identity, and the stale ACK is refused | same | VERIFIED |
| Dependency contract acceptance | `tests/acceptance` (sandbox target) | 4 passed | same | VERIFIED (real endpoints: EXTERNAL BLOCKER) |
| Migrations | `tests/test_migrations_postgres.py` | empty → head with no drift; 02 → head with legacy rows; 03 → 02 → head | same | VERIFIED |
| Load: contention | `Sandbox.ps1 load` | 50 admissions, 10 beds, 2 allocators → **10 ALLOCATED / 40 WAITLISTED / 10 distinct beds**, 0 violations; p95 4.9 s (bound 8 s) | `R\load-evidenceudit-contention.json`, `k6-contention.log` | VERIFIED (MOCK dependencies) |
| Load: sustained multi-pool | same, 2 min, 20 VUs, 3 pools, discharge churn | 4,414 requests, checks 99.97% (1 Triage PUT 504 after retries); audit: 40 ALLOCATED / 415 WAITLISTED / 418 RELEASED, 0 unsettled, 6 outbox streams gap-free and drained | `R\load-evidenceudit-sustained.json`, `k6-sustained.log`, `manifest.json` | VERIFIED (MOCK dependencies) |
| Observability | same (`observability.json`) | 3/3 Prometheus targets up; 3 rules loaded; Grafana datasource and dashboard provisioned; Grafana → Prometheus proxy query returns data; non-Kubernetes dashboard panels return series (HPA and pod-CPU panels need kube-state-metrics, so 0 series locally); endpoint labels are route templates only; 7,729 JSON log lines carry k6 request IDs | `R\load-evidence\observability.json` | VERIFIED (local) |
| Alert rules | `promtool check rules`, `promtool test rules alerts.test.yml` | 3 rules valid; all rule tests SUCCESS | (main tree, same files) | VERIFIED |
| OpenAPI / protobuf | `openapi_spec_validator` ×3; `compile_proto.py` and `git diff` | all OK; no diff | — | VERIFIED |
| Compose | `docker compose config` (main and sandbox) | valid | — | VERIFIED |
| Terraform (Ruchir's code, unchanged) | `terraform fmt -check`; `init -backend=false` and `validate` (both roots) | formatted; both roots valid (v1.15.8) | — | VERIFIED (no plan, state, or apply) |
| Kubernetes templates | `kubectl kustomize` and kubeconform (K8s 1.30, strict) | 16 valid, 2 CRDs skipped | — | VERIFIED |
| Manifest consistency / release gate | `tests/test_k8s_manifests.py`, `tests/test_release_config.py`, `check_release_config.py` | 5 and 3 passed; the gate exits 1 on the templates (expected) | — | VERIFIED |
| Images | `docker build` ×3; `smoke_image.py` (Allocation, Triage) | Allocation: healthz 200, readyz 200, 404, SIGTERM exit 0 in 0.7 s. Triage: healthz 200, readyz 503 without a Coordinator, exit 0 in 0.8 s | main tree, image tags `:2032c31` | VERIFIED |
| GitHub Actions CI | workflow in `.github/workflows/ci.yml` | **NOT RUN / NOT OBSERVED.** The branch is not pushed, and there is no `gh`/API access here | — | NOT RUN |
| GKE, HPA, Cloud Logging, real brokers or teammate services | — | — | — | EXTERNAL BLOCKER |

## Boundaries, risks, safeguards

- **Stand-ins are not teammate services.** The shared Lock, Bed, Staff and Patient process models exclusivity, expiry, monotonic fencing, operation-ID idempotency and ownership. It proves Allocation/Triage behaviour across processes. It does not prove ZooKeeper, Bed Registry or Staff Scheduler correctness, or distributed safety. The documented race between external lease validation and the Firestore commit remains unaddressed.
- **Local brokers are not production brokers.** The KRaft Kafka and Pub/Sub emulator test the real adapters only. Delivery is at-least-once: duplicates keep their `event_id`/`stream_sequence`, and neither exactly-once delivery nor strict broker ordering under claim expiry is claimed.
- **Local latency is not a performance result.** The single-pool 50-way contention p95 is pool-lock serialization on the emulators. It was ~4.9 s on 2026-11-06 and is ~12.6 s on this machine on 2026-11-11 for both the current code and the 2026-11-06 code. Sustained p95 is ~5 s locally. Neither is HPA or GKE evidence.
- **Lease renewal and demo controls are double-verified only.** The real Coordinator's `Renew` semantics (expiry clock, refusal reasons) need Nisarg's service. `lock=false` relies on downstream fencing; Jay's Bed Registry must refuse unlocked mutations, and that is not verified.
- **Residual load errors.** Up to about 1% of Triage PUTs (1 of 874 in the final run) still return 504 after 4 same-key retries under churn (pool-lock wait). These are retryable and leave no inconsistent state.
- **Production setting to review.** The Triage gRPC server uses 10 worker threads (`services/triage/grpc_server.py`). Size it against Allocation fan-out before staging load; it is unchanged here.
- **Cleanup.** `infra-down` removes only the `hospital-sandbox` containers (tmpfs, no volumes). The main `compose.yaml` stack, its `pgdata` volume, and the original `hospital_allocation` database are never touched.

## External blockers and acceptance checklist

| Item | Owner | Needed |
|---|---|---|
| Kafka platform: Terraform provisions Confluent Cloud, which conflicts with Kafka + ZooKeeper on GKE | Ruchir + Jay | Owner decision (not resolved here) |
| Staff Scheduler defect: `FirestoreStaffRepository._on_shift` passes a `CollectionReference` to `transaction.get()` | Ruchir | Apply or replace `docs/proposed-fixes/staff-scheduler-transaction-get.patch`; then remove the strict xfail |
| Terraform declares Allocation DB users, `pubsub.publisher`, Secret Manager IAM, GKE add-ons, and Pub/Sub -> Eventarc -> Cloud Run | Ruchir | Written in Terraform; **not planned or applied** (no state) |
| Lock Coordinator, including `Renew` | Nisarg | Merge, then run `tests/acceptance` and the renewal checks against the real service |
| Bed Registry: fencing refusal of unlocked or stale mutations, operation-ID idempotency | Jay | Merge, then run `tests/acceptance` (mutation checks with designated test resources) |
| Notification contract: `stream_sequence` and nullable `ward` | Nisarg | **Accepted, user-reported sign-off** (2026-11-11). His implementation is not verified here; Jay's Kafka consumer acceptance is still pending |
| Endpoints in `k8s/overlays/staging/*.properties`, image digests, release gate | Pratham + owners | After the merges and GCP outputs |
| GitHub CI run | Pratham | Observe the workflow for this branch's PR |

**Replacing stand-ins with real endpoints:**

1. Run the contract checks:
   ```powershell
   $env:ACCEPTANCE_LOCK_ADDR='<host:port>'; $env:ACCEPTANCE_BED_ADDR='...'; $env:ACCEPTANCE_STAFF_ADDR='...'
   $env:ACCEPTANCE_PATIENT_URL='https://.../api/v1'; $env:ACCEPTANCE_POOL_ID='<test pool>'; $env:ACCEPTANCE_ADMISSION_ID='<test admission>'
   $env:ACCEPTANCE_BED_ID='<test bed>'
   .\.venv\Scripts\python.exe -m pytest tests\acceptance -q
   ```
   These checks are read-only. Add `ACCEPTANCE_ALLOW_MUTATION=1` only with designated test resources, never production or real patient data.
2. Put the endpoints in `k8s/overlays/staging/*.properties`.
3. Pin the image digests (`scripts\gitops\Set-ImageDigest.ps1`).
4. Run `scripts\check_release_config.py` until it passes.
5. Follow `docs/gcp-integration-runbook.md`.

## Revision

Current: `ba2461d` on `feature/precloud-renewal` (from `origin/master` `4c12fd2`), plus this documentation commit. The tree is
dirty only because of Ruchir's uncommitted `infra/terraform/outputs.tf` edit, which is carried over and not committed. A stale,
duplicate `.gitignore` edit from the old local `master` was saved to the session scratchpad and dropped, because `origin/master`
already contains every line of it. Historical: `2032c31` (clean rehearsal clone, 2026-11-06).
