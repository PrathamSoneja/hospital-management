# Pre-GCP Status: Allocation, Triage, Load, Observability (Pratham)

Updated 2026-11-12. **Combined tree tested:** `fc7a378` on local branch `integration/precloud-combined`, plus this
documentation commit. That tree is `origin/master` `1739e57` (PRs #6 Bed Registry, #9 Lock Coordinator, #10
Notification, #11 Coordinator hardening) merged with `feature/precloud-renewal` `285703b` (open PR #8) and the
local commits listed under "What changed". The branch is in the worktree `..\hm-combined`; nothing is pushed or
merged remotely. Evidence `C` = `..\hm-combined\.sandbox\` (ignored). The 2026-11-11 and 2026-11-06 results are
in Git history (`285703b:docs/precloud-status.md`) and are superseded.

**Status.** The required local suite and the load procedure pass on the combined tree. The sustained run settles within its unchanged 180 s window in **4 of 4 runs** after the Triage fix (`f4090d8`), and contention stays within the 8 s bound in 4 of 4. Allocation and Triage have been run against the
real Lock Coordinator (ZooKeeper), Bed Registry and Patient Registry. Exceptions:
- **Staff Scheduler (Ruchir):** BLOCKED on the Firestore defect; the proposed patch is verified (strict xfail).
- **Resource Analytics (Jay):** it counts redelivered events twice (strict xfail).
- **Before the Triage fix:** contention latency exceeded 8 s in 3 of 4 runs and sustained load failed to settle in 2 of 4 (history below).
- **Not run here:** anything on GKE, Cloud Logging, or cloud brokers.

**GitHub CI (observed through the public API):**
- PR #8 at `285703b`: `checks` success.
- `master` `1739e57`: `checks` **failure** at step 14 "Load procedure" (tests passed before it; images step skipped). The cause is not visible without authenticated logs/artifacts. `22e1060` was success.
- The combined tree has not been pushed, so no CI run covers it.

## What changed (local commits on the combined tree)

| Commit | Change and reason |
|---|---|
| `69fdec6` | Merged master (#6, #9). Kept Jay's active-admission replay and the fresh-session same-key recheck. Restored `-lockfile=readonly` (the lock files carry linux hashes). **Reverted:** migration 04's role-existence guard (it silently skipped grants) and the no-op metrics stub used when `prometheus_client` is missing (a silent production fallback). |
| `09cc309` | **Allocation race (found with the real services):** a worker read its next action before waiting for the pool lease. If its claim lapsed, a peer settled the row, and the stale worker then left it WAITLISTED with a reserved bed and staff. Fix: re-read the row under the lease and stop if it has settled. A deterministic regression test fails before the fix. |
| `21bce82`, `986b315` | Sandbox `actual=` mode runs the merged Lock Coordinator (local ZooKeeper), Bed Registry, Patient Registry and optionally Staff, seeded only through their APIs. The stand-in fences against the real Coordinator (`--lock-addr`). Audits read ownership from the real Bed Registry. `cli load`/`up` now use the real Lock and Bed. |
| `dffe2fe` | `BED_SERVICE_ADDR` pointed at port 50051; the Bed Registry Service and container use **50053**. |
| `6ba0b5b`, `132219a` | Structured logs: settled and recovering allocations, and 4xx/5xx responses with `request_id` and the route template (previously Allocation logged nothing per request, so the Cloud Logging 404 check could not succeed). Triage gRPC INTERNAL errors are now logged. Exhausted Firestore transaction contention (`ValueError` from `Aborted`) is a retryable 503, not INTERNAL. |
| `986b315` | Dashboard: Coordinator acquire outcomes and lease expiry, memory, Kubernetes health. Sandbox Prometheus scrapes the real Coordinator, Bed Registry and ZooKeeper (metrics on port 7000). |
| `95872e8` | Staging overlay includes the managed-Prometheus PodMonitoring (Terraform enables managed collection); ingress on 8000 admits `gmp-system`. Without this, no Allocation/Triage metrics would be collected on GKE. |
| `c0fc759` | The release gate also checks `k8s/lock-coordinator/overlays/staging`, which ArgoCD syncs (its placeholders were unchecked). |
| `5a69eea` | k6 `ADMISSIONS_FILE` plus `scripts/seed_load_fixture.py`: the real Patient Registry assigns admission IDs, so the k6 fixed-prefix IDs would 404 in staging. |
| `ae1609a`, `a6403bb` | Boundary/regression tests. Allocation against the real Coordinator: Release contract, lost Release ACK, Release=false on a reaped lease. #11's fixes are now regressions. Release racing Renew across replicas is covered. |
| `a5aac60` | Removed superseded scaffolding (below). |
| `f4090d8` | **Sustained-load settling (root cause below):** Triage queue transactions now use key reads only, via a `waitlist_admission_index` document per admission written/deleted in the same transactions, instead of a query by `admission_id` inside the transaction. One active entry per admission and stable operation-ID replay are unchanged (`tests/test_triage_queue_index_firestore.py`; the latency test fails on the old code with a 6.2 s enqueue). |
| `fc7a378` | Real-Coordinator tests wait for the reaper instead of sleeping (they failed under full-suite load because an expired-but-unreaped lease was still releasable by its owner, which the contract allows). |

## Verification on the combined tree

| Check | Result | Evidence | Status |
|---|---|---|---|
| Full suite, `REQUIRE_INTEGRATION=1` (Postgres, Firestore, Pub/Sub, Kafka, ZooKeeper) at `fc7a378` | **243 passed, 2 xfailed, 0 failed**, 0 unhandled-thread warnings. The two strict xfails are the Staff Scheduler defect and Analytics duplicate counting | `C\settleinal\junit.xml`, `full.log` | VERIFIED |
| Real services: `tests/integration/test_actual_services.py` (Lock+ZK, Bed, Patient real; Staff stand-in) | 12 passed in each of 3 targeted runs on the final tree and in the full suite; a 4th targeted run failed in sandbox setup (`alembic upgrade`, not reproduced, cause unknown). 9 passed / 2 xfailed ×3 before #11. Covers admission → triage → allocation, 404 admission, exhaustion/waitlist, urgency reorder, release → promotion, cancel, same-key retries vs mismatch/different key, 50 admissions/10 beds over 2 allocators, renewal of a 13 s operation past the 10 s lease, Coordinator crash/restart mid-lease, 5 s non-renewing expiry, `lock=false`, allocator crash recovery, stale tokens at Bed and Triage, same-acquire-ID across 2 Coordinator replicas, waiting Acquire + retry, Release racing Renew | `C\pytest-actual\` | VERIFIED (local) |
| Same with real Staff + proposed Staff patch (temporarily applied, reverted) | 8 passed / 2 xfailed (pre-#11); all four real services | `C\combined-evidence\actual-staff-patched.xml` | VERIFIED with patch |
| `tests/acceptance` in real-endpoint mode against local real services | 3/4; the Staff mutation fails INTERNAL (the defect). 4/4 with the patch | `C\combined-evidence\acceptance-real*.xml` | BLOCKED (Staff) |
| Release contract against the real Coordinator (`tests/test_allocation_real_coordinator.py`) | owner True; same-owner retry True (tombstone); wrong owner False; reaped lease False; released lease not renewable or valid. Lost Release ACK → RECOVERING → one recovery under a **fresh** lease (new lock_id, higher token) with **no repeated bed/staff mutation**. Release=false on a reaped lease → outcome kept, not undone, one recovery, then nothing to recover | same | VERIFIED |
| Concurrent same-acquire-ID / lease-start race (Nisarg) | Before #11: duplicates waited until DEADLINE_EXCEEDED, and a concurrent start_lease returned UNKNOWN (30/30). After #11: both pass | `test_actual_services.py` | FIXED by #11 |
| Seed script + k6 `ADMISSIONS_FILE` with all real services (Staff patched temporarily) | k6 checks 100%; 10 ALLOCATED / 40 WAITLISTED / 10 distinct beds; real Bed Registry ownership = ALLOCATED rows | `C\combined-final\k6-real-ids\` | VERIFIED (local) |
| Load: contention, `cli load` (real Lock + Bed; Patient/Staff stand-ins), 4 runs at `f4090d8` (clean tree) | **10 ALLOCATED / 40 WAITLISTED / 10 distinct beds, 0 violations, real Bed Registry ownership = rows, 4/4.** p95 6.64 / 6.29 / 6.23 / 5.94 s (bound 8 s, unchanged) | `C\evidence61112T142435Z`, `T142957Z`, `T143507Z`, `T144018Z` | VERIFIED (local) |
| Load: sustained multi-pool, same 4 runs | 7,380 / 7,208 / 7,508 / 7,594 requests; checks 100 / 99.96 / 100 / 100%; audit after the unchanged 180 s window: **0 unsettled, 0 undelivered, 6 gap-free outbox streams, 0 violations in 4/4**; 0 `allocation_recovering` events. Before the fix (`a6403bb` and earlier): about 2,500 requests per run, unsettled in 2 of 4 (59, 162 rows) | same; history `C\evidence61112T131132Z` | VERIFIED (local) |
| Notification (#10) | Its tests pass in the suite. All 691 real Allocation Pub/Sub events from a load run validate and render (286 with `ward: null`) | - | VERIFIED (local); Eventarc/Cloud Run **NOT RUN** |
| Migrations / runtime role | 04 head, 02 ↔ head, DML ok, DDL refused, revoke on rollback (in suite, strict migration restored) | junit | VERIFIED |
| Static at `a6403bb`: compile (146 files), OpenAPI ×4, proto drift, Compose ×2, Terraform fmt/init `-lockfile=readonly`/validate ×2, kubeconform (25 valid, 4 CRDs skipped; staging + migrate + lock-coordinator), release gate exit 1 on templates (3 paths), promtool SUCCESS | all as expected | `C\combined-final\static-checks.txt` | VERIFIED |
| Images at `a6403bb` | Allocation as `allocation_app`: healthz/readyz 200, missing 404, SIGTERM exit 0 (0.8 s). Triage: healthz 200, readyz 503 without a Coordinator, exit 0 | `C\combined-final\image-smoke.txt` | VERIFIED |

**Local latency is not a performance result.** Contention p95 is pool-lock serialization on one machine with emulators: 5.9 to 6.6 s after the fix, compared with 7.1 to 16.9 s before it. Neither is GKE evidence.

**Root cause of the settling failure.**
- **Reproduction:** the run at `20261112T131132Z` (logs and audit kept). After k6 stopped, recovery kept failing at `ENQUEUE_WAITLIST` about every 2 s (Allocation's Triage deadline) with almost no progress.
- **Classification of the unsettled rows:** mostly `RECOVERING`/`ENQUEUE_WAITLIST` (`SERVICE_UNAVAILABLE`, repeatedly), plus lock-wait timeouts (`ACQUIRE_LOCK`/`RELEASE_BED`, `DEADLINE_EXCEEDED`) and deferred promotions.
- **Mechanism:** Triage on the Firestore emulator answered `Aborted: Transaction lock timeout`, measured directly without Allocation. Three pools enqueueing concurrently, still one at a time per pool as the pool lease guarantees, gave p50 2 s and max 10 s with failures, against 0.05 s for one pool. A raw emulator experiment isolated the cause: a query inside a transaction followed by a write serializes transactions across pools (p50 4.1 s, with or without a pool filter), while key reads stay at 0.02 s.
- **Real defect or emulator limitation:** the coupling of all pools through a query lock is a design problem in Triage's storage access and is now fixed. Its size here comes from the emulator's lock handling. Real Firestore also takes locks for queries in transactions but resolves conflicts differently, and its impact was **not measured** (no cloud access). This is not claimed as a production fix.
- **Other paths checked, no defect found:** recovery after a lock-wait timeout keeps the pending action (`test_lock_timeout_during_recovery_keeps_the_outstanding_enqueue`). Enqueue retries reuse `<allocation_id>:enqueue-waitlist` and replay the stored result. No duplicate bed/staff ownership occurred in any run.
- **Stand-in Staff state:** it is in memory and is lost only if that process restarts. The load procedure never restarts it; the loss appeared only in a manual resume experiment, which was also invalid because it used a new Firestore project. Recovery code was not weakened, and the stand-in was not changed (not needed to settle).
- **Settings:** no timeout, window, worker count or bound was changed.

## Scaffolding removed / retained

- **Removed:** `scripts/local/Start-LocalStack.ps1`, `Stop-LocalStack.ps1`, `Invoke-LocalTests.ps1` (could no longer satisfy `REQUIRE_INTEGRATION`), `Invoke-LocalLoad.ps1`, the `.env.local` helpers in `LocalCommon.ps1`, and `docs/proposed-fixes/lock-coordinator-start-lease.patch` (superseded by #11). None was referenced elsewhere.
- **Retained:** `tests/support/sandbox_dependencies.py` and `stateful_lock.py`, for fault injection (lost ACK, unavailability, delay) and for the load fixtures' Patient/Staff. Also kept: the in-process fakes, `triage_coordinator_server.py`, the Firestore/Postgres emulator tests, and `tests/support/patient_registry_local.py` (the real Patient app with in-memory documents). Production imports nothing from `tests/`.

## Commands

```powershell
.\scripts\local\Sandbox.ps1 infra-up          # Postgres, Firestore, Pub/Sub, Kafka, ZooKeeper, Prometheus, Grafana
.\scripts\local\Sandbox.ps1 verify            # full suite; any skip fails
.\scripts\local\Sandbox.ps1 load -Duration 2m # real Lock + Bed; seed -> k6 -> audit -> evidence
.\scripts\local\Sandbox.ps1 up                # interactive (real Lock + Bed) until Ctrl+C
.\.venv\Scripts\python.exe -m pytest tests\integration\test_actual_services.py tests\test_allocation_real_coordinator.py -q
$env:SANDBOX_ACTUAL_STAFF='1'                 # adds the real Staff Scheduler (needs Ruchir's fix)
```

Fixed ports: Allocation 47201/47202, Triage 47101/47151, stand-ins 47051/47080, Lock 47351 (gRPC)/47301,
Bed 47451 (gRPC)/47401, Prometheus 49090, Grafana 43000.

## Pending actions

| Owner | Action |
|---|---|
| Pratham | Approve pushing the combined branch to update PR #8, then observe CI on it (master `1739e57` failed at the load step; this branch's load procedure now passes locally). Staging: image push, overlay values, approved migration, HPA/Cloud Logging run (runbook §11) |
| Ruchir | Apply `docs/proposed-fixes/staff-scheduler-transaction-get.patch`, then remove its strict xfail. Add Patient/Staff manifests and confirm `staff-scheduler-service:50051`, `patient-registry-service:8000`. Terraform: notification Cloud Run SA lacks `roles/datastore.user`; container lacks `FIRESTORE_PROJECT_ID` (the entrypoint raises `KeyError` without it); kube-state/cAdvisor collection for the HPA and health panels. Kafka/Confluent decision with Jay. No plan/apply yet |
| Jay | Bed Registry: manifests are not in any kustomization/ArgoCD app; Deployment has no database URL, so it silently uses in-memory SQLite (2 replicas, split state); it auto-seeds demo beds; it accepts token 0 on a pool with no fenced writes; CI does not build its image. Analytics: deduplicate by `event_id` (strict xfail) |
| Nisarg | Notification: Firestore project env and datastore role as above; Eventarc → Cloud Run delivery unverified. PodMonitoring, ZooKeeper one-node-down demo and DLQ are his, in progress. Lock requests carry no request ID (no field in `lock.proto`) |
| Shared contract | None changed |
