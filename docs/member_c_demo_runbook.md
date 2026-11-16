# Member C (Nisarg Vagh — 2026H1030066P) Demonstration & Viva Runbook

This is my step-by-step guide for the **12-mark individual demonstration**: task **(f)**, the ZooKeeper Lock Coordinator, and task **(b)**, Notification as a serverless function.

---

## 1. Responsibilities Overview

| Item | Requirement | Deliverable in codebase |
|---|---|---|
| **Service 1** | Lock Coordinator (gRPC + REST) | `services/lock_coordinator/`: `zk_store.py` (raw znodes), `grpc_server.py`, `rest.py`, `metrics.py` |
| **Service 2** | Notification (serverless function) | `services/notification/function.py` (Cloud Run function entry point), `handler.py`, `store.py` |
| **Task (f)** | Scenario concept: ZooKeeper | 3-node ensemble; ephemeral sequential znodes; fencing token = `czxid`; leases plus a reaper |
| **Task (b)** | Serverless, async, event-driven | Pub/Sub `bed-notifications` → Eventarc → Cloud Run function `hospital-notification-fn` → Firestore + Cloud Logging |
| **Platform** | Kubernetes, IaC, observability | `k8s/lock-coordinator/` (ZooKeeper StatefulSet ×3, Coordinator Deployment ×2, PodMonitoring), `infra/terraform/modules/notifications/` (function, Eventarc, DLQ, IAM) |
| **Contract** | gRPC API | `contracts/proto/lock.proto`; behaviour in `docs/contracts.md` → "Lock Coordinator" |

---

## 2. Test Verification

The tests run against real local dependencies (ZooKeeper, the Firestore emulator and the Pub/Sub emulator), not mocks. Start them first. Docker Desktop must be running.
```powershell
$env:GRAFANA_ADMIN_PASSWORD = "local-only"
docker compose up -d zookeeper-1 zookeeper-2 zookeeper-3 firestore-emulator pubsub-emulator
$env:ZK_TEST_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
$env:FIRESTORE_EMULATOR_HOST = "localhost:8080"
$env:PUBSUB_EMULATOR_HOST = "localhost:8085"
```

| Tests | Command | Expected |
|---|---|---|
| Lock Coordinator | `python -m pytest -q tests/test_lock_coordinator.py` | 26 passed |
| Notification (service + function + Pub/Sub end to end) | `python -m pytest -q tests/test_notification.py tests/test_notification_function.py tests/test_notification_pubsub.py` | 21 passed |
| Whole project, as CI runs it | `python -m tests.sandbox.cli infra-up`, then `python -m tests.sandbox.cli verify` | no `failed`, no `skipped` |

**Highlights worth naming in the viva:**
- `test_simultaneous_retries_on_two_replicas_share_one_place_in_line` fires the same retry at two replicas at the same instant, 10 times. It **fails on the old code and passes now**.
- `test_only_one_holder_at_a_time_and_tokens_increase`: 30 concurrent requests, never more than 1 holder.
- `test_a_renew_beats_a_reaper_holding_an_old_read` proves compare-and-swap.
- `test_a_duplicate_is_caught_by_a_second_copy_of_the_service` proves deduplication across instances.

---

## 3. Live Demonstration Walkthrough

### Demo 1: The lock under contention (task f)

**What to explain:**
> *"Two staff request the last ICU bed at the same moment. Without a lock, both see it free and both assign it. My Lock Coordinator gives one key per pool. Each request creates an **ephemeral sequential znode** under `/locks/<pool>`. The lowest number holds the key, and each waiter watches only the znode directly ahead of it, so a release wakes exactly one waiter: no herd effect. The fencing token is the znode's `czxid`, which only ever increases."*

**Commands:**
1. Start ZooKeeper and the Lock Coordinator:
   ```powershell
   docker compose --profile application up -d --build zookeeper-1 zookeeper-2 zookeeper-3 lock-coordinator-service
   ```
2. Alice takes `WARDA-ICU`; Bob asks for the same pool and is refused after 3 s:
   ```powershell
   python scripts/local/lock_demo.py alice WARDA-ICU
   python scripts/local/lock_demo.py bob WARDA-ICU
   ```
   *Expected:* `alice: GOT WARDA-ICU token=...`, then `bob: FAILED 504 DEADLINE_EXCEEDED`.
3. Open the lock inspector: `http://localhost:8010/api/v1/locks`. Alice is `HOLDING`, with her fencing token.
4. The Coordinator's logs show each grant with its **fencing token**, plus Bob's timeout:
   ```powershell
   docker logs hospital-lock-coordinator 2>&1 | Select-String '"event": "lock_'
   ```
   On GKE: `kubectl -n hospital-system logs -l app=lock-coordinator --tail 50`, or Cloud Logging with `jsonPayload.event="lock_granted"`.
5. Look at the raw znodes:
   ```powershell
   docker exec hospital-zookeeper-1 zkCli.sh -server localhost:2181 ls /locks/WARDA-ICU
   ```
6. Alice's lease is 2 minutes. When it runs out, the **reaper** deletes her znode (refresh the inspector), and Bob can now get the key.

---

### Demo 2: ZooKeeper failure and recovery (task f)

**What to explain:**
> *"The ensemble is 3 nodes with a quorum of 2. One node can fail and ZooKeeper keeps working. With two down it refuses writes, which is the safe choice: one node alone could split-brain. My Coordinator's `/readyz` reflects that, so Kubernetes stops routing to it."*

| Step | Command | Expected |
|---|---|---|
| Roles | `docker exec hospital-zookeeper-N zkServer.sh status` for N = 1, 2, 3 | 1 `leader`, 2 `follower` |
| Lock | `python scripts/local/lock_demo.py alice WARDA-ICU` | `GOT`, token like `4294967302` |
| Kill the leader | `docker compose stop zookeeper-<leader>` | A survivor becomes leader. `curl.exe -s localhost:8010/readyz` → ready. Alice is still in the inspector. |
| Lock with 2 of 3 | `python scripts/local/lock_demo.py bob WARDB-GEN` | `GOT`, token like `8589934595` (**a new epoch**) |
| Kill a second node | `docker compose stop zookeeper-<another>` | `readyz` → `not_ready`. `lock_demo.py carol WARDB-HDU` → `FAILED 504`. |
| Recover | `docker compose start zookeeper-<a> zookeeper-<b>` | ready again. `lock_demo.py dave WARDB-ISO` → `GOT` |
| Persistence | Stop and start all 3, then `docker exec hospital-zookeeper-1 zkCli.sh -server localhost:2181 ls /locks` | the pools are still listed |

**Recorded run (2026-11-13):** the leader moved from zk-3 to zk-2. Tokens went `4294967302` → `8589934595` → `12884901896` (epochs 1 → 2 → 3). Screenshots: 1 leader + 2 followers; new leader + `ready` + Bob `GOT`; `not_ready` + Carol `504`; `ls /locks` after a full restart.

To reset between runs: `docker compose rm -s -f zookeeper-1 zookeeper-2 zookeeper-3 lock-coordinator-service`.

---

### Demo 3: Notification as a serverless function (task b)

**What to explain:**
> *"Bed Allocation publishes each bed event to Pub/Sub topic `bed-notifications`. Eventarc delivers it as a CloudEvent to my **Cloud Run function** `handle_bed_notification`. It checks the event against the shared schema, and records it in Firestore in one transaction keyed by `event_id`, so a redelivered message is a duplicate. It tracks `stream_sequence` per pool to spot missing messages, and logs a staff notification to Cloud Logging. Malformed messages are acknowledged. Temporary failures raise, so Pub/Sub retries, and after 10 attempts the message goes to `bed-notifications-dlq`."*

**Locally (any time):**
```powershell
python -m pytest -v tests/test_notification_function.py tests/test_notification_pubsub.py
```
*Show:* delivered → duplicate, `ward: null` accepted, malformed → rejected, Firestore error → raise (retry). The end-to-end test publishes with Bed Allocation's **real** `PubSubPublisher`.

**On GCP (after the function is deployed):** project `hospital-management-514618`, region `asia-south1`.
1. Publish a test event. Use a fresh `event_id` for each new demo:
   ```json
   {"schema_version":"1.0.0","event_id":"11111111-2222-3333-4444-555555555555","stream_sequence":1,"event_type":"BED_ALLOCATED","occurred_at":"2026-11-16T10:00:00+00:00","request_id":"demo-1","allocation_id":"demo-alloc-1","admission_id":"demo-adm-1","pool_id":"WARDA-ICU","bed_id":"bed-warda-icu-01","ward":"ICU-A"}
   ```
   - **Console (easiest, and visual for the demo):** Pub/Sub → Topics → `bed-notifications` → Messages → **Publish message**. Paste the JSON and click Publish.
   - **Cloud Shell (bash):** save it as `event.json`, then run:
     ```bash
     gcloud pubsub topics publish bed-notifications --project hospital-management-514618 --message="$(cat event.json)"
     ```
     Don't do this from Windows PowerShell, because it strips the quotes inside the JSON.
2. Read the function's log. Use Logs Explorer, or this from any terminal:
   ```bash
   gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=hospital-notification-fn AND jsonPayload.event=staff_notification" --project hospital-management-514618 --limit 5 --freshness 10m
   ```
   *Expected:* `[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A allocated to admission demo-adm-1.`
3. Publish the **same** message again. The log shows `notification_duplicate` (change the filter to `jsonPayload.event=notification_duplicate`). Then publish `{"hello":"world"}`, and the log shows `notification_rejected`.
4. In the Firestore console, the `notifications` collection has **one** document for that `event_id`, and `notification_streams/WARDA-ICU` holds `highest_sequence`.

---

### Demo 4: Observability for my services

| What | Where |
|---|---|
| Lock inspector (who holds and who waits, per pool) | `http://localhost:8010/api/v1/locks` locally; port 8000 on the pod |
| Coordinator metrics: `lock_acquire_total`, `lock_wait_seconds`, `lease_expirations_total`, `grpc_requests_total` | `http://localhost:8010/metrics` |
| ZooKeeper metrics, e.g. `znode_count` | port 7000 on each node |
| Prometheus targets | `docker compose --profile application up -d lock-coordinator-service prometheus`, then `http://localhost:9090/targets`: `lock-coordinator` 1/1 and `zookeeper` 3/3 UP |
| GKE | PodMonitoring in `k8s/lock-coordinator/components/managed-prometheus`, enabled in the `gke` overlay |
| Notification logs | Cloud Logging, `jsonPayload.event` = `staff_notification`, `notification_duplicate`, `notification_gap` or `notification_rejected` |

---

## 4. Known limitations (say these before they're asked)

- **A caller that gives up during a ZooKeeper outage.** The server may still grant its lock when quorum returns, and the pool stays blocked until that lease expires (the reaper cleans it up). The fix would be to check that the caller is still connected before replying.
- **`Validate` is not atomic with downstream commits** (`docs/decisions.md` §3). A lease can expire just after Validate says yes. Fencing tokens checked by Bed Registry and Triage are the real guarantee.
- **Cut-over gap for Notification.** While the Cloud Run service is replaced by the function, messages published during that one apply are dropped, so it's done outside the demo.
