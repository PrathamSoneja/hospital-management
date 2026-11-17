# Running, demonstrating and defending the project

How to run the whole system (Part A), the demo script with every requirement of the brief (Part B), viva
preparation for each member (Part C) and troubleshooting (Part D). What the system is and how it works:
`docs/PROJECT.md`.

Project `hospital-management-514618`, region `asia-south1`, cluster `hospital-gke` (zone `asia-south1-a`),
namespace `hospital-system`. Use **synthetic data only**.

---

# Part A — Running the project

## A.1 Access (every member, once)

```bash
gcloud auth login
gcloud config set project hospital-management-514618
gcloud container clusters get-credentials hospital-gke --location=asia-south1-a --project=hospital-management-514618 --dns-endpoint
kubectl -n argocd get applications
```
The DNS endpoint is IAM-gated, so no IP allowlist or VPN is needed. Every Application should be `Synced` and
`Healthy` (`hospital-load-test` stays OutOfSync until a load test is run; that is expected).

## A.2 Provision the infrastructure (Terraform, Ruchir)

The state lives in `gs://hospital-management-514618-tf-state` (prefix `terraform/state`). Run from
`infra/terraform` in **one PowerShell session** (see D.1 for why):

```powershell
terraform init -backend-config=backend.hcl          # from backend.hcl.example: bucket = "hospital-management-514618-tf-state", prefix = "terraform/state"
$env:TF_VAR_confluent_cloud_api_key = "<Cloud resource management key>"
$env:TF_VAR_confluent_cloud_api_secret = "<its secret>"
# Database passwords only when rotating: set all TF_VAR_*_db_password and bump every *_db_password_version
terraform plan -var-file staging.tfvars -out tfplan.bin
terraform apply tfplan.bin
Remove-Item tfplan.bin; Remove-Item Env:TF_VAR_*
```
`staging.tfvars` is local and never committed; it holds non-secret values (project, CIDRs, password *versions*,
`eventarc_subscription_id`, `flink_operator_members`). A fresh project also needs, once: the `bootstrap` stack (state
bucket) first, and the two-step Eventarc import (apply with `eventarc_subscription_id = ""`, then with the
subscription name from `terraform output notification_function_trigger`). A plan must never show GKE, Cloud SQL,
Firestore, buckets or the VPC being replaced.

## A.3 Deploy the services (Argo CD, Pratham)

1. **Argo CD** is installed by the `infra/terraform/bootstrap_k8s` stack (Helm).
2. **Register the root Application once** (this is the only `kubectl apply`; it registers Argo CD, not a service):
   ```bash
   kubectl apply -f gitops/bootstrap/root-application.yaml
   ```
   Lock Coordinator, Patient Registry and Staff Scheduler then sync automatically.
3. **Sync the others in this order** (Argo CD UI → Sync, or the command below):
   `hospital-bed-registry-staging` → `hospital-allocation-triage-staging` → `hospital-allocation-migrations-staging`
   → `hospital-monitoring` → `hospital-resource-analytics-staging` → `hospital-demo-ui`.
   ```bash
   kubectl -n argocd patch application <name> --type merge -p '{"operation":{"sync":{}}}'
   ```
   Allocation + Triage must come before the migrations because it creates the `bed-allocation-sa` service account
   the migration Job uses; the Allocation pods become ready once the migration has run.
4. **Images.** Every merge to `master` runs CI, builds the 8 images, pins their digests in `k8s/` and commits
   `chore(gitops): promote images …`. Afterwards sync the manual apps again. The repository variables
   `GCP_WIF_PROVIDER` and `GCP_WIF_SERVICE_ACCOUNT` enable this (identifiers, not secrets).

## A.4 Open the consoles

```bash
kubectl -n hospital-system port-forward svc/demo-ui 8080:80        # demo console  → http://localhost:8080
kubectl -n hospital-system port-forward svc/grafana 3000:3000      # Grafana       → http://localhost:3000
kubectl -n argocd port-forward svc/argocd-server 8443:443          # Argo CD       → https://localhost:8443
kubectl -n hospital-system get ingress                             # public URL IP → http://<ip>/docs
```

## A.5 Flink on Dataproc (Jay)

```bash
gcloud dataproc clusters start hospital-dataproc-flink --region=asia-south1
gcloud compute ssh hospital-dataproc-flink-m --zone=asia-south1-c --tunnel-through-iap --command="flink --version"
KAFKA_BOOTSTRAP_SERVERS=pkc-41p56.asia-south1.gcp.confluent.cloud:9092 \
FLINK_KAFKA_CONNECTOR_JAR=gs://hospital-flink-514618/jars/flink-sql-connector-kafka-1.15.4.jar \
bash services/resource_analytics/submit_flink_job.sh
```
Run from Cloud Shell (bash). The connector jar must match the Flink version (image 2.1 → Flink 1.15); check its SHA-1
against Maven Central before staging it in `gs://hospital-flink-514618/jars/`. The script reads the Flink Kafka
credentials from Secret Manager in memory and submits over IAP SSH. Stop the cluster when not in use:
`gcloud dataproc clusters stop hospital-dataproc-flink --region=asia-south1`. After a restart the job must be
submitted again.

## A.6 Load test (HPA, Pratham)

```bash
kubectl -n argocd patch application hospital-load-test --type merge -p '{"operation":{"sync":{}}}'
kubectl -n hospital-system get hpa -w
kubectl get pods -A --field-selector=status.phase=Pending
```
The Argo CD sync creates the k6 Job (`k6/load-test-job.yaml`): it seeds 2 pools (`LOADTEST-A/B`, 10 beds and 600
synthetic admissions each), ramps to 40 virtual users over 2 minutes and holds for 10 minutes. Watch Grafana's
"HPA current vs desired", RPS and p95 panels. Stop early with `kubectl -n hospital-system delete job hpa-load-test`.
Two nodes cannot hold both HPAs at their maximum; a few Pending pods at the peak are expected — do not add a node
without approval.

## A.7 Local development and tests

```bash
python -m venv .venv && .venv/Scripts/activate      # Windows; use bin/activate elsewhere
pip install -r requirements.txt
python -m pytest -q                                   # unit tests (integration tests skip without the sandbox)
python -m tests.sandbox.cli infra-up                  # PostgreSQL, Firestore/Pub/Sub emulators, Kafka, ZooKeeper
python -m tests.sandbox.cli verify                    # full suite including integration (what CI runs)
python -m tests.sandbox.cli load --duration 45s --vus 15
python -m tests.sandbox.cli infra-down
```
`compose.yaml` runs the services locally (`docker compose --profile application up -d --build`), with
`.env.local.example` as the template.

---

# Part B — Demo script (about 20 minutes, four presenters)

Before starting: A.1 and A.4 done; all apps Synced/Healthy; Flink job running (A.5); Grafana showing data. Use a
fresh pool ID per run, e.g. `DEMO-ICU-<time>`. Each presenter shows their two requirements and two services.

## B.1 Ruchir — (a) Infrastructure as Code, (d) GitOps · Patient Registry, Staff Scheduler

1. **(a)** `infra/terraform`: modules for network, GKE, Cloud SQL, Firestore (with indexes), storage, registry, IAM,
   notifications (function, Pub/Sub, Eventarc), Dataproc and Confluent. Show `terraform state list | head -30` and
   explain the GCS backend and write-only passwords.
2. **(d)** Argo CD UI: the root app and its children, all tracking `master`; show a `chore(gitops): promote images`
   commit and the matching image digest in a running pod. No service is deployed with `kubectl apply`.
3. **Staff Scheduler:** demo console step 1, "Create beds + on-shift nurse" → Firestore `staff` document with a shift.
4. **Patient Registry (public URL):** open `http://<ingress-ip>/docs`; `POST /api/v1/patients` (body
   `first_name`, `last_name`, `date_of_birth`; header **`Idempotency-Key`**), `GET` it back, `POST /api/v1/admissions`.
   Without the header the API returns 400; repeating the key returns the same patient.
5. **(g) Cloud Storage:** `POST /api/v1/patients/{id}/documents` with `document_type=REFERRAL_LETTER` and a small
   synthetic PDF; `gcloud storage ls gs://hospital-patient-docs-514618/patient-documents/` shows it. Cloud SQL
   Studio (`patients` database) shows the rows.

## B.2 Pratham — (c) GKE, HPA and load test, (h) observability · Bed Allocation, Triage

1. Demo console: admit 3 patients with urgency 3, 2 and 1 into a pool with 2 beds; **Allocate all pending
   concurrently** → 2 ALLOCATED on different beds, 1 WAITLISTED; no bed used twice.
2. **Repeat** an allocation with the same key → the same allocation is returned (idempotency).
3. Waitlist panel → release one allocation → the waitlisted admission becomes ALLOCATED on the freed bed.
4. **(c)** Start the load test (A.6); `kubectl -n hospital-system get hpa -w` shows Bed Allocation and Triage scaling
   out; Grafana shows RPS, p95 latency and HPA current vs desired. Recorded run:
   `docs/evidence/2026-11-16-gke/11-hpa-load-test.txt`.
5. Resilience: `kubectl -n hospital-system delete pod -l app=bed-allocation-service --wait=false` during
   allocations → requests complete or recover; nothing stays RECOVERING.
6. **(h)** Grafana dashboard panels (RPS, 5xx, p50/p95, outcomes, lock waits, outbox backlog, HPA, CPU, restarts);
   Logs Explorer: `resource.type="k8s_container" jsonPayload.event="allocation_settled"`.

## B.3 Nisarg — (f) the ZooKeeper concept, (b) serverless function · Lock Coordinator, Notification

1. `kubectl -n hospital-system get pods -l app=zookeeper` (3 members) and `-l app=lock-coordinator` (2 replicas).
2. **(f)** During B.2 step 1:
   `kubectl -n hospital-system logs -l app=lock-coordinator --since=5m | grep <pool>` → `lock_granted` with strictly
   increasing fencing tokens and growing `waited=` times, each followed by `lock_released`
   (`07-lock-logs.txt`). Grafana: "Lock Coordinator: Acquire Outcomes and Lease Expiry".
3. **(f) Failure tolerance:** `kubectl -n hospital-system delete pod zookeeper-1` → the ensemble keeps a quorum,
   allocations keep working, the pod returns with its data. Losing two of three nodes (quorum loss, readiness 503,
   acquire 504, recovery, persistence) is shown on the local Compose ensemble:
   ```powershell
   docker compose --profile application up -d --build zookeeper-1 zookeeper-2 zookeeper-3 lock-coordinator-service
   python scripts/local/lock_demo.py alice WARDA-ICU      # GOT, token
   docker compose stop zookeeper-<leader>                  # new leader, still ready; token jumps to a new epoch
   docker compose stop zookeeper-<another>                 # /readyz not_ready; lock_demo → FAILED 504
   docker compose start zookeeper-<a> zookeeper-<b>        # ready again
   ```
4. **(b)** Logs Explorer:
   `resource.type="cloud_run_revision" resource.labels.service_name="hospital-notification-fn"` → one
   `staff_notification` per allocation, waitlisting and release of the pool.
5. **(b)** Publish a synthetic event twice and a malformed one (Cloud Shell, pool `NISARG-TEST`, fresh `event_id`):
   ```bash
   E='{"schema_version":"1.0.0","event_id":"0f0e0d0c-0000-4000-8000-000000000001","stream_sequence":1,"event_type":"BED_ALLOCATED","occurred_at":"2026-11-16T15:00:00Z","request_id":"demo-1","allocation_id":"alloc-test-1","admission_id":"adm-test-1","pool_id":"NISARG-TEST","bed_id":"BED-T1","ward":"TEST"}'
   gcloud pubsub topics publish bed-notifications --message="$E"
   gcloud pubsub topics publish bed-notifications --message="$E"
   gcloud pubsub topics publish bed-notifications --message='{"hello":"world"}'
   ```
   → `staff_notification`, then `notification_duplicate`, then `notification_rejected`; Firestore `notifications`
   holds one document. Show the DLQ policy:
   `gcloud pubsub subscriptions describe eventarc-asia-south1-hospital-notification-fn-336429-sub-587 --format="yaml(deadLetterPolicy,retryPolicy)"`.

## B.4 Jay — (e) Flink and Kafka, (g) storage · Bed Registry, Resource Analytics

1. **Bed Registry fencing:** port-forward `svc/bed-registry-service 8003:8000`, then
   `GET /api/v1/pools/<pool>/fencing-token`, and a `POST /api/v1/beds/<bed>/allocate` with a token **lower** than
   the recorded one → 409 stale fencing token. This is what stops a paused lock holder.
2. **(e)** Confluent Cloud → cluster `pkc-41p56` → topic `bed-events` → messages from the allocations in B.2.
3. **(e)** Dataproc → Web Interfaces → YARN → Flink: job `hospital-bed-stats-1m` RUNNING, checkpoints completing in
   `gs://hospital-flink-514618/checkpoints`.
4. **(e)** Topic `bed-stats`: one record per active pool per minute (`requests`, `allocated`, `waitlisted`,
   `released`, `conflicts`, `contention_rate`, average lock and allocation wait).
5. **(e)** Resource Analytics: Firestore `pool_stats` documents and the demo console's analytics panel
   (`GET /api/v1/analytics/summary`); Logs Explorer `resource.type="cloud_dataproc_cluster"` for the job's logs.
6. **(g)** The three storage products and why each: Cloud SQL (relational, ACID, fencing state), Firestore
   (high-write documents, analytics windows), Cloud Storage (documents, checkpoints).

## B.5 Requirement checklist (tick during the demo)

| Requirement | Shown in |
|---|---|
| ≥ 8 microservices | B.1 step 2 (Argo CD) + Notification function in B.3 |
| Public URL web service | B.1 step 4 |
| (a) IaC | B.1 step 1 |
| (b) serverless function | B.3 steps 4–5 |
| (b) REST, gRPC, message queues | B.1 step 4 (REST), B.2 step 1 (gRPC calls), B.3 step 4 (Pub/Sub), B.4 step 2 (Kafka) |
| (c) GKE + 2 HPAs + load test | B.2 step 4 |
| (d) GitOps | B.1 step 2 |
| (e) Flink, Kafka in, windows, results topic | B.4 steps 2–5 |
| (f) ZooKeeper concept | B.2 step 1 + B.3 steps 2–3 + B.4 step 1 |
| (g) three storage products | B.1 step 5, B.4 step 6 |
| (h) Prometheus/Grafana + central logs | B.2 step 6, B.4 step 5 |

**Reset:** use a new pool ID; old synthetic rows are harmless. Stop a load test with
`kubectl -n hospital-system delete job hpa-load-test`.

---

# Part C — Viva preparation

## C.1 Everyone

- **What does the system guarantee?** No double booking (lock + fencing), fair waiting (urgency then arrival), safe
  retries (idempotency keys and operation IDs), no lost events (transactional outbox).
- **Why ZooKeeper?** Ordered ephemeral sequential znodes give a fair queue and automatic release when a holder's
  session dies; ZAB keeps a consistent view across a 3-node quorum.
- **What if a lease expires mid-operation?** The next holder gets a higher fencing token; Bed Registry, Staff
  Scheduler and Triage reject the stale holder's lower token.
- **Why an outbox?** State and events commit together; brokers never block a lock or a transaction; delivery is
  at-least-once, so consumers deduplicate by `event_id`.
- **Why GitOps?** The cluster equals Git; rollback is a Git revert; images are digest-pinned and gated in CI.
- **Why these storage products?** Relational integrity on Cloud SQL, high-write documents on Firestore, binary
  objects on Cloud Storage.

## C.2 Ruchir — (a) IaC, (d) GitOps, Patient Registry, Staff Scheduler

- **Is everything really Terraform?** Yes: network, GKE, Cloud SQL, Firestore and its indexes, buckets, Pub/Sub,
  Eventarc, the function, IAM and Workload Identity, Dataproc and the whole Confluent side; Argo CD itself through
  `bootstrap_k8s`; the state bucket through `bootstrap`.
- **How are secrets handled?** Write-only password attributes (never stored in state), Secret Manager versions
  mounted with the CSI driver, Workload Identity instead of keys, `TF_VAR_*` only in memory.
- **How does a change reach the cluster?** Pull request → CI (tests, release gate) → merge → CI builds images, pins
  digests, commits → Argo CD syncs. Nobody runs `kubectl apply` for a service.
- **Why the app of apps?** One registration discovers every service's Application; projects restrict them to this
  repository and namespace; auto-sync for stateless services, manual sync where migrations or load tests run.
- **Patient Registry:** Cloud SQL `patients`, documents in GCS with generated object names (no filenames or bytes in
  logs), Alembic migrations with a migrator identity, admissions consumed by Allocation.
- **Staff Scheduler:** Firestore roster; a nurse is assignable only when active, on shift, eligible for the pool and
  under the concurrency limit; assignment is one Firestore transaction with operation dedup and pool fencing.

## C.3 Pratham — (c) GKE/HPA/load test, (h) observability, Bed Allocation, Triage

- **Why HPAs on Allocation and Triage?** They are the stateless, CPU-bound hot path; 60 % CPU target, 2–10 and 2–6
  replicas. The lock, not the replica count, guarantees correctness, so scaling out is safe.
- **What did the load test show?** With 40 virtual users Bed Allocation scaled 2 → 5 and Triage 2 → 4 within a few
  minutes, then back down; all 25,914 checks passed at about 36 requests/s (median 24 ms). p95 was 4.9 s, above
  the 3 s target, because writes for one pool queue on that pool's lock and the test used only two pools — the lock
  trades latency for correctness. Recorded in `11-hpa-load-test.txt`.
- **How does Allocation stay correct with many replicas?** Per-pool lock with fencing; durable idempotency claims;
  each remote step persisted with a stable operation ID; `RECOVERING` instead of guessing; the recovery worker on
  any replica reconciles.
- **Why can waitlisting return 201?** It is a successful business outcome, not an error.
- **Waitlist order?** `triage_level` ascending (1 most urgent), then `queued_at`, then `entry_id`; pool changes while
  waitlisted are rejected; promotion happens under the same pool lock as allocation.
- **Observability?** Prometheus metrics from every service via Managed Prometheus, one Grafana dashboard (RPS,
  errors, latency, outcomes, lock waits, outbox backlog, HPA, CPU, restarts), alert rules, structured JSON logs with
  request IDs in Cloud Logging.

## C.4 Nisarg — (f) ZooKeeper lock, (b) serverless Notification

- **How is the lock built?** Hand-written on raw znodes (kazoo only for the connection): each acquire creates an
  ephemeral sequential znode under `/locks/<pool>`; the lowest holds the lock; each waiter watches only the znode
  directly ahead (no herd effect); after a watch fires the queue is re-read.
- **Fencing token = `czxid`?** ZooKeeper assigns it, it only increases, and a lower queue position has a lower
  `czxid`. It is (epoch × 2³²) + counter, so it jumps after a leader change (4294967302 → 8589934595 → …); it still
  increases, which is all fencing needs.
- **Why leases if znodes are ephemeral?** Ephemeral covers the coordinator dying; leases cover a frozen *caller*
  while the coordinator is healthy. The lease starts at grant; a reaper deletes expired locks every 0.5 s.
- **Reaper vs renew race?** The reaper deletes with the version it read; a renew in between bumps the version, so
  ZooKeeper refuses the delete (`BadVersionError`).
- **Two replicas?** All state is in ZooKeeper; `lock_id` contains the pool, so any replica serves Renew/Release.
  A retried Acquire on two replicas creates one claim znode in the same multi-op transaction, so both get the same
  lease.
- **Release retries?** Release deletes the lock and writes a tombstone in one transaction; a retry within 10 minutes
  returns `true`. `false` means "not confirmed", not "failed"; fencing is the real guarantee.
- **ZooKeeper failures?** 3 nodes, quorum 2. One down: a new leader in seconds, locks survive. Two down: writes are
  refused (no split brain), readiness 503, Acquire 504. Data is on PVCs; a StatefulSet gives stable identity and
  DNS; a PDB keeps two nodes up during maintenance.
- **Why a Cloud Run function?** The brief's serverless function: deployed from source with an entry point,
  Eventarc trigger, scales 0–3, no image to manage.
- **Duplicates?** Firestore transaction on `event_id` across all instances. **Gaps?** Highest `stream_sequence` per
  pool; jumps logged as `notification_gap`; late lower sequences still delivered.
- **Failures?** Malformed → acknowledged (`notification_rejected`); Firestore errors → raise → retried with 10–600 s
  backoff → `bed-notifications-dlq` after 10 attempts (policy on the Eventarc subscription, imported into Terraform).
- **Known limitations:** a caller that gives up during a ZooKeeper outage may still be granted the lock when quorum
  returns, blocking the pool until the lease expires; `Validate` is not atomic with downstream commits (fencing
  covers it).

## C.5 Jay — (e) Flink and Kafka, (g) storage, Bed Registry, Resource Analytics

- **Why fencing in Bed Registry if there is a lock?** The stale-client problem: a holder paused past its lease
  still writes; Bed Registry stores the highest token per pool and rejects lower ones (409 / `FAILED_PRECONDITION`).
- **Out-of-order events?** Event-time processing: the window comes from the event's own time, with a 5-second
  watermark, so a late event still lands in its window.
- **The query?** `TUMBLE(TABLE bed_events, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)` grouped by `pool_id` and
  window; `requests` = distinct allocations that were allocated or waitlisted, `allocated`, `waitlisted`,
  `released`, `conflicts` = allocations whose lock was contended, `contention_rate`, average lock and allocation
  wait; written to `bed-stats` keyed by `pool_id`.
- **Why `COUNT(DISTINCT …)`?** Delivery is at-least-once; a redelivered `event_id` is counted once and the query
  stays an append-only window aggregation, which the Kafka sink requires.
- **Why read `event_time` as a string?** The producer writes ISO-8601 with microseconds and an offset, which Flink's
  JSON timestamp parser rejects; it is converted to `event_ts` in SQL.
- **Exactly-once?** Checkpoints every 60 s in GCS; Kafka offsets are part of the checkpoint.
- **Credentials?** Read from Secret Manager into memory at submit time and sent over IAP SSH; never in the template,
  GCS or disk.
- **Why Confluent?** Managed Kafka allowed by the brief; keeps GKE capacity for the services; per-component keys with
  least-privilege role bindings, all in Terraform.
- **Storage choices (g):** see C.1; Bed Registry on Cloud SQL (ACID inventory and fencing state), analytics windows
  on Firestore (`pool_stats/{pool}_{window_start}`), checkpoints and documents on Cloud Storage.

---

# Part D — Troubleshooting (lessons from the deployment)

1. **`password authentication failed for user …`** (migration Job fails, `/readyz` 503): the secret and Cloud SQL
   disagree. Rotate in one session: generate every password, bump every `*_password_version`, apply, then add the
   hand-written URL secret versions (`cloudsql-db-password`, `patient-registry-*`, `allocation-*`) from the same
   variables. Never disable the Terraform-managed versions (`bed-registry-*`, `kafka-*`).
2. **Allocations stuck `RECOVERING` at `CHECK_PRIORITY`**, Triage logs "Firestore is unavailable": a Firestore
   composite index is missing. Indexes are in `infra/terraform/modules/firestore`; the emulator never needs them.
3. **Migration Job waits for `bed-allocation-sa`:** sync Allocation + Triage first (A.3).
4. **Plan replaces Dataproc:** the module must list all four default scopes and set worker machine type/disk only when
   `worker_count > 0`.
5. **Confluent 401 during plan:** the provider needs a Cloud API key, even when nothing in Confluent changes.
6. **Eventarc subscription "already exists":** apply once with `eventarc_subscription_id = ""`, then with the ID.
7. **A pod reads `X_PORT=tcp://…`:** Kubernetes service links; the demo console sets `enableServiceLinks: false`.
8. **CI "Address already in use":** the first CI step moves the ephemeral port range above the sandbox ports.
9. **App OutOfSync after a merge:** CI promoted new images; sync it. An old generated ConfigMap stays OutOfSync until
   a sync with prune.
10. **400 `Idempotency-Key: Field required`:** every POST needs the header.
11. **Allocation migrations app OutOfSync after a merge:** the migration Job is an Argo CD Sync hook
    (`BeforeHookCreation`), so syncing `hospital-allocation-migrations-staging` deletes the old Job and runs a fresh
    one; at the current schema `alembic upgrade head` does nothing. (Before this, a promoted image digest could not
    be patched into the completed Job, because a Job's pod template is immutable.)
