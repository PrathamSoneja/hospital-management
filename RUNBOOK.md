# Runbook: running and demonstrating the project

How to run the whole system (Part A), the demo script covering every requirement of the brief (Part B) and
troubleshooting (Part C). What the system is and how it works: `PROJECT.md`.

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
`infra/terraform` in **one PowerShell session** (see Part C, item 1, for why):

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

# Part C — Troubleshooting (lessons from the deployment)

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
