# Completion guide: deployed system, remaining work, and lessons

Status on **2026-11-16** (master `14face5`, PRs #18–#25 merged). Everything below the "Remaining" heading is still to
do; everything else is deployed and was verified in the cloud. Evidence: `docs/evidence/2026-11-16-gke/`.
Current inventory and endpoints: `docs/deployment-status-2026-11-16.md`.

## 1. Requirement status (brief CS/SS G527, scenario S7)

| Req | What the brief needs | Status | Evidence |
|---|---|---|---|
| ≥8 services | 8 microservices | ✅ All 8 running: 7 on GKE through Argo CD, Notification as a Cloud Run function | `01-argocd-applications.txt`, `02-cluster.txt` |
| Public URL | a web service by public URL | ✅ Patient Registry at `http://35.244.157.132/docs` (REST API, `/docs`, `/healthz`) | `03-public-url.txt` |
| a | all infra via IaC | ✅ Terraform (GCS backend `terraform/state`): VPC, GKE, Cloud SQL, Firestore + indexes, GCS, Pub/Sub, Eventarc, function, IAM, Dataproc, Confluent | `terraform state list` (Ruchir) |
| b | serverless async function | ✅ `hospital-notification-fn` (Cloud Run functions, Eventarc on Pub/Sub, DLQ after 10 attempts) | `08-notification-function.txt` |
| b | REST + gRPC + MQ | ✅ REST (clients, Patient Registry), gRPC (Allocation → Triage, Lock, Bed, Staff), Pub/Sub and Kafka | `05`–`09` |
| c | GKE stateless + 2 HPAs + load test | ✅ HPAs `bed-allocation-hpa` (2–10) and `triage-hpa` (2–6). **Load test: remaining (step R4)** | `02-cluster.txt` |
| d | GitOps only | ✅ Argo CD app of apps tracks `master`; CI builds, digest-pins and commits images; no `kubectl apply` of services | `01-argocd-applications.txt` |
| e | Flink on Dataproc, Kafka in → window → results topic | ✅ Confluent `bed-events`/`bed-stats`, Allocation publishes, Analytics subscribed. **Flink job: remaining (step R2)** | `09-kafka-outbox.txt` |
| f | ZooKeeper concept implemented + demoed | ✅ Concurrent allocations queue on the pool lock with increasing fencing tokens; no double booking | `04-demo-flow.txt`, `07-lock-logs.txt` |
| g | 3 storage products, real use | ✅ Cloud SQL (patients, beds, allocations), Firestore (triage, staff, notifications, analytics), GCS (patient documents uploaded through the public API; Flink checkpoints after R2) | `03`, `04`, `10` |
| h | Prometheus/Grafana dashboards, central logs incl. analytics | ✅ Managed Prometheus + Grafana in cluster; Cloud Logging for pods, the function and Dataproc. **Dashboard screenshots: step R5** | Grafana (port-forward) |
| Deliverables | design doc, diagrams, code, IaC, manifests, `task_divison.txt` | Design doc + as-built supplement; diagrams to export (step R6) | `docs/design-as-built.md` |

## 2. What was done (deployment record)

| Step | Who | Result |
|---|---|---|
| Release merged (PR #20) | Pratham | Pub/Sub outbox, Bed Registry DB, monitoring, demo UI, k6 Job, CI image publishing, Patient Registry public URL, leaked `temp_*.txt` removed |
| Notification as a function (PR #19) + cut-over | Nisarg, Ruchir | Function live; the Eventarc subscription imported into Terraform with the dead-letter policy |
| Lock logging (#21), member C docs (#23) | Nisarg | Grants, timeouts and releases logged with fencing tokens |
| Terraform applies, password rotation, state moved to GCS | Ruchir | Single-node Dataproc, node cap 2, GKE monitoring packages, Confluent cluster and topics, Firestore indexes |
| CI fixes (#24 and in #22) | Pratham | Ephemeral port range moved above the sandbox ports as the first CI step |
| CI image publishing | repo admin | `GCP_WIF_PROVIDER` / `GCP_WIF_SERVICE_ACCOUNT` set; each master merge promotes images |
| Argo CD deployment | Pratham | Root app registered; all apps Synced/Healthy on 2 nodes |
| Demo UI fix (#25) | Pratham | `enableServiceLinks: false` |
| Kafka switched on (#22) | Pratham | Allocation `OUTBOX_DESTINATIONS=pubsub,kafka`; outbox backlog 0; Analytics subscribed to `bed-stats` |
| Least privilege | Ruchir | `bed_registry`, `allocation_app`, `patient_registry` no longer in `cloudsqlsuperuser`; demo flow re-run afterwards |
| Cleanup | Ruchir | Old `confluent-api-*` versions disabled; `default` network and its open SSH/RDP rules deleted |

## 3. Remaining (in order)

**R1. Nisarg — function checks.** Real events are already logged (`08-notification-function.txt`). Run the synthetic
handled / duplicate / malformed checks on pool `NISARG-TEST` (`docs/proposals/serverless-function-handoff.md`) and
show the dead-letter configuration. Screenshots into `docs/evidence/`.

**R2. Jay — Flink job.** Follow `docs/kafka-enable/CHECKLIST.md` steps 6 and 8: Dataproc image 2.1 ships Flink 1.15,
so stage `flink-sql-connector-kafka-1.15.4.jar` (SHA-1 checked) in `gs://hospital-flink-514618/jars/`, then
```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-41p56.asia-south1.gcp.confluent.cloud:9092 \
FLINK_KAFKA_CONNECTOR_JAR=gs://hospital-flink-514618/jars/flink-sql-connector-kafka-1.15.4.jar \
bash services/resource_analytics/submit_flink_job.sh
```
Verify the checklist's table (job RUNNING, checkpoints, `bed-stats` records, Firestore `pool_stats`, Dataproc logs).

**R3. Ruchir — evidence.** `terraform state list`, the Argo CD UI, a Staff Scheduler nurse in Firestore, the public
`/docs` flow, the Cloud SQL rows.

**R4. Pratham — HPA load test (after R2, so analytics also gets traffic).** Sync `hospital-load-test` (in-cluster k6
Job), then watch:
```bash
kubectl -n hospital-system get hpa -w
```
```bash
kubectl get pods -A --field-selector=status.phase=Pending
```
At both HPA maxima two e2-standard-4 nodes are about 94 % *requested* CPU. If pods stay Pending, stop the test
(`kubectl -n hospital-system delete job hpa-load-test`) and report; do not add a third node without approval.

**R5. Pratham — observability evidence.** Grafana (`kubectl -n hospital-system port-forward svc/grafana 3000:3000`):
RPS, error rate, p95 latency, HPA current vs desired, cluster health; Cloud Logging queries for every service and
the Flink job.

**R6. Everyone — deliverables.** Export the Mermaid diagrams in `docs/design-as-built.md` (mermaid.live → PNG); add
"Differences from the plan" (§4 of that file) to `cloud_proj_plan.docx`; confirm names and IDs in `task_divison.txt`;
prepare viva answers for your two services; submit on Nalanda (one per group, GitHub link).

**R7. After grading (Ruchir, Jay; approval).** Stop Dataproc; delete the Confluent cluster; remove the public Ingress
through Git (Argo CD prunes the load balancer); scale the node pool to 0; set Cloud SQL activation policy NEVER.
Never delete databases, buckets or Terraform state.

## 4. Lessons from the deployment (read before changing anything)

1. **Write-only passwords and their secrets must change together.** A `google_sql_user` with `password_wo` sends the
   password only when `*_password_version` changes. Rotate in **one PowerShell session**: generate every password,
   bump every version, plan and apply, then add the hand-written URL secret versions (`cloudsql-db-password`,
   `patient-registry-*`, `allocation-*`) from the same variables. A user that already existed in state keeps its old
   password until its version is bumped. Symptom of a mismatch: `password authentication failed for user …` in a
   migration Job or `/readyz` 503.
2. **Do not disable Terraform-managed secret versions** (`bed-registry-database-url`,
   `bed-registry-migration-database-url`, `kafka-*`). Terraform destroys the previous version itself; disabling the new
   one leaves the service with no readable version. Disable old versions only of the hand-written secrets.
3. **Argo CD sync order.** `hospital-allocation-triage-staging` creates `bed-allocation-sa`, which the migration Job
   uses; sync it **before** `hospital-allocation-migrations-staging` (the Allocation pods become ready once the
   migration finishes). Full order: Bed Registry → Allocation + Triage → allocation migrations → monitoring →
   Resource Analytics → demo UI. Patient Registry, Staff Scheduler and Lock Coordinator auto-sync.
4. **Firestore composite indexes live in Terraform** (`modules/firestore`: `waitlist_entries` and `pool_stats`). The
   emulator used in CI needs none, so a missing index appears only in the cloud: Triage then answers "Firestore is
   unavailable" and allocations stay `RECOVERING` at `CHECK_PRIORITY`. Any new ordered Firestore query needs an index
   added there.
5. **Dataproc with 0 workers.** Declare worker machine type and disk only when `worker_count > 0`, and list all four
   service-account scopes Dataproc adds; otherwise every plan replaces the cluster.
6. **Confluent provider needs a Cloud API key** (Cloud resource management, owner OrganizationAdmin), even for plans
   that change nothing in Confluent; a cluster key fails with 401 during refresh. Set it only as `TF_VAR_*`.
7. **Eventarc subscription import.** The first apply with `eventarc_subscription_id = ""` removes the old managed
   subscription; the next one with the generated ID imports it. Retry policy and push config stay Eventarc's
   (`ignore_changes`).
8. **Kubernetes service links.** A Service named `X` injects `X_PORT=tcp://…`; the demo UI therefore runs with
   `enableServiceLinks: false`.
9. **CI port collisions.** The sandbox's fixed ports (42181–49092) are inside Linux's default ephemeral range; the
   first CI step moves the range to 50000–65000. Reserving a block inside the range instead makes `bind(0)` return
   the same port repeatedly.
10. **Every master merge re-promotes images.** Apps show OutOfSync until synced; a changed generated ConfigMap leaves
    the old one OutOfSync until it is pruned (sync with prune).
11. **Every POST needs an `Idempotency-Key` header** (any unique string); repeating it returns the same result.

## 5. Security status

| Item | Status |
|---|---|
| S1 Leaked Confluent key | ✅ New Cloud key in use; old `confluent-api-*` versions disabled. Confirm in the Confluent console that the old key is deleted |
| S2 Cloud SQL passwords | ✅ Rotated (all users, one session); old secret versions disabled |
| S3 Ruchir's laptop | Ruchir: no `temp_*.txt` copies, no passwords in `staging.tfvars`, no `tfplan*` files, cleared `TF_VAR_*` |
| S4 Repository | ✅ `temp_*.txt` removed; values remain in the history of `42fb8e6`, neutralised by S1–S2 |
| S5 Terraform state | ✅ One GCS backend (`terraform/state/default.tfstate`); the stale root `default.tfstate` can be archived |
| S6 Network | ✅ `default` network deleted. Open: replace the shared WARP addresses in `master_authorized_networks`; remove the untagged `hospital-vpc-allow-iap-ssh` rule (the tagged Dataproc rule covers IAP SSH) |
| S7 Database least privilege | ✅ Runtime roles out of `cloudsqlsuperuser`; demo flow verified afterwards |
| S8 Capacity | Watch during R4 |
| Known drift | `hospital-notification` service account display name is set by two modules and flips on every apply (harmless) |

## 6. Cost guard

Running now: 2 GKE nodes, Cloud SQL, NAT, the HTTP load balancer, single-node Dataproc and a Confluent **Standard**
cluster (hourly base charge on the $400 trial credit). Check *Billing → Credits* daily, stop Dataproc whenever Flink is
not being shown, and do R7 right after grading.
