# Deployment status — hospital-management on GCP — 2026-11-16

Project `hospital-management-514618` · region `asia-south1` · cluster `hospital-gke` (zonal, `asia-south1-a`, 2 ×
e2-standard-4, private nodes, DNS endpoint) · namespace `hospital-system`. **Snapshot of 2026-11-16 ~15:45 UTC**,
master `14face5` (Kafka enabled). Evidence files: `docs/evidence/2026-11-16-gke/`. Remaining work and lessons:
`docs/COMPLETION-GUIDE.md`. The 2026-11-15 snapshot is historical.

## 1. Endpoints

| What | Where | Access |
|---|---|---|
| Public web service (Patient Registry) | `http://35.244.157.132/docs` (`/api/v1/*`, `/openapi.json`, `/healthz`) | Public, plain HTTP, synthetic data; POSTs need `Idempotency-Key` |
| Demo console | `kubectl -n hospital-system port-forward svc/demo-ui 8080:80` → `http://localhost:8080` | Private |
| Grafana | `kubectl -n hospital-system port-forward svc/grafana 3000:3000` | Private |
| Argo CD | `kubectl -n argocd port-forward svc/argocd-server 8443:443` | Private |
| Kafka | Confluent Cloud `pkc-41p56.asia-south1.gcp.confluent.cloud:9092`, topics `bed-events`, `bed-stats` | SASL_SSL, per-component keys in Secret Manager |
| Flink | Dataproc `hospital-dataproc-flink` (single node, image 2.1 / Flink 1.15), master in `asia-south1-c` | IAP SSH only |

## 2. Services (all Argo CD Applications Synced/Healthy; manual sync unless noted)

| Service | Owner | Argo CD Application (path) | Replicas | Image | Storage | Status |
|---|---|---|---|---|---|---|
| Patient Registry | Ruchir | `patient-registry` (auto) | 2 | `patient-registry@sha256:979fd976…` | Cloud SQL `patients`, GCS | CLOUD-VERIFIED (public API, upload to GCS) |
| Staff Scheduler | Ruchir | `staff-scheduler` (auto) | 2 | `staff-scheduler@sha256:5a089bf3…` | Firestore | CLOUD-VERIFIED (nurse + shift, staff assignment) |
| Lock Coordinator + ZooKeeper ×3 | Nisarg | `lock-coordinator` (auto) | 2 + 3 | `lock-coordinator@sha256:94c022b3…`, `zookeeper:3.9` | ZooKeeper PVCs | CLOUD-VERIFIED (queued grants, fencing tokens) |
| Notification | Nisarg | — (Terraform: Cloud Run function `hospital-notification-fn`) | 0–3 | built from source | Firestore | CLOUD-VERIFIED (real events; synthetic checks pending) |
| Bed Registry | Jay | `hospital-bed-registry-staging` | 2 | `bed-registry@sha256:c2588856…` | Cloud SQL `beds` | CLOUD-VERIFIED |
| Resource Analytics | Jay | `hospital-resource-analytics-staging` | 1 | `resource-analytics@sha256:a3c1ac27…` | Firestore `pool_stats` | Subscribed to `bed-stats`; waits for the Flink job |
| Bed Allocation | Pratham | `hospital-allocation-triage-staging` + migrations | 2 (HPA 2–10) | `bed-allocation@sha256:59a48223…` | Cloud SQL `allocations` | CLOUD-VERIFIED (concurrency, waitlist, promotion, outbox to Pub/Sub + Kafka) |
| Triage | Pratham | `hospital-allocation-triage-staging` | 2 (HPA 2–6) | `triage@sha256:3b9d8c1e…` | Firestore | CLOUD-VERIFIED |
| Flink job `hospital-bed-stats-1m` | Jay | — (submitted to Dataproc) | — | SQL + connector jar | GCS checkpoints | PENDING (Jay, step R2) |
| Demo UI | Pratham | `hospital-demo-ui` | 1 | `demo-ui@sha256:b54a2b16…` | — | CLOUD-VERIFIED (private) |
| Monitoring | Pratham | `hospital-monitoring` | Grafana + GMP frontend | `grafana/grafana:11.6.0` | Managed Prometheus | Running; dashboards to capture (R5) |
| Load test | Pratham | `hospital-load-test` | k6 Job | — | — | Not run yet (R4); app intentionally OutOfSync |

Images are built by CI from master and digest-pinned in Git by the `chore(gitops): promote images` commits.

## 3. Verified flow (2026-11-16, pool `DEMO-C91B24`)

Three concurrent allocations for two beds: 2 ALLOCATED (different beds), 1 WAITLISTED; an idempotent replay
returned the same allocation; releasing a bed promoted the waitlisted admission onto it. The Lock Coordinator
granted the pool lock in order with increasing fencing tokens (waits 18, 401, 727 ms). The Notification function
logged five `staff_notification` events; the outbox backlog is 0 for Pub/Sub and Kafka. Files `04`–`09`.

## 4. Infrastructure (Terraform, state `gs://hospital-management-514618-tf-state/terraform/state`)

VPC `hospital-vpc` with Cloud NAT and private services access; GKE with Managed Prometheus packages (SYSTEM,
HPA, POD, DEPLOYMENT, cAdvisor) and node pool min/max 2; Cloud SQL `hospital-postgres` (PostgreSQL 16, private IP,
`db-custom-2-7680`, databases `patients`, `beds`, `allocations`); Firestore Native with two composite indexes;
buckets `hospital-patient-docs-514618`, `hospital-flink-514618`; Pub/Sub `bed-notifications` + DLQ; Eventarc and the
notification function; Secret Manager; Artifact Registry; Dataproc; Confluent environment, Standard cluster, topics,
service accounts and keys; Workload Identity for every service and GitHub Actions (WIF). CPU quota: 10 of 12 vCPUs
with Dataproc running.

## 5. Open items

Flink job (Jay), function synthetic checks (Nisarg), HPA load test and dashboards (Pratham), evidence (everyone),
design-document differences and diagram export, submission; see `docs/COMPLETION-GUIDE.md` §3.

## 6. Cost and shutdown

Billable now: 2 GKE nodes, Cloud SQL, NAT, the HTTP load balancer, Dataproc (stop when not showing Flink) and the
Confluent Standard cluster (trial credit). After grading: `docs/COMPLETION-GUIDE.md` step R7.
