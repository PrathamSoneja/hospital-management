# GCP Integration Runbook (Bed Allocation, Triage, HPA, Observability)

Status: **UNEXECUTED.** Nothing in this runbook has been run against GCP. Every command that touches
the cloud needs explicit approval of its target and action and is run by the named owner. Values in
`<ANGLE_BRACKETS>` / `PLACEHOLDER_*` are deliberately unset; do not invent them.

Shell: Windows PowerShell from the repository root unless a step says otherwise. Authentication:
`gcloud auth login` + `gcloud auth application-default login` (ADC) for operators; Workload Identity
for pods. Never export access tokens, never create service-account key files, never set
`GOOGLE_APPLICATION_CREDENTIALS`.

Platform (fixed): GKE Standard, Artifact Registry, Cloud SQL PostgreSQL, Firestore Native, GCS,
Pub/Sub → Eventarc → Cloud Run service, Secret Manager, Cloud Logging, Dataproc/Flink, Kafka and
ZooKeeper self-hosted on GKE. Terraform provisions infrastructure; ArgoCD deploys applications; no
application `kubectl apply`.

## 0. Owners and open conflicts (resolve before step 1)

| Area | Owner | State |
|---|---|---|
| Project, billing, budget, Terraform state, network, GKE, Cloud SQL, IAM, Artifact Registry, ArgoCD | Ruchir (A) | Terraform written; no state/apply verified |
| Patient Registry, Staff Scheduler | Ruchir (A) | Images built locally; undeployed |
| Lock Coordinator, ZooKeeper, Notification (Pub/Sub → Eventarc → Cloud Run) | Nisarg (C) | Undeployed; no endpoint |
| Bed Registry, Kafka, Flink/Dataproc, Resource Analytics | Jay (D) | Undeployed; no endpoint |
| Bed Allocation, Triage, HPA/k6, Prometheus/Grafana/logging | Pratham (B) | LOCAL-VERIFIED / GENERATED-UNDEPLOYED |

Conflicts found in the current Terraform (not changed by Pratham):

1. **Confluent Cloud Kafka** (`modules/confluent`, `confluent_*` variables/outputs) contradicts the
   GKE-hosted Kafka + ZooKeeper requirement. Owners: Ruchir + Jay decide; Allocation needs only an
   in-cluster bootstrap address, topic, security protocol, and SASL secret names.
2. Cloud SQL passwords use the provider's write-only password arguments and are not retained in Terraform
   state. Secret Manager values are deliberately not provisioned by Terraform.
3. Terraform creates distinct Allocation runtime and migration users. Before the first migration, a
   database owner must grant `USAGE, CREATE` on schema `public` to `allocation_migrator`; revision
   `20261031_04` grants runtime DML to `allocation_app` on current and future tables/sequences.
4. `bed-allocation` has topic-scoped Pub/Sub publisher permission and access only to its database and
   Kafka secrets. Patient Registry has access only to its database secret.
5. GKE Secret Manager CSI, Dataplane V2 NetworkPolicy enforcement, and managed Prometheus collection
   are enabled in Terraform.
6. Terraform defines the Pub/Sub topic, Eventarc trigger, private Cloud Run service, and Secret Manager
   secret containers. Applying requires the Notification image from Nisarg (C).

## 1. Project, billing, budget, IAM, state (Ruchir)

Prerequisites: project created with billing; operator has Owner or equivalent scoped roles.

```powershell
gcloud config set project <PROJECT_ID>
gcloud billing projects describe <PROJECT_ID>          # expect billingEnabled: true
gcloud billing budgets create --billing-account=<BILLING_ACCOUNT_ID> --display-name="hospital-staging" `
  --budget-amount=<BUDGET_AMOUNT> --threshold-rule=percent=0.5 --threshold-rule=percent=0.9
```

State: follow `infra/terraform/README.md` (bootstrap bucket first). Keep `backend.hcl` and
`*.tfvars` local (both ignored).

```powershell
.\scripts\cloud\Invoke-TerraformPlan.ps1 -ProjectId <PROJECT_ID> -BackendConfig backend.hcl -VarFile staging.tfvars
# review infra\terraform\tfplan.txt; record the printed sha256 in the approval
.\scripts\cloud\Invoke-TerraformApply.ps1 -ProjectId <PROJECT_ID> -ApprovedPlanSha256 <APPROVED_SHA256>
terraform -chdir=infra/terraform output              # non-secret outputs only
```

Verify: `gcloud container clusters list`, `gcloud sql instances list`, `gcloud artifacts repositories list`.
Rollback: revert the Terraform change and plan/apply again; never `terraform destroy` without separate approval.

## 2. Network and private Cloud SQL access (Ruchir)

Expected from Terraform: private VPC, Private Service Access, Cloud SQL **private IP only**, private GKE
nodes, NAT for egress. Verify:

```powershell
gcloud sql instances describe <SQL_INSTANCE> --format="value(ipAddresses)"   # PRIVATE only
gcloud container clusters describe <CLUSTER> --location <LOCATION> --format="value(privateClusterConfig)"
```

Allocation connects through the Cloud SQL Auth Proxy sidecar (`--private-ip`) using Workload Identity.

## 3. Databases, users, migrations vs runtime (Ruchir creates users; Pratham runs approved migration)

1. Terraform creates two Allocation users with write-only passwords:
   - `allocation_migrator` — owns schema objects in `allocations`.
   - `allocation_app` — `CONNECT`, `USAGE` on schema, `SELECT/INSERT/UPDATE/DELETE` on tables, `USAGE` on
     sequences; no DDL.
2. Store URLs (never in Git):
   - `allocation-migration-database-url` = `postgresql+psycopg2://allocation_migrator:<SECRET>@127.0.0.1:5432/allocations`
   - `allocation-database-url` = `postgresql+psycopg2://allocation_app:<SECRET>@127.0.0.1:5432/allocations`
3. Before the first migration only, have a database owner grant `USAGE, CREATE` on schema `public` to
   `allocation_migrator`. Then run the migration job with `allocation-migration-database-url` (after approval):
   ```powershell
   .\scripts\cloud\Start-ApprovedMigration.ps1 -ProjectId <PROJECT_ID> -KubeContext <KUBE_CONTEXT> -Revision 20261031_04
   ```
   Expected: Job completes; log ends with `20261031_04 (head)`. Legacy outbox rows are reported (not changed).
4. Verify the app can access tables/sequences but cannot run DDL.

Rollback: the migration has a tested `downgrade()`, but downgrading needs separate approval; prefer
roll-forward. Never run migrations from a laptop against the shared database.

## 4. Firestore indexes, GCS, Secret Manager, identity

```powershell
# Firestore composite indexes for Triage (Pratham, after approval)
gcloud firestore indexes composite create --project <PROJECT_ID> --database "(default)" `
  --collection-group=waitlist_entries --query-scope=COLLECTION `
  --field-config=field-path=pool_id,order=ascending --field-config=field-path=triage_level,order=ascending `
  --field-config=field-path=queued_at,order=ascending --field-config=field-path=entry_id,order=ascending
# (source of truth: services/triage/firestore.indexes.json)
gcloud firestore indexes composite list --project <PROJECT_ID>

# Secret Manager (Ruchir creates; values entered interactively, never on the command line history)
gcloud secrets create allocation-database-url --replication-policy=automatic
gcloud secrets create allocation-migration-database-url --replication-policy=automatic
gcloud secrets create kafka-allocation-sasl-username --replication-policy=automatic
gcloud secrets create kafka-allocation-sasl-password --replication-policy=automatic
# add versions with: gcloud secrets versions add <name> --data-file=-   (paste, then Ctrl+Z Enter)

# Least-privilege access for the Allocation GSA (Terraform preferred)
gcloud secrets add-iam-policy-binding allocation-database-url `
  --member=serviceAccount:<NAME_PREFIX>-bed-allocation@<PROJECT_ID>.iam.gserviceaccount.com `
  --role=roles/secretmanager.secretAccessor
```

GCS: Allocation/Triage use no buckets. Identity: KSAs `bed-allocation-sa` and `triage-sa` in
`<K8S_NAMESPACE>` map to `<NAME_PREFIX>-bed-allocation` / `<NAME_PREFIX>-triage` (Terraform `iam` module).

## 5. Pub/Sub → Eventarc → Cloud Run service (Nisarg)

Required for Allocation: topic `<PUBSUB_TOPIC>`; `roles/pubsub.publisher` for the Allocation GSA; the
function must accept the notification schema in `contracts/events/notification.schema.json`, including
`stream_sequence` and nullable `ward` (**contract acceptance pending**), dedupe on `event_id`, and route by
`pool_id`. Verify with a staging event (section 10). Rollback: disable the trigger; Allocation keeps
retrying with backoff and the backlog stays visible (`outbox_pending_events`).

## 6. Kafka and ZooKeeper on GKE (Jay; Nisarg for ZooKeeper used by the Coordinator)

Needed from the owner: in-cluster bootstrap `host:port`, topic `bed-events` (key = `pool_id`, partitions,
replication, retention), security protocol (`SASL_SSL`/`SSL`), SASL mechanism, CA bundle mount path if
TLS, and a dedicated Allocation principal with produce-only ACL on the topic. Consumers dedupe on
`event_id`, detect gaps with `stream_sequence`, and must not drop lower sequences automatically.

## 7. GKE add-ons and namespace (Ruchir)

Enable: Secret Manager CSI add-on, network-policy enforcement (Dataplane V2), metrics-server (default),
optionally Managed Prometheus. Create `<K8S_NAMESPACE>` and a `monitoring` namespace if used.

## 8. Registry, images, GitOps (Pratham; ArgoCD install by Ruchir)

```powershell
.\scripts\cloud\Push-Images.ps1 -ProjectId <PROJECT_ID> -Region <REGION> -Repository <AR_REPOSITORY>
.\scripts\gitops\Set-ImageDigest.ps1 -Image bed-allocation -Reference <REGION>-docker.pkg.dev/<PROJECT_ID>/<AR_REPOSITORY>/bed-allocation@sha256:<DIGEST>
.\scripts\gitops\Set-ImageDigest.ps1 -Image triage -Reference <REGION>-docker.pkg.dev/<PROJECT_ID>/<AR_REPOSITORY>/triage@sha256:<DIGEST>
# replace remaining PLACEHOLDER_* values in k8s/overlays/staging and k8s/jobs/allocation-migrate
kubectl kustomize k8s/overlays/staging | Select-String PLACEHOLDER    # expect no output
git add k8s/overlays/staging k8s/jobs/allocation-migrate ; git commit -m "deploy(staging): pin images <SHA>"
```

Register `k8s/argocd/pratham-applications.yaml` through the platform owner's ArgoCD project (it sets
`ignoreDifferences` on `/spec/replicas` + `RespectIgnoreDifferences=true` so the HPA owns replicas).
Sync manually after review: `argocd app diff hospital-allocation-triage-staging`, then
`argocd app sync hospital-allocation-triage-staging`. Rollback: `argocd app rollback` to the previous
revision or revert the digest commit.

## 9. Real teammate endpoints

Before wiring an endpoint, run the dependency contract checks against it (read-only unless
`ACCEPTANCE_ALLOW_MUTATION=1` with designated test resources): see `docs/precloud-status.md`,
"Replacing stand-ins". Before any ArgoCD sync, `python scripts/check_release_config.py` must exit 0.

Fill `k8s/overlays/staging/allocation.properties` and `triage.properties` with the owners' in-cluster
addresses (`LOCK_SERVICE_ADDR`, `BED_SERVICE_ADDR`, `STAFF_SERVICE_ADDR`, `PATIENT_SERVICE_URL`,
`LOCK_COORDINATOR_ADDR`). The services refuse to start in `staging`/`production` with empty, loopback,
emulator, or fake settings. Contract sources: `contracts/proto/*.proto`,
`contracts/openapi/patient-registry-admissions.openapi.yaml`.

## 10. Staging event and recovery tests (Pratham, with owners present)

1. Create one allocation for a test admission → expect `BED_ALLOCATED` on Kafka (key `pool_id`) and
   Pub/Sub with matching `event_id`, `stream_sequence` = previous + 1 per pool.
2. Waitlist case (no bed) → `ALLOCATION_WAITLISTED` with `ward: null` on Pub/Sub.
3. Release → `BED_RELEASED` (−1) and promotion `BED_ALLOCATED` (+1).
4. Kill one Allocation pod mid-request → recovery worker completes or the row reaches `RECOVERING` with a
   durable pending action; no duplicate bed ownership.
5. Block Kafka temporarily (owner) → outbox backlog grows, Pub/Sub stream continues, Kafka resumes in order.

## 11. Prometheus/Grafana, Cloud Logging, HPA, load (Pratham)

```powershell
.\scripts\cloud\Invoke-PostDeployChecks.ps1 -ProjectId <PROJECT_ID> -KubeContext <KUBE_CONTEXT> -Namespace <K8S_NAMESPACE>
```

Expected evidence (written to the ignored `evidence\`): rollouts complete, digests (not tags) in
`images.txt`, `/healthz` 200, `/readyz` 200, missing allocation 404, Cloud Logging entries matching
the request ID, `outbox_pending_events` near 0. HPA test: run `k6/sustained-multi-pool.js` from an
in-cluster or private VM against the ClusterIP Services with `-WatchHpa`; expect Allocation to scale
within 2–10 and Triage within 2–6 at ~60% CPU, and ArgoCD to stay Synced while replicas change.
Correctness under contention: `k6/contention.js` against a **fresh** test pool with exactly 10 free beds.
Dashboards: backlog panels/alerts use `max()` because each replica reports the same table-wide count.

## 12. Evidence and cost

Record per step: command, timestamp, operator, approval reference, and output file. Cost drivers:
GKE nodes, Cloud SQL tier, Dataproc, Cloud Run invocations, logging volume. Stop load tests promptly;
scale node pool/Dataproc down or tear down only with approval.

## Variable sources

| Variable | Source | Consumer | Secret? |
|---|---|---|---|
| `GCP_PROJECT_ID`, `PUBSUB_PROJECT_ID`, `FIRESTORE_PROJECT_ID` | Terraform `project_id` | Allocation, Triage | No |
| `GCP_REGION` / image registry host | Terraform `region`, output `artifact_registry_repository` | Images, overlay | No |
| `PLACEHOLDER_K8S_NAMESPACE` | Terraform `kubernetes_namespace` | Overlay, ArgoCD | No |
| `PLACEHOLDER_NAME_PREFIX` | Terraform `name_prefix` | KSA annotations | No |
| `CLOUD_SQL_INSTANCE_CONNECTION_NAME` | Output `cloudsql_connection_name` | Proxy sidecar, migration Job | No |
| `DATABASE_URL` (runtime user) | Secret Manager `allocation-database-url` | Allocation (`DATABASE_URL_FILE`) | **Yes** |
| Migration DB URL | Secret Manager `allocation-migration-database-url` | Migration Job only | **Yes** |
| `LOCK_SERVICE_ADDR`, `LOCK_COORDINATOR_ADDR` | Nisarg | Allocation, Triage | No |
| `BED_SERVICE_ADDR` | Jay | Allocation | No |
| `STAFF_SERVICE_ADDR`, `PATIENT_SERVICE_URL` | Ruchir | Allocation | No |
| `PUBSUB_TOPIC_NOTIFICATIONS` | Nisarg / Terraform | Allocation outbox | No |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_BED_EVENTS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SSL_CA_LOCATION` | Jay | Allocation outbox | No |
| `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | Secret Manager `kafka-allocation-sasl-*` | Allocation (`*_FILE`) | **Yes** |
| `RPC_TIMEOUT_MS`, `LOCK_WAIT_TIMEOUT_MS`, `LOCK_LEASE_MS`, `OUTBOX_*` | Repository defaults (bounded) | Allocation | No |
| `ALLOCATION_DEMO_MODE`, `LOCK_RENEWAL_ENABLED` | Leave unset in staging/production (defaults: demo off, renewal on); demo profiles only, refused in production | Allocation | No |
| Git repo URL/branch, ArgoCD project | Ruchir | ArgoCD Applications | No |
| `<BILLING_ACCOUNT_ID>`, `<BUDGET_AMOUNT>` | Project owner | Budget | No |

## Mock → real replacement checklist

| Local stand-in | Real dependency | Replaced when | Check |
|---|---|---|---|
| `tests/support/stateful_lock.py` (in-process/gRPC) | Nisarg's ZooKeeper Lock Coordinator | `LOCK_*` addresses set | Triage `/readyz` 200; staging contention test; fencing rejection on stale token |
| `FakeBedRegistryClient` | Jay's Bed Registry gRPC | `BED_SERVICE_ADDR` set | Allocate/release idempotent by operation ID; ownership errors surface |
| `FakeStaffSchedulerClient` | Ruchir's Staff Scheduler | `STAFF_SERVICE_ADDR` set | Capacity exhaustion → waitlist only when confirmed |
| `FakePatientRegistryClient` | Ruchir's Patient Registry | `PATIENT_SERVICE_URL` set | Malformed response → dependency-contract error |
| In-process Triage (`DirectTriageClient`) in load target | Triage gRPC service | Allocation `TRIAGE_SERVICE_ADDR` | Enqueue/Remove ledger replay in staging |
| Firestore emulator | Firestore Native | `FIRESTORE_EMULATOR_HOST` absent (rejected in cloud) | `/readyz`; index list |
| Docker PostgreSQL | Cloud SQL via proxy | Secret-mounted `DATABASE_URL_FILE` | `/readyz`; migration head |
| `RecordingPublisher` / client doubles | Pub/Sub + Kafka on GKE | `ENABLE_OUTBOX_WORKER=true` with broker settings | Section 10 event tests; backlog drains |
| Local Prometheus/Grafana (Compose) | Prometheus scraping pods or Managed Prometheus | Scrape config/PodMonitoring applied by ArgoCD | Targets UP; dashboard queries return data |
