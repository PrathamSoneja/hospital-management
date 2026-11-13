# Hospital Bed Allocation

A Python microservices system for hospital patient admission, bed allocation, staff scheduling,
distributed locking, and resource analytics. The target platform is Google Cloud Platform:
GKE Standard, Cloud SQL, Firestore, Cloud Storage, Artifact Registry, Pub/Sub/Eventarc/Cloud Run,
Dataproc/Flink, and Confluent Cloud Kafka. Infrastructure is managed with Terraform and Kubernetes
workloads are managed through Argo CD; application manifests are not deployed with `kubectl apply`.

## Recent repository update

### Snapshot before the latest pull

Before the latest pull, the local branch `ruchir_kumar` ended at `018b8db`
(`feat(gitops): add app-of-apps and image promotion workflow`, November 12, 2026).
That update consisted of four focused commits:

- `d745c27` — Terraform platform updates and a separate Helm-based Argo CD bootstrap stack.
- `0897d38` — Patient Registry database migration-role grants.
- `f773d3b` — Patient Registry and Staff Scheduler GKE manifests, Secret Manager CSI mounts,
  and GKE overlays.
- `018b8db` — App-of-Apps Applications, image-promotion CI workflow, and GitOps setup notes.

The Phase 3 work targets `hospital-system`, `asia-south1`, and the `hospital-repo` Artifact Registry
repository. Patient Registry has a separate migration workload identity; Staff Scheduler uses
Firestore and requires no SQL migration.

### Latest pull and changes now present

The most recent `git pull origin master` was a fast-forward from `018b8db` to `14d7827`
(`Merge pull request #14 from PrathamSoneja/feature/bed-registry-and-analytics`, November 13, 2026).
The pull brought in several merged feature branches, including PRs #10–#14 and the pre-cloud
integration work. The key additions and updates are:

- **Bed Registry and Resource Analytics (PR #14):** added deployment paths for Bed Registry and
  Resource Analytics, connected analytics to Flink `bed-stats`, improved fencing/event handling,
  and separated those services from the Allocation/Triage staging overlay.
- **Notification (PR #10):** added a Pub/Sub/Eventarc event handler, duplicate and sequence-gap
  handling with Firestore, metrics, a Docker image, and CI integration.
- **Lock Coordinator (PRs #9, #11, and #13):** added ZooKeeper-backed Kubernetes resources,
  hardened acquire/renew/release behavior, and added metrics and monitoring configuration.
- **Allocation and Triage integration:** added lease renewal and recovery improvements, safer
  idempotent retries, real-service integration coverage, and Triage queue transaction changes.
- **Local verification tooling:** expanded the sandbox, migration/role checks, load tests, release
  checks, dashboards, and integration runbooks.
- **Branch update:** the repository default branch changed from `main` to `master`; CI's push
  trigger now targets `master`.

The working tree was clean when this README was first written. The subsequent local integration
updates are summarized below; validation evidence remains explicitly identified by its run.

## Service and deployment layout

| Component | Source / deployment path | Main responsibility |
|---|---|---|
| Patient Registry | `services/patient_registry/`; `k8s/patient-registry/` | Patient, admission, and document APIs; Cloud SQL and Cloud Storage |
| Staff Scheduler | `services/staff_scheduler/`; `k8s/staff-scheduler/` | Firestore-backed roster and shifts; REST and gRPC capacity/assignment APIs |
| Bed Allocation | `services/bed_allocation/`; GKE overlays | Admission workflow, coordination, and event publishing |
| Triage | `services/triage/`; GKE overlays | Firestore-backed waitlist and triage operations |
| Lock Coordinator | `services/lock_coordinator/`; `k8s/lock-coordinator/` | Distributed locks and fencing using ZooKeeper |
| Bed Registry | `services/bed_registry/`; `k8s/bed-registry/` | Bed inventory and fencing-aware bed operations |
| Notification | `services/notification/` | Eventarc-triggered Pub/Sub notification processing |
| Resource Analytics | `services/resource_analytics/`; `k8s/resource-analytics/` | Flink stream aggregation and analytics APIs |

Terraform configuration is under `infra/terraform/`. The `bootstrap_k8s` stack installs Argo CD
after the GKE cluster exists. GitOps application definitions are under `gitops/` and service
manifests and overlays are under `k8s/`.

## Verification status

The latest recorded local CI-mode evidence in [`docs/precloud-status.md`](docs/precloud-status.md)
reports 245 passing tests and 2 expected failures, with the local load procedure passing. That
evidence predates the November 13 pull and is not a verification result for the current HEAD.
Nothing in this repository has been deployed to GCP according to
[`docs/gcp-integration-runbook.md`](docs/gcp-integration-runbook.md).

## Subsequent integration updates

- **Argo CD branch and inventory:** root and child Applications now target `master`, and Terraform's
  GitHub Actions Workload Identity branch default is aligned to `master`. The unique
  Allocation/Triage, migration, Bed Registry, and Resource Analytics Applications are under
  `gitops/apps/`; the duplicate Lock Coordinator staging Application was consolidated with the
  existing GKE child Application.
- **Notification Terraform:** Cloud Run now receives `FIRESTORE_PROJECT_ID`, its service account
  has Firestore `roles/datastore.user`, and the Pub/Sub/Eventarc transport subscription can be
  imported and configured to dead-letter to `bed-notifications-dlq`. The Pub/Sub service agent is
  granted publisher access to the DLQ and subscriber access to the source subscription.
- **Staff Scheduler:** `_on_shift()` now passes a projected Firestore query to the transaction,
  and the strict expected-failure marker has been removed. The boundary test still needs the local
  Firestore emulator to run rather than skip.
- **GKE Prometheus:** Managed Service for Prometheus was already enabled in the GKE Terraform
  module; no duplicate change was necessary.

## Follow-up before deployment

1. **Complete environment values and secrets.** Replace project, service-account, cluster, and image
   values with approved deployment values. Populate required Secret Manager versions outside Git.
   Never commit credentials or service-account keys.
2. **Review and apply Terraform deliberately.** Confirm resource names, region, provider lock files,
   notification image, and any existing state before applying. Review for replacement or destructive
   changes.
3. **Re-run current validation.** Run the repository CI checks and integration suite against the
   pulled tree. Review the current Staff Scheduler and Resource Analytics test status rather than
   relying on pre-pull strict-xfail evidence.

See [`gitops/README.md`](gitops/README.md), [`infra/terraform/README.md`](infra/terraform/README.md),
and [`docs/gcp-integration-runbook.md`](docs/gcp-integration-runbook.md) for setup and operational
details.
