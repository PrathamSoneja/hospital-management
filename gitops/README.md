# GitOps bootstrap and image promotion

The GitOps layout is:

```text
gitops/
  bootstrap/root-application.yaml       # One-time Argo CD root app registration
  apps/                                  # Child Application manifests tracked by the root
    project.yaml                         # Restricts child apps to this repository and namespace
    patient-registry.yaml
    staff-scheduler.yaml
    lock-coordinator.yaml
k8s/
  patient-registry/base/                 # Runtime resources and migration hook
  staff-scheduler/base/
  overlays/gke/
    kustomization.yaml                   # Aggregate render and centrally reviewed image tags
    patient-registry/
    staff-scheduler/
  lock-coordinator/overlays/gke/
```

All service resources are deployed to `hospital-system`. The Argo CD chart is installed into the
separate `argocd` namespace. The root Application watches `main` and discovers child Applications
under `gitops/apps`; each child auto-syncs its own Kustomize overlay.

## Provisioning order

1. Apply the main Terraform stack in `infra/terraform` with `region=asia-south1`,
   `zone=asia-south1-a`, `kubernetes_namespace=hospital-system`, and the repository name
   `hospital-repo`. Review the plan before applying. This provisions GKE, Artifact Registry, Workload
   Identity, scoped secret access, and the Pub/Sub/Eventarc/Cloud Run resources.
2. After GKE exists, initialize and apply the separate `infra/terraform/bootstrap_k8s` stack using the
   same remote-state bucket and a distinct prefix, for example
   `hospital-management/bootstrap-k8s`. This installs the pinned Argo CD Helm chart; it does not
   install application workloads.
3. Configure the GitHub repository variables listed below. Add repository credentials to Argo CD
   through the approved secret workflow if the GitHub repository is private; never commit a token.
4. Register `gitops/bootstrap/root-application.yaml` once from the Argo CD UI or with the Argo CD CLI.
   The root then registers and syncs child Applications from Git. Do not deploy application manifests
   with `kubectl apply`.
5. Before enabling sync, create Secret Manager versions outside Terraform for
   `patient-registry-database-url` and `patient-registry-migration-database-url`. The runtime URL must
   use the `patient_registry` SQL login and the migration URL must use `patient_registry_migrator`.
   Both URLs connect to `127.0.0.1:5432/patients`, where the Cloud SQL Auth Proxy sidecar listens.
   Before the first migration, a database owner must grant that migration role `CONNECT` on
   `patients`, grant `USAGE, CREATE` on schema `public`, and transfer ownership of that schema to it.
   For an existing schema, review and transfer ownership of existing tables and sequences too. The
   migration secret is readable only by the migration workload identity.
6. Argo CD applies the Patient Registry config and CSI identities at sync wave `-2`, runs the Alembic
   migration Job at wave `-1`, and rolls out the Deployment at the default wave. The migration is
   repeatable (`upgrade head`); if it fails, sync stops for operator review. Staff Scheduler uses
   Firestore and requires no SQL migration or seed job. Populate roster and shifts through its
   idempotent REST API after the service is ready.

## GitHub Actions repository variables

The publishing job in `.github/workflows/ci.yml` requires:

| Variable | Value |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID |
| `GCP_WIF_PROVIDER` | Terraform output `github_actions_workload_identity_provider` |
| `GCP_WIF_SERVICE_ACCOUNT` | Terraform output `github_actions_service_account` |
| `CLOUD_SQL_INSTANCE_CONNECTION_NAME` | Terraform output, in `project:region:instance` form |
| `PATIENT_DOCUMENTS_BUCKET` | Terraform output `patient_documents_bucket` |
| `PATIENT_REGISTRY_GSA` | Terraform output `patient_registry_gsa` |
| `PATIENT_REGISTRY_MIGRATION_GSA` | Terraform output `patient_registry_migration_gsa` |
| `STAFF_SCHEDULER_GSA` | Terraform output `staff_scheduler_gsa` |
| `LOCK_COORDINATOR_GSA` | Terraform output `lock_coordinator_gsa` |
| `ZOOKEEPER_GSA` | Terraform output `zookeeper_gsa` |

Terraform's GitHub WIF provider must trust the same repository and `main` branch. The federated
service account needs Artifact Registry Writer on `hospital-repo`; the existing Terraform IAM module
grants this. The GitHub workflow token needs permission to push the image-promotion commit to `main`.
Branch protection must permit that bot commit or provide an equivalent approved promotion workflow.

Before the first root sync, set `newTag` in
`k8s/lock-coordinator/overlays/gke/kustomization.yaml` to the immutable image tag published by
Nisarg. On each successful push to `main`, CI builds Patient Registry and Staff Scheduler images,
pushes them with the full commit SHA as an immutable Artifact Registry tag, updates the root GKE
Kustomization and service overlays, and pushes the image-reference change back to `main`. Argo CD
then syncs the updated image tags. The workflow never builds or changes the teammate-owned Lock
Coordinator image.

Secret values never enter Git or Terraform. GKE Secret Manager CSI mounts the two Patient Registry
database URLs as files. The application Deployment reads the runtime URL from the mounted file;
the migration Job reads the separate migration URL. Workload Identity is used for Cloud SQL proxy,
Cloud Storage, Firestore, and Secret Manager access; no service-account keys are mounted.
