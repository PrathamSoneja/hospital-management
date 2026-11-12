# Terraform: Phase 1.1–1.4

This directory provisions the shared GCP foundation described in `cloud_proj_plan.docx`:

- A separately bootstrapped, versioned GCS bucket for remote Terraform state.
- A custom VPC, regional subnet with GKE secondary ranges, Private Service Access, Cloud Router, Cloud NAT, and restricted ingress firewall rules.
- A zonal GKE Standard cluster with private nodes, Workload Identity, and an autoscaling 2–4-node pool.
- A private-IP Cloud SQL for PostgreSQL 16 instance and the `patients`, `beds`, and `allocations` databases, including separate Bed Allocation runtime and migration users.
- A Firestore Native database, private patient-document and Flink buckets, and a Docker Artifact Registry repository.
- GitHub Actions Workload Identity Federation restricted to one repository and branch, an Artifact Registry writer identity, and per-service GKE Workload Identity bindings with scoped Google Cloud IAM roles.
- GKE Secret Manager CSI integration, Dataplane V2 NetworkPolicy enforcement, and managed Prometheus.
- A Pub/Sub notification topic routed through Eventarc to a private Cloud Run service, plus Secret Manager secret containers and narrowly scoped workload IAM.
- A Confluent Cloud GCP Basic Kafka cluster, `bed-events` and `bed-stats` topics, and scoped runtime Kafka identities for Bed Allocation, Flink, and Resource Analytics.

Kubernetes application workloads are not deployed by this Terraform stack. ArgoCD remains the deployment mechanism for workloads.

## Prerequisites

- An existing GCP project with billing enabled and permissions to enable APIs, manage IAM, networking, GKE, Cloud SQL, Firestore, Artifact Registry, and Cloud Storage.
- Application Default Credentials configured for the Terraform operator.
- A globally unique state bucket name and `bucket_suffix`.
- Trusted CIDR blocks for GKE control-plane access. Do not authorize `0.0.0.0/0`.
- PostgreSQL administrator, Patient Registry, Bed Allocation runtime, and Bed Allocation migration passwords of at least 16 characters. Passwords are passed to Cloud SQL using write-only provider arguments; Terraform does not create Secret Manager versions.
- The notification container image supplied by the teammate who owns that service.
- The GitHub owner/repository and branch that will run GitHub Actions.
- Confluent Cloud API credentials (Terraform provider uses `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET`).

The default VPC and GKE ranges must not overlap with any connected network. Change the range variables if they do.

## Bootstrap remote state, then initialize the root

Run Terraform from `infra/terraform/bootstrap` first, supplying `project_id` and a globally unique `bucket_name` (for example via `TF_VAR_project_id` and `TF_VAR_bucket_name`). Bootstrap initially stores its own state locally; protect that local state and retain it safely after bootstrap.

After the bucket exists, copy `backend.hcl.example` to `backend.hcl`, set its bucket name, and initialize the root stack from `infra/terraform` with that backend configuration and state migration enabled. The backend configuration file is intentionally not committed.

For the root stack, supply `project_id`, `github_owner`, `github_repository`, `bucket_suffix`, `master_authorized_networks`, all five database password variables, and `notification_image` as Terraform variables. Prefer environment variables or your approved secret-injection mechanism for secrets; never commit credentials or populated `*.tfvars` files. The GCS backend bucket has uniform bucket-level access, public access prevention, and object versioning. Restrict its IAM access to Terraform operators.

Patient Registry has separate `patient_registry` runtime and `patient_registry_migrator` database logins.
Before the migration hook's first run, have a database owner grant `CONNECT` on `patients` to
`patient_registry_migrator`, grant it `USAGE, CREATE` on `public`, and transfer ownership of `public`
to that migration role. For an existing schema, review and transfer ownership of existing tables and
sequences as well. This one-time SQL bootstrap is separate from Terraform-managed infrastructure.

The defaults target `asia-south1`, the zonal cluster `hospital-gke` in `asia-south1-a`,
`hospital-system`, and the Docker repository `hospital-repo`. Review these names and the Terraform
plan against the target environment before applying. The notification topic ID is exactly
`bed-notifications`.

Run `terraform fmt -recursive` from `infra/terraform`, then `terraform init`, `terraform validate`, and `terraform plan` against the root. Review the plan before applying: GKE, Cloud SQL, NAT, and the managed resources can incur charges.

The Confluent Terraform manager and application Kafka credentials are stored in Terraform state and marked sensitive in outputs. This Kafka architecture remains unchanged pending the Ruchir/Jay decision. Secure the GCS state bucket with least-privilege IAM; do not print or commit credential outputs.

Terraform creates Secret Manager secret containers only, not secret values. Populate database URLs and Kafka credentials using the approved secret-management workflow before starting workloads. Before the first Alembic run, a database owner must grant `USAGE, CREATE` on the `public` schema to `allocation_migrator`; PostgreSQL does not expose this database grant through the Cloud SQL Terraform resource. Run the migrations using the migration URL. Objects created by Alembic are owned by `allocation_migrator`; revision `20261031_04` grants runtime DML on existing and future objects to `allocation_app`. The notification image must exist before applying the Cloud Run service.

The GitHub OIDC provider only accepts the configured repository and branch. The GitHub Actions service account has Artifact Registry write access but no GCP project-wide editor role. The runtime GKE service accounts are bound to their corresponding Kubernetes service accounts in `kubernetes_namespace`; keep those names in sync with GitOps manifests.

Install Argo CD only after the GKE cluster has been created. Use the independent
`infra/terraform/bootstrap_k8s` Terraform stack, a distinct GCS backend prefix, and the same project,
cluster name, and cluster location. This split avoids configuring Helm against a cluster endpoint
that does not exist during the first infrastructure apply. Argo CD itself is installed with a pinned
Helm chart and a ClusterIP service; expose its UI only through your approved private access path.

Deletion protection defaults to enabled. To destroy protected stateful resources, first make an explicitly reviewed change to disable protection and apply it; do not bypass protections for routine cleanup. The Confluent Basic cluster and Cloud SQL/GKE/NAT resources can incur charges.
