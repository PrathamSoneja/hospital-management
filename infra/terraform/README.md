# Terraform: Phase 1.1–1.4

This directory provisions the shared GCP foundation described in `cloud_proj_plan.docx`:

- A separately bootstrapped, versioned GCS bucket for remote Terraform state.
- A custom VPC, regional subnet with GKE secondary ranges, Private Service Access, Cloud Router, Cloud NAT, and restricted ingress firewall rules.
- A zonal GKE Standard cluster with private nodes, Workload Identity, and an autoscaling 2–4-node pool.
- A private-IP Cloud SQL for PostgreSQL 16 instance and the `patients`, `beds`, and `allocations` databases.
- A Firestore Native database, private patient-document and Flink buckets, and a Docker Artifact Registry repository.
- GitHub Actions Workload Identity Federation restricted to one repository and branch, an Artifact Registry writer identity, and per-service GKE Workload Identity bindings with scoped Google Cloud IAM roles.
- A Confluent Cloud GCP Basic Kafka cluster, `bed-events` and `bed-stats` topics, and scoped runtime Kafka identities for Bed Allocation, Flink, and Resource Analytics.

Kubernetes application workloads are not deployed by this Terraform stack. ArgoCD remains the deployment mechanism for workloads.

## Prerequisites

- An existing GCP project with billing enabled and permissions to enable APIs, manage IAM, networking, GKE, Cloud SQL, Firestore, Artifact Registry, and Cloud Storage.
- Application Default Credentials configured for the Terraform operator.
- A globally unique state bucket name and `bucket_suffix`.
- Trusted CIDR blocks for GKE control-plane access. Do not authorize `0.0.0.0/0`.
- A PostgreSQL administrator password of at least 16 characters.
- The GitHub owner/repository and branch that will run GitHub Actions.
- Confluent Cloud API credentials (Terraform provider uses `CONFLUENT_CLOUD_API_KEY` and `CONFLUENT_CLOUD_API_SECRET`).

The default VPC and GKE ranges must not overlap with any connected network. Change the range variables if they do.

## Bootstrap remote state, then initialize the root

Run Terraform from `infra/terraform/bootstrap` first, supplying `project_id` and a globally unique `bucket_name` (for example via `TF_VAR_project_id` and `TF_VAR_bucket_name`). Bootstrap initially stores its own state locally; protect that local state and retain it safely after bootstrap.

After the bucket exists, copy `backend.hcl.example` to `backend.hcl`, set its bucket name, and initialize the root stack from `infra/terraform` with that backend configuration and state migration enabled. The backend configuration file is intentionally not committed.

For the root stack, supply `project_id`, `github_owner`, `github_repository`, `bucket_suffix`, `master_authorized_networks`, `db_admin_password`, and `patient_registry_db_password` as Terraform variables. Prefer environment variables or your approved secret-injection mechanism for secrets; never commit credentials or populated `*.tfvars` files. The GCS backend bucket has uniform bucket-level access, public access prevention, and object versioning. Restrict its IAM access to Terraform operators.

Run `terraform fmt -recursive` from `infra/terraform`, then `terraform init`, `terraform validate`, and `terraform plan` against the root. Review the plan before applying: GKE, Cloud SQL, NAT, and the managed resources can incur charges.

The Confluent Terraform manager and application Kafka credentials are stored in Terraform state and marked sensitive in outputs. Secure the GCS state bucket with least-privilege IAM; do not print or commit credential outputs. Configure workload secrets through the organization's approved secret mechanism.

The GitHub OIDC provider only accepts the configured repository and branch. The GitHub Actions service account has Artifact Registry write access but no GCP project-wide editor role. The runtime GKE service accounts are bound to their corresponding Kubernetes service accounts in `kubernetes_namespace`; keep those names in sync with GitOps manifests.

Deletion protection defaults to enabled. To destroy protected stateful resources, first make an explicitly reviewed change to disable protection and apply it; do not bypass protections for routine cleanup. The Confluent Basic cluster and Cloud SQL/GKE/NAT resources can incur charges.
