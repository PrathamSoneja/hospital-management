output "gke_cluster_name" {
  description = "Name of the zonal GKE Standard cluster."
  value       = module.gke.cluster_name
}

output "gke_cluster_location" {
  description = "Zone of the zonal GKE Standard cluster."
  value       = module.gke.cluster_location
}

output "gke_workload_identity_pool" {
  description = "Workload Identity pool configured on the GKE cluster."
  value       = module.gke.workload_identity_pool
}

output "artifact_registry_repository" {
  description = "Artifact Registry Docker repository resource name."
  value       = module.registry.repository_id
}

output "cloudsql_private_ip" {
  description = "Private IP address of the Cloud SQL instance."
  value       = module.cloudsql.private_ip_address
}

output "cloudsql_connection_name" {
  description = "Cloud SQL connection name for use by authorized clients."
  value       = module.cloudsql.connection_name
}

output "patient_registry_database_username" {
  description = "Least-privilege database username for the Patient Registry."
  value       = module.cloudsql.patient_registry_username
}

output "patient_documents_bucket" {
  description = "Private Cloud Storage bucket for patient documents."
  value       = module.storage.patient_documents_bucket
}

output "flink_bucket" {
  description = "Private Cloud Storage bucket for Flink artifacts and checkpoints."
  value       = module.storage.flink_bucket
}

output "firestore_database_name" {
  description = "Firestore Native database resource name."
  value       = module.firestore.database_name
}

output "github_actions_workload_identity_provider" {
  description = "Workload Identity Provider resource name for GitHub Actions."
  value       = module.iam.github_actions_provider_name
}

output "github_actions_service_account" {
  description = "Service account email to configure in google-github-actions/auth."
  value       = module.iam.github_actions_service_account_email
}

output "kubernetes_service_account_gsas" {
  description = "Google service account emails bound to Kubernetes service accounts."
  value       = module.iam.kubernetes_service_account_gsas
}

output "confluent_cluster_id" {
  description = "Confluent Cloud Kafka cluster ID."
  value       = module.confluent.cluster_id
}

output "confluent_bootstrap_endpoint" {
  description = "Kafka bootstrap endpoint."
  value       = module.confluent.bootstrap_endpoint
}

output "confluent_runtime_credentials" {
  description = "Sensitive Kafka credentials for Bed Allocation and Resource Analytics. Store as Kubernetes secrets through the approved secret workflow."
  value       = module.confluent.runtime_credentials
  sensitive   = true
}
