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
