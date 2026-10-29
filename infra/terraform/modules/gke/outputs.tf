output "cluster_name" {
  value = google_container_cluster.main.name
}

output "cluster_location" {
  value = google_container_cluster.main.location
}

output "workload_identity_pool" {
  value = google_container_cluster.main.workload_identity_config[0].workload_pool
}
