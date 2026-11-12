output "cluster_name" {
  value       = google_dataproc_cluster.flink_cluster.name
  description = "Name of the Dataproc Flink cluster"
}

output "cluster_id" {
  value       = google_dataproc_cluster.flink_cluster.id
  description = "ID of the Dataproc Flink cluster"
}

output "http_ports" {
  value       = google_dataproc_cluster.flink_cluster.cluster_config[0].endpoint_config[0].http_ports
  description = "Map of HTTP ports exposed by the Dataproc cluster (including Flink Dashboard)"
}
