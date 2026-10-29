output "network_name" {
  value = google_compute_network.main.name
}

output "network_self_link" {
  value = google_compute_network.main.self_link
}

output "subnet_name" {
  value = google_compute_subnetwork.main.name
}

output "pods_range_name" {
  value = google_compute_subnetwork.main.secondary_ip_range[0].range_name
}

output "services_range_name" {
  value = google_compute_subnetwork.main.secondary_ip_range[1].range_name
}

output "private_services_range_name" {
  value = google_compute_global_address.private_services.name
}
