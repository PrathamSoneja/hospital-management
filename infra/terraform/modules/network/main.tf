resource "google_compute_network" "main" {
  project                 = var.project_id
  name                    = var.network_name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "main" {
  project                  = var.project_id
  name                     = var.subnet_name
  region                   = var.region
  network                  = google_compute_network.main.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "${var.subnet_name}-pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "${var.subnet_name}-services"
    ip_cidr_range = var.services_cidr
  }
}

resource "google_compute_global_address" "private_services" {
  # Reserve a non-overlapping VPC range before creating the Cloud SQL peering.
  project       = var.project_id
  name          = "${var.network_name}-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
  address       = cidrhost(var.private_services_cidr, 0)
}

resource "google_service_networking_connection" "private_services" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}

resource "google_compute_router" "main" {
  project = var.project_id
  name    = "${var.network_name}-router"
  region  = var.region
  network = google_compute_network.main.id
}

resource "google_compute_router_nat" "main" {
  project                             = var.project_id
  name                                = "${var.network_name}-nat"
  router                              = google_compute_router.main.name
  region                              = var.region
  nat_ip_allocate_option              = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat  = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  enable_endpoint_independent_mapping = true

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_compute_firewall" "allow_internal" {
  project   = var.project_id
  name      = "${var.network_name}-allow-internal"
  network   = google_compute_network.main.name
  direction = "INGRESS"
  priority  = 1000

  source_ranges = [
    var.subnet_cidr,
    var.pods_cidr,
  ]

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_compute_firewall" "allow_gke_health_checks" {
  # Permit Google Cloud load-balancer health checks to reach GKE NodePorts only.
  project   = var.project_id
  name      = "${var.network_name}-allow-gke-health-checks"
  network   = google_compute_network.main.name
  direction = "INGRESS"
  priority  = 1000
  target_tags = [
    "gke-node",
  ]
  source_ranges = [
    "35.191.0.0/16",
    "130.211.0.0/22",
  ]

  allow {
    protocol = "tcp"
    ports    = ["30000-32767"]
  }
}
