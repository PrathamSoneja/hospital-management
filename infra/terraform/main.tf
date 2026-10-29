locals {
  required_apis = toset([
    "artifactregistry.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "firestore.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_apis

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

module "network" {
  source = "./modules/network"

  project_id            = var.project_id
  network_name          = "${var.name_prefix}-vpc"
  subnet_name           = "${var.name_prefix}-subnet"
  region                = var.region
  subnet_cidr           = var.subnet_cidr
  pods_cidr             = var.pods_cidr
  services_cidr         = var.services_cidr
  private_services_cidr = var.private_services_cidr

  depends_on = [google_project_service.required]
}

module "gke" {
  source = "./modules/gke"

  project_id                 = var.project_id
  cluster_name               = "${var.name_prefix}-gke"
  region                     = var.region
  zone                       = var.zone
  network                    = module.network.network_name
  subnetwork                 = module.network.subnet_name
  pods_range_name            = module.network.pods_range_name
  services_range_name        = module.network.services_range_name
  node_machine_type          = var.node_machine_type
  master_ipv4_cidr_block     = var.master_ipv4_cidr_block
  master_authorized_networks = var.master_authorized_networks
  min_node_count             = var.min_node_count
  max_node_count             = var.max_node_count
  deletion_protection        = var.deletion_protection

  depends_on = [
    google_project_service.required,
    module.network,
  ]
}

module "cloudsql" {
  source = "./modules/cloudsql"

  project_id                  = var.project_id
  instance_name               = "${var.name_prefix}-postgres"
  region                      = var.region
  network_self_link           = module.network.network_self_link
  private_services_range_name = module.network.private_services_range_name
  database_tier               = var.database_tier
  admin_password              = var.db_admin_password
  deletion_protection         = var.deletion_protection

  depends_on = [
    google_project_service.required,
    module.network,
  ]
}

module "firestore" {
  source = "./modules/firestore"

  project_id          = var.project_id
  location_id         = var.region
  deletion_protection = var.deletion_protection

  depends_on = [google_project_service.required]
}

module "storage" {
  source = "./modules/storage"

  project_id    = var.project_id
  name_prefix   = var.name_prefix
  bucket_suffix = var.bucket_suffix
  location      = var.region
  labels        = var.resource_labels

  depends_on = [google_project_service.required]
}

module "registry" {
  source = "./modules/registry"

  project_id      = var.project_id
  repository_name = "${var.name_prefix}-docker"
  location        = var.region

  depends_on = [google_project_service.required]
}
