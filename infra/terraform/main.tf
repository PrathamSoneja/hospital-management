locals {
  required_apis = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "dataproc.googleapis.com",
    "eventarc.googleapis.com",
    "firestore.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "sts.googleapis.com",
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

  project_id                                 = var.project_id
  instance_name                              = "${var.name_prefix}-postgres"
  region                                     = var.region
  network_self_link                          = module.network.network_self_link
  private_services_range_name                = module.network.private_services_range_name
  database_tier                              = var.database_tier
  admin_password                             = var.db_admin_password
  admin_password_version                     = var.db_admin_password_version
  patient_registry_password                  = var.patient_registry_db_password
  patient_registry_password_version          = var.patient_registry_db_password_version
  patient_registry_migrator_password         = var.patient_registry_migrator_db_password
  patient_registry_migrator_password_version = var.patient_registry_migrator_db_password_version
  allocation_app_password                    = var.allocation_app_db_password
  allocation_app_password_version            = var.allocation_app_db_password_version
  allocation_migrator_password               = var.allocation_migrator_db_password
  allocation_migrator_password_version       = var.allocation_migrator_db_password_version
  bed_registry_password                      = var.bed_registry_db_password
  bed_registry_password_version              = var.bed_registry_db_password_version
  bed_registry_migrator_password             = var.bed_registry_migrator_db_password
  bed_registry_migrator_password_version     = var.bed_registry_migrator_db_password_version
  deletion_protection                        = var.deletion_protection

  depends_on = [
    google_project_service.required,
    module.network,
  ]
}

module "notifications" {
  source = "./modules/notifications"

  project_id               = var.project_id
  region                   = var.region
  name_prefix              = var.name_prefix
  deletion_protection      = var.deletion_protection
  eventarc_subscription_id = var.eventarc_subscription_id

  depends_on = [google_project_service.required]
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
  repository_name = "hospital-repo"
  location        = var.region

  depends_on = [google_project_service.required]
}

module "iam" {
  source = "./modules/iam"

  project_id                           = var.project_id
  name_prefix                          = var.name_prefix
  github_owner                         = var.github_owner
  github_repository                    = var.github_repository
  github_branch                        = var.github_branch
  kubernetes_namespace                 = var.kubernetes_namespace
  workload_identity_pool               = module.gke.workload_identity_pool
  artifact_registry_repository         = module.registry.repository_id
  patient_documents_bucket             = module.storage.patient_documents_bucket
  flink_bucket                         = module.storage.flink_bucket
  allocation_database_secret_id        = module.notifications.allocation_database_secret_id
  allocation_migration_secret_id       = module.notifications.allocation_migration_secret_id
  kafka_username_secret_id             = module.notifications.kafka_username_secret_id
  kafka_password_secret_id             = module.notifications.kafka_password_secret_id
  patient_registry_secret_id           = module.notifications.patient_registry_secret_id
  patient_registry_migration_secret_id = module.notifications.patient_registry_migration_secret_id
  bed_registry_secret_id               = module.notifications.bed_registry_secret_id
  bed_registry_migration_secret_id     = module.notifications.bed_registry_migration_secret_id
  notification_topic_id                = module.notifications.notification_topic_id

  depends_on = [
    google_project_service.required,
    module.gke,
  ]
}

module "dataproc" {
  source = "./modules/dataproc"

  project_id     = var.project_id
  cluster_name   = "${var.name_prefix}-dataproc-flink"
  region         = var.region
  subnetwork     = module.network.subnet_name
  staging_bucket = module.storage.flink_bucket
  labels         = var.resource_labels

  depends_on = [
    google_project_service.required,
    module.network,
    module.storage,
  ]
}

# TEMPORARY: Confluent excluded from initial GCP-only deployment.
# module "confluent" {
#   source = "./modules/confluent"
#
#   name_prefix         = var.name_prefix
#   environment_name    = var.confluent_environment_name
#   kafka_cluster_name  = var.confluent_cluster_name
#   region              = var.confluent_region
#   deletion_protection = var.deletion_protection
# }

# Bed Registry database URLs (Cloud SQL Auth Proxy sidecar on 127.0.0.1:5432, private IP). Write-only secret
# data built from ephemeral variables: not stored in plan files or state; a new version is written only when
# the matching *_password_version changes.
resource "google_secret_manager_secret_version" "bed_registry_database_url" {
  secret                 = "projects/${var.project_id}/secrets/${module.notifications.bed_registry_secret_id}"
  secret_data_wo         = "postgresql+psycopg2://bed_registry:${urlencode(var.bed_registry_db_password)}@127.0.0.1:5432/beds"
  secret_data_wo_version = var.bed_registry_db_password_version

  depends_on = [module.cloudsql]
}

resource "google_secret_manager_secret_version" "bed_registry_migration_database_url" {
  secret                 = "projects/${var.project_id}/secrets/${module.notifications.bed_registry_migration_secret_id}"
  secret_data_wo         = "postgresql+psycopg2://bed_registry_migrator:${urlencode(var.bed_registry_migrator_db_password)}@127.0.0.1:5432/beds"
  secret_data_wo_version = var.bed_registry_migrator_db_password_version

  depends_on = [module.cloudsql]
}
