resource "google_sql_database_instance" "main" {
  project             = var.project_id
  name                = var.instance_name
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = var.deletion_protection

  settings {
    tier                  = var.database_tier
    availability_type     = "ZONAL"
    disk_type             = "PD_SSD"
    disk_size             = 20
    disk_autoresize       = true
    disk_autoresize_limit = 100
    user_labels           = var.resource_labels

    backup_configuration {
      # PITR enables recovery beyond the daily backup window.
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7

      backup_retention_settings {
        retained_backups = 7
        retention_unit   = "COUNT"
      }
    }

    ip_configuration {
      # Cloud SQL has no public IPv4 address and uses Private Service Access.
      ipv4_enabled       = false
      private_network    = var.network_self_link
      allocated_ip_range = var.private_services_range_name
      ssl_mode           = "ENCRYPTED_ONLY"
    }

    maintenance_window {
      day          = 7
      hour         = 4
      update_track = "stable"
    }
  }
}

resource "google_sql_user" "postgres_admin" {
  project  = var.project_id
  name     = "postgres"
  instance = google_sql_database_instance.main.name
  password = var.admin_password
}

resource "google_sql_database" "service_databases" {
  for_each = toset(["patients", "beds", "allocations"])

  project  = var.project_id
  name     = each.value
  instance = google_sql_database_instance.main.name
}
