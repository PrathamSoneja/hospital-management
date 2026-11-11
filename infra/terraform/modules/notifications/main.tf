resource "google_pubsub_topic" "notifications" {
  project = var.project_id
  name    = "${var.name_prefix}-bed-notifications"
}

resource "google_pubsub_topic" "dead_letter" {
  project = var.project_id
  name    = "${var.name_prefix}-bed-notifications-dlq"
}

resource "google_service_account" "notification" {
  project      = var.project_id
  account_id   = trimsuffix(substr("${var.name_prefix}-notification", 0, 30), "-")
  display_name = "Cloud Run notification handler"
}

resource "google_cloud_run_v2_service" "notification" {
  project             = var.project_id
  name                = "${var.name_prefix}-notification"
  location            = var.region
  deletion_protection = var.deletion_protection
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.notification.email

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    containers {
      image = var.notification_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }
}

resource "google_service_account" "eventarc" {
  project      = var.project_id
  account_id   = trimsuffix(substr("${var.name_prefix}-eventarc", 0, 30), "-")
  display_name = "Eventarc invoker for the notification service"
}

resource "google_project_iam_member" "eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc.email}"
}

resource "google_cloud_run_v2_service_iam_member" "eventarc_invoker" {
  project  = var.project_id
  location = google_cloud_run_v2_service.notification.location
  name     = google_cloud_run_v2_service.notification.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.eventarc.email}"
}

resource "google_eventarc_trigger" "notifications" {
  project         = var.project_id
  name            = "${var.name_prefix}-notification-pubsub"
  location        = var.region
  service_account = google_service_account.eventarc.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.notification.name
      region  = google_cloud_run_v2_service.notification.location
    }
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.notifications.id
    }
  }

  depends_on = [
    google_project_iam_member.eventarc_receiver,
    google_cloud_run_v2_service_iam_member.eventarc_invoker,
  ]
}

resource "google_secret_manager_secret" "allocation_database_url" {
  project   = var.project_id
  secret_id = "allocation-database-url"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "allocation_migration_database_url" {
  project   = var.project_id
  secret_id = "allocation-migration-database-url"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kafka_allocation_username" {
  project   = var.project_id
  secret_id = "kafka-allocation-sasl-username"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kafka_allocation_password" {
  project   = var.project_id
  secret_id = "kafka-allocation-sasl-password"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "patient_registry_database_url" {
  project   = var.project_id
  secret_id = "patient-registry-database-url"

  replication {
    auto {}
  }
}
