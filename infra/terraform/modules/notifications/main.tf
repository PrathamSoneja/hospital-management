resource "google_pubsub_topic" "notifications" {
  project = var.project_id
  name    = "bed-notifications"
}

resource "google_pubsub_topic" "dead_letter" {
  project = var.project_id
  name    = "bed-notifications-dlq"
}

data "google_project" "current" {
  project_id = var.project_id
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

      env {
        name  = "FIRESTORE_PROJECT_ID"
        value = var.project_id
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

  }

  depends_on = [google_project_iam_member.notification_firestore]
}

resource "google_project_iam_member" "notification_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.notification.email}"
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

locals {
  eventarc_subscription_id   = google_eventarc_trigger.notifications.transport[0].pubsub[0].subscription
  eventarc_subscription_name = basename(local.eventarc_subscription_id)
  pubsub_service_agent       = "service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Eventarc creates and owns its transport subscription. Supply its full resource
# name after the trigger exists; this import block then brings that subscription
# under Terraform so its dead-letter policy can be managed without creating a
# competing subscription.
resource "google_pubsub_subscription" "eventarc" {
  for_each = var.eventarc_subscription_id == "" ? {} : {
    eventarc = var.eventarc_subscription_id
  }

  project = var.project_id
  name    = local.eventarc_subscription_name
  topic   = google_pubsub_topic.notifications.id

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 10
  }

  lifecycle {
    # Eventarc configures the delivery endpoint on this generated subscription.
    ignore_changes = [push_config]
  }
}

import {
  for_each = var.eventarc_subscription_id == "" ? {} : {
    eventarc = var.eventarc_subscription_id
  }

  to = google_pubsub_subscription.eventarc[each.key]
  id = each.value
}

resource "google_pubsub_topic_iam_member" "pubsub_dead_letter_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${local.pubsub_service_agent}"

  depends_on = [google_eventarc_trigger.notifications]
}

resource "google_pubsub_subscription_iam_member" "pubsub_source_subscriber" {
  project      = var.project_id
  subscription = local.eventarc_subscription_name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_service_agent}"

  depends_on = [google_eventarc_trigger.notifications]
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

resource "google_secret_manager_secret" "patient_registry_migration_database_url" {
  project   = var.project_id
  secret_id = "patient-registry-migration-database-url"

  replication {
    auto {}
  }
}
