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

# Lets the function deduplicate on event_id and track stream_sequence in Firestore.
resource "google_project_iam_member" "notification_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.notification.email}"
}

# ---------------------------------------------------------------------------
# Notification as a serverless function (requirement b): Cloud Run functions (2nd gen).
# Eventarc delivers each Pub/Sub message on bed-notifications as a CloudEvent to
# services/notification/function.py:handle_bed_notification. Built from source; no image to publish.
# ---------------------------------------------------------------------------

locals {
  repo_root = "${path.module}/../../../.."

  # Everything the function imports, at the same paths as in the repository. The repo uses
  # namespace packages, so missing __init__.py files are added to the zip as empty files.
  function_files = [
    "services/__init__.py",
    "services/notification/__init__.py",
    "services/notification/function.py",
    "services/notification/handler.py",
    "services/notification/store.py",
    "shared/__init__.py",
    "shared/observability/__init__.py",
    "shared/observability/logging.py",
    "contracts/events/notification.schema.json",
  ]
}

data "archive_file" "notification_function" {
  type        = "zip"
  output_path = "${path.module}/.build/notification-function.zip"

  # Cloud Run functions (Python) load the entry point from main.py at the source root.
  source {
    filename = "main.py"
    content  = "from services.notification.function import handle_bed_notification  # noqa: F401\n"
  }

  source {
    filename = "requirements.txt"
    content  = file("${local.repo_root}/services/notification/function-requirements.txt")
  }

  dynamic "source" {
    for_each = toset(local.function_files)
    content {
      filename = source.value
      content  = fileexists("${local.repo_root}/${source.value}") ? file("${local.repo_root}/${source.value}") : ""
    }
  }
}

resource "google_storage_bucket" "function_source" {
  project                     = var.project_id
  name                        = "${var.project_id}-notification-fn-source"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true # Holds only build zips, which Terraform recreates.
}

# The object name changes with the code, so a code change redeploys the function.
resource "google_storage_bucket_object" "function_source" {
  name   = "notification-function-${data.archive_file.notification_function.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.notification_function.output_path
}

# Cloud Build builds the function with this account instead of the project's default compute account.
resource "google_service_account" "function_build" {
  project      = var.project_id
  account_id   = trimsuffix(substr("${var.name_prefix}-notif-build", 0, 30), "-")
  display_name = "Builds the notification function"
}

resource "google_project_iam_member" "function_build" {
  project = var.project_id
  role    = "roles/run.builder"
  member  = "serviceAccount:${google_service_account.function_build.email}"
}

resource "google_cloudfunctions2_function" "notification" {
  project  = var.project_id
  name     = "${var.name_prefix}-notification-fn"
  location = var.region

  build_config {
    runtime         = "python311"
    entry_point     = "handle_bed_notification"
    service_account = google_service_account.function_build.id

    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    min_instance_count    = 0
    max_instance_count    = 3
    available_memory      = "512Mi"
    timeout_seconds       = 60
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = google_service_account.notification.email

    environment_variables = {
      FIRESTORE_PROJECT_ID = var.project_id
    }
  }

  # Eventarc trigger on bed-notifications. Failures are retried; after 10 attempts the
  # imported subscription below moves the message to bed-notifications-dlq.
  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.notifications.id
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.eventarc.email
  }

  depends_on = [
    google_project_iam_member.notification_firestore,
    google_project_iam_member.function_build,
    google_project_iam_member.eventarc_receiver,
  ]
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

# A 2nd gen function runs as a Cloud Run service; Eventarc needs Cloud Run Invoker on it.
resource "google_cloud_run_v2_service_iam_member" "eventarc_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.notification.service_config[0].service
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.eventarc.email}"
}

locals {
  pubsub_service_agent = "service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# The function's Eventarc trigger creates and owns its Pub/Sub subscription. After the first
# apply, set eventarc_subscription_id to that subscription's full name (see the
# notification_function_trigger output) and apply again: the import block then brings it under
# Terraform so its dead-letter policy can be managed without creating a competing subscription.
resource "google_pubsub_subscription" "eventarc" {
  for_each = var.eventarc_subscription_id == "" ? {} : {
    eventarc = var.eventarc_subscription_id
  }

  project = var.project_id
  name    = basename(each.value)
  topic   = google_pubsub_topic.notifications.id

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 10
  }

  lifecycle {
    # Eventarc configures the delivery endpoint and retry backoff (10s-600s) on this generated subscription.
    ignore_changes = [push_config, retry_policy]
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
}

resource "google_pubsub_subscription_iam_member" "pubsub_source_subscriber" {
  for_each = var.eventarc_subscription_id == "" ? {} : {
    eventarc = var.eventarc_subscription_id
  }

  project      = var.project_id
  subscription = basename(each.value)
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_service_agent}"

  depends_on = [google_pubsub_subscription.eventarc]
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

resource "google_secret_manager_secret" "bed_registry_database_url" {
  project   = var.project_id
  secret_id = "bed-registry-database-url"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "bed_registry_migration_database_url" {
  project   = var.project_id
  secret_id = "bed-registry-migration-database-url"

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
