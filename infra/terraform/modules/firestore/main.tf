# --- VIVA (Member D - Jay): Storage Product 2 - Cloud Firestore Native (NoSQL) ---
# Holds pool_stats 1-minute window documents, triage waitlists, notification streams.
# Horizontally scalable document database with composite indexes for fast real-time queries.
resource "google_firestore_database" "main" {
  project                     = var.project_id
  name                        = "(default)"
  location_id                 = var.location_id
  type                        = "FIRESTORE_NATIVE"
  concurrency_mode            = "PESSIMISTIC"
  app_engine_integration_mode = "DISABLED"
  delete_protection_state     = var.deletion_protection ? "DELETE_PROTECTION_ENABLED" : "DELETE_PROTECTION_DISABLED"
}

# Composite indexes for the services' ordered queries. The Firestore emulator used in CI needs none, so these
# are only exercised in the cloud; without them the queries fail with FAILED_PRECONDITION.

# Triage: next waitlisted admission per pool by urgency, then arrival (services/triage/repository/firestore.py).
resource "google_firestore_index" "waitlist_by_priority" {
  project    = var.project_id
  database   = google_firestore_database.main.name
  collection = "waitlist_entries"

  fields {
    field_path = "pool_id"
    order      = "ASCENDING"
  }
  fields {
    field_path = "triage_level"
    order      = "ASCENDING"
  }
  fields {
    field_path = "queued_at"
    order      = "ASCENDING"
  }
  fields {
    field_path = "entry_id"
    order      = "ASCENDING"
  }
}

# Resource Analytics: per-pool window history (services/resource_analytics/storage.py).
resource "google_firestore_index" "pool_stats_history" {
  project    = var.project_id
  database   = google_firestore_database.main.name
  collection = "pool_stats"

  fields {
    field_path = "pool_id"
    order      = "ASCENDING"
  }
  fields {
    field_path = "window_start"
    order      = "ASCENDING"
  }
}
