resource "google_storage_bucket" "patient_documents" {
  project                     = var.project_id
  name                        = "${var.name_prefix}-patient-docs-${var.bucket_suffix}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = var.labels

  versioning {
    enabled = true
  }
}

# --- VIVA (Member D - Jay): Storage Product 3 - Google Cloud Storage (Object Store) ---
# Holds Flink exactly-once state checkpoints, Kafka connector jars, and patient medical PDFs.
resource "google_storage_bucket" "flink" {
  project                     = var.project_id
  name                        = "${var.name_prefix}-flink-${var.bucket_suffix}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = var.labels
}
