output "patient_documents_bucket" {
  value = google_storage_bucket.patient_documents.name
}

output "flink_bucket" {
  value = google_storage_bucket.flink.name
}
