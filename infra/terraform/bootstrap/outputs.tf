output "state_bucket_name" {
  description = "Use this bucket in the runtime root's GCS backend configuration."
  value       = google_storage_bucket.terraform_state.name
}
