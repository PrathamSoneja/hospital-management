resource "google_firestore_database" "main" {
  project                     = var.project_id
  name                        = "(default)"
  location_id                 = var.location_id
  type                        = "FIRESTORE_NATIVE"
  concurrency_mode            = "PESSIMISTIC"
  app_engine_integration_mode = "DISABLED"
  delete_protection_state     = var.deletion_protection ? "DELETE_PROTECTION_ENABLED" : "DELETE_PROTECTION_DISABLED"
}
