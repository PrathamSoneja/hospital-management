output "notification_topic_id" {
  value = google_pubsub_topic.notifications.id
}

output "dead_letter_topic_id" {
  value = google_pubsub_topic.dead_letter.id
}

output "notification_service_uri" {
  value = google_cloud_run_v2_service.notification.uri
}

output "allocation_database_secret_id" {
  value = google_secret_manager_secret.allocation_database_url.secret_id
}

output "allocation_migration_secret_id" {
  value = google_secret_manager_secret.allocation_migration_database_url.secret_id
}

output "kafka_username_secret_id" {
  value = google_secret_manager_secret.kafka_allocation_username.secret_id
}

output "kafka_password_secret_id" {
  value = google_secret_manager_secret.kafka_allocation_password.secret_id
}

output "patient_registry_secret_id" {
  value = google_secret_manager_secret.patient_registry_database_url.secret_id
}

output "patient_registry_migration_secret_id" {
  value = google_secret_manager_secret.patient_registry_migration_database_url.secret_id
}
