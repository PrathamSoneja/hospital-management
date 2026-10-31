output "private_ip_address" {
  value = google_sql_database_instance.main.private_ip_address
}

output "connection_name" {
  value = google_sql_database_instance.main.connection_name
}

output "database_names" {
  value = [for database in google_sql_database.service_databases : database.name]
}

output "patient_registry_username" {
  value = google_sql_user.patient_registry.name
}
