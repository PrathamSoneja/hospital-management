resource "google_artifact_registry_repository" "docker" {
  project       = var.project_id
  location      = var.location
  repository_id = var.repository_name
  description   = "Docker images for Hospital Bed Management services"
  format        = "DOCKER"

  docker_config {
    immutable_tags = true
  }
}
