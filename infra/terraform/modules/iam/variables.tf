variable "project_id" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "github_owner" {
  type = string
}

variable "github_repository" {
  type = string
}

variable "github_branch" {
  type = string
}

variable "kubernetes_namespace" {
  type = string
}

variable "workload_identity_pool" {
  type = string
}

variable "artifact_registry_repository" {
  description = "Artifact Registry repository full resource ID."
  type        = string
}

variable "patient_documents_bucket" {
  type = string
}

variable "flink_bucket" {
  type = string
}
