variable "project_id" {
  description = "GCP project containing the already-provisioned GKE cluster."
  type        = string
}

variable "region" {
  description = "GCP region for provider defaults."
  type        = string
  default     = "asia-south1"
}

variable "cluster_name" {
  description = "Name of the existing GKE Standard cluster."
  type        = string
  default     = "hospital-gke"
}

variable "cluster_location" {
  description = "Zone or region where the existing GKE cluster runs."
  type        = string
  default     = "asia-south1-a"
}

variable "argocd_chart_version" {
  description = "Pinned Argo CD Helm chart version."
  type        = string
  default     = "7.7.16"
}
