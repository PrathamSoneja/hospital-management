variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
}

variable "region" {
  type        = string
  description = "Google Cloud Region"
}

variable "cluster_name" {
  type        = string
  description = "Dataproc Cluster Name"
}

variable "subnetwork" {
  type        = string
  description = "VPC Subnetwork self-link or name for Dataproc nodes"
}

variable "machine_type" {
  type        = string
  default     = "e2-standard-2"
  description = "Machine type for Dataproc single-node master"
}

variable "staging_bucket" {
  type        = string
  description = "GCS bucket for Dataproc staging and Flink state"
}

variable "service_account" {
  type        = string
  default     = null
  description = "Service account email running the Dataproc cluster"
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Resource labels"
}
