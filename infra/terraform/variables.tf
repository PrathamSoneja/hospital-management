variable "project_id" {
  description = "Existing Google Cloud project ID with billing enabled."
  type        = string
}

variable "github_owner" {
  description = "GitHub organization or account that owns the repository used by GitHub Actions."
  type        = string
}

variable "github_repository" {
  description = "Repository name (without owner) permitted to federate through GitHub Actions."
  type        = string
}

variable "github_branch" {
  description = "Only this branch may request a GitHub Actions cloud identity."
  type        = string
  default     = "main"
}

variable "kubernetes_namespace" {
  description = "Namespace used by the app-of-apps service workloads and their Kubernetes service accounts."
  type        = string
  default     = "default"
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key. Prefer CONFLUENT_CLOUD_API_KEY environment injection."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret. Prefer CONFLUENT_CLOUD_API_SECRET environment injection."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_environment_name" {
  description = "Confluent Cloud environment name."
  type        = string
  default     = "hospital-management"
}

variable "confluent_cluster_name" {
  description = "Confluent Cloud Kafka cluster name."
  type        = string
  default     = "hospital-events"
}

variable "confluent_region" {
  description = "Confluent Cloud GCP region for the Kafka cluster."
  type        = string
  default     = "us-central1"
}

variable "region" {
  description = "GCP region for regional resources and the zonal GKE cluster."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone in region where the zonal GKE cluster and node pool run."
  type        = string
  default     = "us-central1-a"

  validation {
    condition     = startswith(var.zone, "${var.region}-")
    error_message = "zone must be in the configured region."
  }
}

variable "name_prefix" {
  description = "Short lowercase prefix used for named resources."
  type        = string
  default     = "hospital"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,19}$", var.name_prefix))
    error_message = "name_prefix must be 2-20 lowercase letters, digits, or hyphens and start with a letter."
  }
}

variable "bucket_suffix" {
  description = "Globally unique lowercase suffix to avoid Cloud Storage bucket name collisions."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,15}$", var.bucket_suffix))
    error_message = "bucket_suffix must be 3-16 lowercase letters, digits, or hyphens."
  }
}

variable "subnet_cidr" {
  description = "Primary subnet range for GKE nodes and private Google access."
  type        = string
  default     = "10.10.0.0/20"

  validation {
    condition     = can(cidrhost(var.subnet_cidr, 0))
    error_message = "subnet_cidr must be a valid IPv4 CIDR range."
  }
}

variable "pods_cidr" {
  description = "Secondary subnet range reserved for GKE pods."
  type        = string
  default     = "10.20.0.0/16"

  validation {
    condition     = can(cidrhost(var.pods_cidr, 0))
    error_message = "pods_cidr must be a valid IPv4 CIDR range."
  }
}

variable "services_cidr" {
  description = "Secondary subnet range reserved for GKE services."
  type        = string
  default     = "10.30.0.0/20"

  validation {
    condition     = can(cidrhost(var.services_cidr, 0))
    error_message = "services_cidr must be a valid IPv4 CIDR range."
  }
}

variable "private_services_cidr" {
  description = "Non-overlapping /16 IPv4 range reserved for Private Service Access (Cloud SQL)."
  type        = string
  default     = "10.40.0.0/16"

  validation {
    condition     = can(cidrhost(var.private_services_cidr, 0)) && can(regex("/16$", var.private_services_cidr))
    error_message = "private_services_cidr must be a valid IPv4 /16 range."
  }
}

variable "master_ipv4_cidr_block" {
  description = "Non-overlapping /28 IPv4 range for the GKE control plane."
  type        = string
  default     = "172.16.0.0/28"

  validation {
    condition     = can(cidrhost(var.master_ipv4_cidr_block, 0)) && can(regex("/28$", var.master_ipv4_cidr_block))
    error_message = "master_ipv4_cidr_block must be a valid IPv4 /28 range."
  }
}

variable "master_authorized_networks" {
  description = "Public client CIDRs permitted to reach the GKE control-plane endpoint. Supply your trusted VPN or office CIDRs; do not use 0.0.0.0/0."
  type = list(object({
    cidr_block   = string
    display_name = string
  }))

  validation {
    condition     = length(var.master_authorized_networks) > 0
    error_message = "Configure at least one trusted master_authorized_networks CIDR."
  }
}

variable "node_machine_type" {
  description = "Machine type used by the autoscaling GKE node pool."
  type        = string
  default     = "e2-standard-4"
}

variable "min_node_count" {
  description = "Minimum number of nodes in the zonal GKE node pool."
  type        = number
  default     = 2

  validation {
    condition     = var.min_node_count >= 2 && var.min_node_count <= 4
    error_message = "min_node_count must be between 2 and 4."
  }
}

variable "max_node_count" {
  description = "Maximum number of nodes in the zonal GKE node pool."
  type        = number
  default     = 4

  validation {
    condition     = var.max_node_count >= 2 && var.max_node_count <= 4
    error_message = "max_node_count must be between 2 and 4."
  }
}

variable "database_tier" {
  description = "Cloud SQL machine tier. The default is a modest starting point; size it to measured workload and budget."
  type        = string
  default     = "db-custom-2-7680"
}

variable "db_admin_password" {
  description = "Password for the initial PostgreSQL administrator. Supply through TF_VAR_db_admin_password or a secret variable; never commit it in a tfvars file."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.db_admin_password) >= 16
    error_message = "db_admin_password must be at least 16 characters."
  }
}

variable "patient_registry_db_password" {
  description = "Password for the least-privilege Patient Registry PostgreSQL login. Supply through TF_VAR_patient_registry_db_password or secret injection; never commit it."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.patient_registry_db_password) >= 16
    error_message = "patient_registry_db_password must be at least 16 characters."
  }
}

variable "deletion_protection" {
  description = "Enable deletion protection on stateful data resources and GKE."
  type        = bool
  default     = true
}

variable "resource_labels" {
  description = "Labels applied to supported resources."
  type        = map(string)
  default = {
    application = "hospital-management"
    managed-by  = "terraform"
  }
}
