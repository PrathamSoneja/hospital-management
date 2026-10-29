variable "project_id" {
  description = "Existing Google Cloud project ID with billing enabled."
  type        = string
}

variable "location" {
  description = "Location for the Terraform state bucket."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Globally unique name for the Terraform state bucket."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be a valid 3-63 character Cloud Storage bucket name."
  }
}
