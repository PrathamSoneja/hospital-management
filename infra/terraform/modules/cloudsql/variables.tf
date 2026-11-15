variable "project_id" {
  type = string
}

variable "instance_name" {
  type = string
}

variable "region" {
  type = string
}

variable "network_self_link" {
  type = string
}

variable "private_services_range_name" {
  type = string
}

variable "database_tier" {
  type = string
}

variable "admin_password" {
  type      = string
  sensitive = true
}

variable "admin_password_version" {
  type    = number
  default = 1
}

variable "patient_registry_password" {
  type      = string
  sensitive = true
}

variable "patient_registry_password_version" {
  type    = number
  default = 1
}

variable "patient_registry_migrator_password" {
  type      = string
  sensitive = true
}

variable "patient_registry_migrator_password_version" {
  type    = number
  default = 1
}

variable "allocation_app_password" {
  type      = string
  sensitive = true
}

variable "allocation_app_password_version" {
  type    = number
  default = 1
}

variable "allocation_migrator_password" {
  type      = string
  sensitive = true
}

variable "allocation_migrator_password_version" {
  type    = number
  default = 1
}

variable "deletion_protection" {
  type = bool
}

variable "resource_labels" {
  type    = map(string)
  default = {}
}

# Bed Registry database users. Ephemeral + write-only: the values never enter plan files or state. They are
# needed only when a user is first created or its *_version is incremented (rotation).
variable "bed_registry_password" {
  type      = string
  sensitive = true
  ephemeral = true
  default   = null
}

variable "bed_registry_password_version" {
  type    = number
  default = 1
}

variable "bed_registry_migrator_password" {
  type      = string
  sensitive = true
  ephemeral = true
  default   = null
}

variable "bed_registry_migrator_password_version" {
  type    = number
  default = 1
}
