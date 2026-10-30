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

variable "deletion_protection" {
  type = bool
}

variable "resource_labels" {
  type    = map(string)
  default = {}
}
