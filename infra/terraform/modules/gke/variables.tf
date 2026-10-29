variable "project_id" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "network" {
  type = string
}

variable "subnetwork" {
  type = string
}

variable "pods_range_name" {
  type = string
}

variable "services_range_name" {
  type = string
}

variable "node_machine_type" {
  type = string
}

variable "master_ipv4_cidr_block" {
  type = string
}

variable "master_authorized_networks" {
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
}

variable "min_node_count" {
  type = number
}

variable "max_node_count" {
  type = number
}

variable "deletion_protection" {
  type = bool
}

variable "resource_labels" {
  type    = map(string)
  default = {}
}
