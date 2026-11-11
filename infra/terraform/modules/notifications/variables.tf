variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "notification_image" {
  description = "Notification service container image built and published by Member C."
  type        = string
}

variable "deletion_protection" {
  type = bool
}
