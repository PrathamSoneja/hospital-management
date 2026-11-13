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

variable "eventarc_subscription_id" {
  description = "Full name of Eventarc's generated Pub/Sub subscription. Set after the Eventarc trigger is created to import and configure its dead-letter policy."
  type        = string
  default     = ""
}
