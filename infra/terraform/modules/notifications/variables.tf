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


variable "dead_letter_max_attempts" {
  description = "Failed Eventarc pushes before a notification moves to the dead-letter topic (Pub/Sub allows 5-100)."
  type        = number
  default     = 5

  validation {
    condition     = var.dead_letter_max_attempts >= 5 && var.dead_letter_max_attempts <= 100
    error_message = "Pub/Sub allows 5 to 100 delivery attempts."
  }
}