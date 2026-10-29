variable "project_id" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "bucket_suffix" {
  type = string
}

variable "location" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}
