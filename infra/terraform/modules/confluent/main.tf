resource "confluent_environment" "main" {
  display_name = var.environment_name
}

resource "confluent_kafka_cluster" "main" {
  display_name        = var.kafka_cluster_name
  availability        = "SINGLE_ZONE"
  cloud               = "GCP"
  region              = var.region
  deletion_protection = var.deletion_protection
  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

resource "confluent_service_account" "terraform" {
  display_name = "${var.name_prefix}-terraform-kafka"
  description  = "Scoped Terraform identity for managing the hospital Kafka cluster."
}

resource "confluent_role_binding" "terraform_cluster_admin" {
  principal   = "User:${confluent_service_account.terraform.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_api_key" "terraform_kafka" {
  display_name = "${var.name_prefix}-terraform-kafka-key"
  description  = "Kafka API key used by Terraform to create and manage topics."

  owner {
    id          = confluent_service_account.terraform.id
    api_version = confluent_service_account.terraform.api_version
    kind        = confluent_service_account.terraform.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_kafka_topic" "bed_events" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name       = "bed-events"
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint
  partitions_count = 3

  credentials {
    key    = confluent_api_key.terraform_kafka.id
    secret = confluent_api_key.terraform_kafka.secret
  }

  depends_on = [confluent_role_binding.terraform_cluster_admin]
}

resource "confluent_kafka_topic" "bed_stats" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name       = "bed-stats"
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint
  partitions_count = 3

  credentials {
    key    = confluent_api_key.terraform_kafka.id
    secret = confluent_api_key.terraform_kafka.secret
  }

  depends_on = [confluent_role_binding.terraform_cluster_admin]
}

resource "confluent_service_account" "bed_allocation" {
  display_name = "${var.name_prefix}-bed-allocation"
  description  = "Runtime Kafka producer identity for the Bed Allocation service."
}

resource "confluent_api_key" "bed_allocation" {
  display_name = "${var.name_prefix}-bed-allocation-key"
  description  = "Kafka API key for producing bed-events."

  owner {
    id          = confluent_service_account.bed_allocation.id
    api_version = confluent_service_account.bed_allocation.api_version
    kind        = confluent_service_account.bed_allocation.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_role_binding" "bed_allocation_write" {
  principal   = "User:${confluent_service_account.bed_allocation.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=bed-events"
  depends_on  = [confluent_kafka_topic.bed_events]
}

resource "confluent_service_account" "resource_analytics" {
  display_name = "${var.name_prefix}-resource-analytics"
  description  = "Runtime Kafka consumer identity for the Resource Analytics API."
}

resource "confluent_api_key" "resource_analytics" {
  display_name = "${var.name_prefix}-resource-analytics-key"
  description  = "Kafka API key for consuming bed-stats."

  owner {
    id          = confluent_service_account.resource_analytics.id
    api_version = confluent_service_account.resource_analytics.api_version
    kind        = confluent_service_account.resource_analytics.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_role_binding" "resource_analytics_read_stats" {
  principal   = "User:${confluent_service_account.resource_analytics.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=bed-stats"
  depends_on  = [confluent_kafka_topic.bed_stats]
}

resource "confluent_service_account" "flink" {
  display_name = "${var.name_prefix}-flink-analytics"
  description  = "Runtime Kafka identity for the Flink bed-events to bed-stats job."
}

resource "confluent_api_key" "flink" {
  display_name = "${var.name_prefix}-flink-analytics-key"
  description  = "Kafka API key for the Flink analytics job."

  owner {
    id          = confluent_service_account.flink.id
    api_version = confluent_service_account.flink.api_version
    kind        = confluent_service_account.flink.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }
}

resource "confluent_role_binding" "flink_read_events" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=bed-events"
  depends_on  = [confluent_kafka_topic.bed_events]
}

resource "confluent_role_binding" "flink_write_stats" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/topic=bed-stats"
  depends_on  = [confluent_kafka_topic.bed_stats]
}

resource "confluent_role_binding" "flink_consumer_group" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/group=flink-*"
  depends_on  = [confluent_kafka_topic.bed_events]
}

resource "confluent_role_binding" "resource_analytics_consumer_group" {
  principal   = "User:${confluent_service_account.resource_analytics.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.main.rbac_crn}/kafka=${confluent_kafka_cluster.main.id}/group=resource-analytics-*"
  depends_on  = [confluent_kafka_topic.bed_stats]
}

output "cluster_id" {
  value = confluent_kafka_cluster.main.id
}

output "bootstrap_endpoint" {
  value = confluent_kafka_cluster.main.bootstrap_endpoint
}

output "runtime_credentials" {
  value = {
    bed_allocation = {
      username = confluent_api_key.bed_allocation.id
      password = confluent_api_key.bed_allocation.secret
    }
    resource_analytics = {
      username = confluent_api_key.resource_analytics.id
      password = confluent_api_key.resource_analytics.secret
    }
    flink = {
      username = confluent_api_key.flink.id
      password = confluent_api_key.flink.secret
    }
  }
  sensitive = true
}
