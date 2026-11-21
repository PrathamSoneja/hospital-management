resource "google_dataproc_cluster" "flink_cluster" {
  project = var.project_id
  name    = var.cluster_name
  region  = var.region
  labels  = var.labels

  cluster_config {
    staging_bucket = var.staging_bucket

    gce_cluster_config {
      subnetwork       = var.subnetwork
      internal_ip_only = true
      service_account  = var.service_account
      # Dataproc always adds these three default scopes; listing them keeps the plan from replacing the cluster
      # on every run (cloud-platform already covers them).
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
      ]
      tags = ["dataproc", "flink"]
    }

    master_config {
      num_instances = 1
      machine_type  = var.machine_type

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    # Single-node cluster by default (worker_count = 0): 2 vCPU, so two GKE e2-standard-4 nodes + Dataproc fit
    # the project's 12-vCPU global quota. Changing the worker count replaces the cluster.
    # With 0 workers Dataproc stores no worker machine type or disk, so they are set only when workers exist
    # (otherwise every plan sees them as new and replaces the cluster).
    worker_config {
      num_instances = var.worker_count
      machine_type  = var.worker_count > 0 ? var.machine_type : null

      dynamic "disk_config" {
        for_each = var.worker_count > 0 ? [1] : []
        content {
          boot_disk_type    = "pd-standard"
          boot_disk_size_gb = 50
        }
      }
    }

    software_config {
      image_version       = "2.1-debian11"
      # --- VIVA (Member D - Jay): Managed Dataproc Cluster with Apache Flink component ---
      optional_components = ["FLINK", "DOCKER"]
      override_properties = merge(
        { "dataproc:dataproc.allow.custom.images" = "true" },
        var.worker_count == 0 ? { "dataproc:dataproc.allow.zero.workers" = "true" } : {},
      )
    }

    endpoint_config {
      enable_http_port_access = true
    }
  }
}
