locals {
  service_accounts = {
    patient-registry = {
      kubernetes_name = "patient-registry-sa"
      project_roles   = ["roles/cloudsql.client"]
      bucket_roles = {
        patient_documents = "roles/storage.objectUser"
      }
    }
    staff-scheduler = {
      kubernetes_name = "staff-scheduler-sa"
      project_roles   = ["roles/datastore.user"]
      bucket_roles    = {}
    }
    bed-allocation = {
      kubernetes_name = "bed-allocation-sa"
      project_roles   = ["roles/cloudsql.client"]
      bucket_roles    = {}
    }
    triage = {
      kubernetes_name = "triage-sa"
      project_roles   = ["roles/datastore.user"]
      bucket_roles    = {}
    }
    bed-registry = {
      kubernetes_name = "bed-registry-sa"
      project_roles   = ["roles/cloudsql.client"]
      bucket_roles    = {}
    }
    resource-analytics = {
      kubernetes_name = "resource-analytics-sa"
      project_roles   = ["roles/datastore.user"]
      bucket_roles = {
        flink = "roles/storage.objectAdmin"
      }
    }
    lock-coordinator = {
      kubernetes_name = "lock-coordinator-sa"
      project_roles   = []
      bucket_roles    = {}
    }
    notification = {
      kubernetes_name = "notification-sa"
      project_roles   = []
      bucket_roles    = {}
    }
  }

  project_role_bindings = flatten([
    for service_name, service in local.service_accounts : [
      for role in service.project_roles : {
        key          = "${service_name}-${replace(role, "/", "-")}"
        service_name = service_name
        role         = role
      }
    ]
  ])

  bucket_role_bindings = flatten([
    for service_name, service in local.service_accounts : [
      for bucket_key, role in service.bucket_roles : {
        key          = "${service_name}-${bucket_key}-${replace(role, "/", "-")}"
        service_name = service_name
        bucket_name  = bucket_key == "patient_documents" ? var.patient_documents_bucket : var.flink_bucket
        role         = role
      }
    ]
  ])
}

resource "google_iam_workload_identity_pool" "github_actions" {
  project                   = var.project_id
  workload_identity_pool_id = "${var.name_prefix}-github"
  display_name              = "GitHub Actions"
  description               = "OIDC identities for the configured repository and branch."
  disabled                  = false
}

resource "google_iam_workload_identity_pool_provider" "github_actions" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_actions.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions"
  display_name                       = "GitHub Actions OIDC"
  description                        = "Trust only the configured repository's main branch."

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  attribute_condition = "assertion.repository == \"${var.github_owner}/${var.github_repository}\" && assertion.ref == \"refs/heads/${var.github_branch}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "github_actions" {
  project      = var.project_id
  account_id   = trimsuffix(substr("${var.name_prefix}-github-actions", 0, 30), "-")
  display_name = "GitHub Actions Artifact Registry publisher"
}

resource "google_service_account_iam_member" "github_actions_federation" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/${var.github_owner}/${var.github_repository}"
}

resource "google_artifact_registry_repository_iam_member" "github_actions_writer" {
  project    = var.project_id
  location   = split("/", var.artifact_registry_repository)[3]
  repository = split("/", var.artifact_registry_repository)[5]
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.github_actions.email}"
}

resource "google_service_account" "workloads" {
  for_each = local.service_accounts

  project      = var.project_id
  account_id   = trimsuffix(substr("${var.name_prefix}-${each.key}", 0, 30), "-")
  display_name = "GKE workload identity for ${each.key}"
}

resource "google_service_account_iam_member" "workload_identity" {
  for_each = local.service_accounts

  service_account_id = google_service_account.workloads[each.key].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.workload_identity_pool}[${var.kubernetes_namespace}/${each.value.kubernetes_name}]"
}

resource "google_project_iam_member" "workload_project_roles" {
  for_each = {
    for binding in local.project_role_bindings : binding.key => binding
  }

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.workloads[each.value.service_name].email}"
}

resource "google_storage_bucket_iam_member" "workload_bucket_roles" {
  for_each = {
    for binding in local.bucket_role_bindings : binding.key => binding
  }

  bucket = each.value.bucket_name
  role   = each.value.role
  member = "serviceAccount:${google_service_account.workloads[each.value.service_name].email}"
}
