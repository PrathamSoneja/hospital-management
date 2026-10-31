output "github_actions_provider_name" {
  value = google_iam_workload_identity_pool_provider.github_actions.name
}

output "github_actions_service_account_email" {
  value = google_service_account.github_actions.email
}

output "kubernetes_service_account_gsas" {
  value = {
    for service, account in google_service_account.workloads : service => account.email
  }
}
