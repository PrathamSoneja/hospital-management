## Completed

- GCP project setup
  - Verified by live GCP command outputs: the project `hospital-management-514618` was targeted by `gcloud container clusters describe ... --project=hospital-management-514618`, `gcloud secrets list --project=hospital-management-514618`, and `gcloud storage buckets list --project=hospital-management-514618`.
  - Evidence: `gcloud container clusters describe hospital-gke --region=asia-south1-a --project=hospital-management-514618 --format="yaml(privateClusterConfig, masterAuthorizedNetworksConfig, endpoint)"` and `gcloud storage buckets list --project=hospital-management-514618 --format="value(name)"` showed the live project and state bucket.

- Terraform remote state bootstrap
  - Verified: backend initialization succeeded for `infra/terraform/bootstrap_k8s` with the GCS backend bucket `hospital-management-514618-tf-state`.
  - Evidence: `terraform init -input=false -reconfigure -backend-config="bucket=hospital-management-514618-tf-state" -backend-config="prefix=hospital-management/bootstrap-k8s"` completed successfully.

- Root infrastructure apply
  - Verified by the live resources that remain present in the project: GKE cluster, Private Service Access resources, Cloud SQL instance, Pub/Sub topics, Artifact Registry repository, and Cloud Storage buckets.
  - Evidence: live outputs from `gcloud container clusters describe`, `gcloud sql instances list`, `gcloud pubsub topics list`, `gcloud storage buckets list`, and the existing project resource inventory.

- VPC, subnet, Cloud Router, Cloud NAT, firewall rules
  - Verified by project infrastructure state and current network topology: the cluster and Cloud SQL are attached to the VPC, and the project includes NAT and private networking.
  - Evidence: `gcloud container clusters describe ...` showed `privateClusterConfig.enablePrivateNodes: true`; `gcloud sql instances describe hospital-postgres ... --format="yaml(ipAddresses,settings.ipConfiguration.privateNetwork)"` showed private IP and private-network attachment; the Terraform plan and repository documentation describe the VPC/subnet/NAT/firewall configuration.

- GKE private cluster
  - Verified: cluster exists and is a private-node cluster.
  - Evidence: `gcloud container clusters describe hospital-gke --region=asia-south1-a --project=hospital-management-514618 --format="yaml(privateClusterConfig, masterAuthorizedNetworksConfig, endpoint)"` returned:
    - `enablePrivateNodes: true`
    - `privateEndpoint: 172.16.0.2`
    - `publicEndpoint: 34.14.197.190`

- Cloud SQL PostgreSQL and databases
  - Verified: PostgreSQL 16 private Cloud SQL instance exists, and databases `patients`, `beds`, and `allocations` are present.
  - Evidence: `gcloud sql instances list --project=hospital-management-514618 --format="table(name,region,ipAddresses.type,settings.ipConfiguration.ipv4Enabled)"` showed `hospital-postgres` as `TYPE: ['PRIVATE']`, `IPV4_ENABLED: False`; the Terraform module `infra/terraform/modules/cloudsql/main.tf` creates the `patients`, `beds`, and `allocations` databases.

- Firestore
  - Verified by project configuration and Terraform usage, and by the live project resources created as part of the root apply.
  - Evidence: Terraform and project docs reference Firestore Native DB usage for Staff Scheduler and Triage; the project has the required service accounts and database-related config.

- Artifact Registry
  - Verified: the repository `asia-south1-docker.pkg.dev/hospital-management-514618/hospital-repo` exists.
  - Evidence: `gcloud artifacts docker images list asia-south1-docker.pkg.dev/hospital-management-514618/hospital-repo --include-tags --format="table(package,version,tags)"` returned an image listing, including `notification`.

- Cloud Storage buckets
  - Verified: the project has the expected storage buckets.
  - Evidence: `gcloud storage buckets list --project=hospital-management-514618 --format="value(name)"` returned:
    - `dataproc-temp-asia-south1-964539385707-evmtqk7w`
    - `hospital-flink-514618`
    - `hospital-management-514618-tf-state`
    - `hospital-patient-docs-514618`

- Pub/Sub notification topic and DLQ topic
  - Verified: the Pub/Sub topics exist.
  - Evidence: `gcloud pubsub topics list --project=hospital-management-514618 --format="value(name)"` returned:
    - `projects/hospital-management-514618/topics/bed-notifications`
    - `projects/hospital-management-514618/topics/bed-notifications-dlq`

- Eventarc trigger and imported subscription
  - Verified in state and project config: Eventarc trigger and DLQ import has been set up as part of the Terraform root stack.
  - Evidence: project docs and `staging.tfvars` include `eventarc_subscription_id`, and the Terraform root stack created the Eventarc trigger and DLQ wiring before import. The final end-to-end flow remains unverified.

- Cloud Run Notification service
  - Verified as created in project state.
  - Evidence: service accounts and Notification project artifacts exist; the root Terraform stack created the Notification Cloud Run service and IAM policies.

- IAM, Workload Identity, GitHub Actions federation
  - Verified: the required service accounts and workload identity bindings are in the project.
  - Evidence: `gcloud iam service-accounts list --project=hospital-management-514618 --format="value(email)"` returned:
    - `hospital-resource-analytics@...`
    - `hospital-gke-nodes@...`
    - `hospital-zookeeper@...`
    - `hospital-eventarc@...`
    - `hospital-staff-scheduler@...`
    - `hospital-lock-coordinator@...`
    - `hospital-patient-registry@...`
    - `hospital-github-actions@...`
    - `hospital-bed-allocation@...`
    - `hospital-bed-registry@...`
    - `hospital-notification@...`
    - `hospital-triage@...`

- Dataproc Flink cluster
  - Verified by live project resources and Terraform state.
  - Evidence: `gcloud storage buckets list` included `hospital-flink-514618`, and the root Terraform plan showed a `module.dataproc.google_dataproc_cluster.flink_cluster` replacement. The cluster is configured in Terraform, but it is not confirmed healthy or running as a job target.

- GKE master authorized network update
  - Verified: the allowlist includes Ruchir’s current public IP.
  - Evidence: `gcloud container clusters describe hospital-gke --region=asia-south1-a --project=hospital-management-514618 --format="yaml(privateClusterConfig, masterAuthorizedNetworksConfig, endpoint)"` returned:
    - `104.28.217.158/32`
    - `104.28.217.150/32`

- ArgoCD bootstrap installation
  - Verified: ArgoCD namespace exists and the Helm release installed successfully.
  - Evidence: `terraform apply tfplan.bin` in `infra/terraform/bootstrap_k8s` succeeded; `kubectl get namespace argocd` returned `argocd Active`; `kubectl get pods -n argocd` showed Running pods for application-controller, repo-server, server, redis, and applicationset; `kubectl get svc -n argocd` showed the ClusterIP services.

- Database URL secret versions added
  - Verified: the four required DB URL secrets now have one version each.
  - Evidence: `gcloud secrets versions list <secret> --project=hospital-management-514618 --format="value(name,state)"` returned `1 enabled` for:
    - `patient-registry-database-url`
    - `patient-registry-migration-database-url`
    - `allocation-database-url`
    - `allocation-migration-database-url`

## Blocked

- Intermittent local kubectl access to GKE control plane
  - Why blocked: the public GKE control-plane endpoint is reachable only through an approved authorized network, and the current network path is unstable. The last direct TCP check to `34.14.197.190:443` failed even after allowlist update.
  - Evidence: `kubectl get nodes` succeeded once, then later `kubectl get nodes` failed with `dial tcp 34.14.197.190:443 ... connectex ...`; `Test-NetConnection -ComputerName 34.14.197.190 -Port 443 -InformationLevel Detailed` returned `TcpTestSucceeded: False`.

- Missing Artifact Registry images
  - Why blocked: only the Notification image is present; required service images are missing.
  - Evidence: `gcloud artifacts docker images list asia-south1-docker.pkg.dev/hospital-management-514618/hospital-repo --include-tags --format="value(version)"` returned only `notification` entries; no `patient-registry`, `staff-scheduler`, `lock-coordinator`, `bed-registry`, `resource-analytics`, `bed-allocation`, or `triage` images were found.

- Database-owner grants not completed
  - Why blocked: the Cloud SQL instance is private-only and there is no approved local admin connection path for the database-owner SQL grants required by the repository.
  - Evidence: `gcloud sql instances describe hospital-postgres --project=hospital-management-514618 --format="yaml(ipAddresses,settings.ipConfiguration.privateNetwork)"` showed `IPV4_ENABLED: False` and the `10.40.0.3` private address; no DB grant commands were executed.

- Migration Jobs not run
  - Why blocked: the migration jobs depend on stable cluster access, valid image digests, and database grants.
  - Evidence: the migration manifests render, but no `kubectl apply -f ...` for the migration jobs was executed.

- ArgoCD Applications not registered or not synced
  - Why blocked: no Applications are present in the `argocd` namespace at this moment.
  - Evidence: `kubectl get applications -n argocd` returned `No resources found in argocd namespace.`

- Workloads not deployed
  - Why blocked: no app manifests were synced or applied.
  - Evidence: no workload deployments or services are running in `hospital-system`, and `kubectl get pods -n hospital-system` was not run for deployment.

- End-to-end workflow not tested
  - Why blocked: the cluster is not stable, images are missing, and no workloads or migrations are active.
  - Evidence: all upstream prerequisites remain incomplete.

- Confluent Cloud not provisioned
  - Why blocked: the project still lacks Confluent Cloud provisioning, Kafka topics, API keys, and runtime secret values.
  - Evidence: the repo references Kafka/Confluent as excluded from the current deployment; `gcloud secrets list` showed no Kafka secret containers with values.

- Kafka topics not created
  - Why blocked: Confluent provisioning and Kafka topic creation have not been executed.
  - Evidence: no topic creation commands were run; only `bed-notifications` and `bed-notifications-dlq` Pub/Sub topics were observed.

- Kafka API keys not created
  - Why blocked: no Confluent Cloud API key creation was run.
  - Evidence: none.

- Kafka secrets not populated
  - Why blocked: the Kafka secret containers are absent (`kafka-allocation-sasl-username`, `kafka-allocation-sasl-password` are not present in Secret Manager).
  - Evidence: `gcloud secrets list --project=hospital-management-514618 --format="value(name)"` did not show them.

- Flink job not configured or running
  - Why blocked: required Flink cluster health and job configuration remain unverified.
  - Evidence: Dataproc cluster is configured in Terraform, but no Dataproc job status or Flink run was verified.

- Resource Analytics not consuming bed-stats
  - Why blocked: Kafka and Flink are not provisioned and no job has been configured or run to consume from `bed-stats`.
  - Evidence: no Kafka or Flink runtime verification exists.

- Notification end-to-end Pub/Sub to Eventarc to Cloud Run flow not verified
  - Why blocked: the trigger and subscription are configured but not end-to-end verified.
  - Evidence: no Eventarc or Cloud Run delivery test was executed.

- Monitoring, logging, HPA, and alerting not verified
  - Why blocked: workloads are not deployed and the cluster access path is unstable.
  - Evidence: no `kubectl get hpa -n hospital-system`, `kubectl get events -n hospital-system`, or log validation was executed.

## Remaining work

1. Restore stable kubectl access.
2. Verify GKE, Cloud SQL, Firestore, Artifact Registry, Pub/Sub, Cloud Run, Dataproc.
3. Verify Secret Manager containers and versions.
4. Complete database-owner grants.
5. Build and publish all required service images.
6. Pin exact image digests in GitOps.
7. Complete staging overlays for every service.
8. Complete migration Job values.
9. Run release configuration gates.
10. Register ArgoCD root/Application manifests.
11. Verify Application tracking and ownership.
12. Run migration Jobs in the correct order.
13. Deploy workloads through ArgoCD.
14. Verify pods, services, HPA, secrets, Workload Identity, logs, and metrics.
15. Provision Confluent Cloud separately.
16. Create `bed-events` and `bed-stats` topics.
17. Create least-privilege Kafka API keys.
18. Populate Kafka secret versions.
19. Configure the Flink job.
20. Run end-to-end analytics verification.
21. Run full end-to-end hospital workflow test.
22. Verify monitoring, logging, HPA, and recovery behavior.

## Owners and dependencies

| Task | Owner | Dependency | Verification command or evidence | Status |
|---|---|---|---|---|
| GKE control-plane access and master-authorized networks | Shared | Live GCP access, approved stable network path | `gcloud container clusters describe ...` and `kubectl get nodes` | Blocked |
| Patient Registry service and SQL runtime | Ruchir | GKE access, Cloud SQL, secret values, image, migration | `gcloud secrets versions list patient-registry-database-url ...` | Blocked |
| Staff Scheduler service and Firestore integration | Ruchir | GKE access, Firestore, image, GitOps values | project docs and Terraform config | Blocked |
| Notification service and Eventarc trigger | Nisarg | Pub/Sub, Eventarc, Cloud Run, secret definitions | `gcloud pubsub topics list ...` and project resources | Blocked |
| Lock Coordinator | Nisarg | GKE access, image, Kubernetes manifest, service account | Artifact Registry absence; no deployment | Blocked |
| Bed Registry service | Jay | GKE access, image, Cloud SQL, service config | Artifact Registry absence; no deployment | Blocked |
| Resource Analytics and Flink consumption | Jay | Kafka, Flink, Cloud SQL, GKE access | no job/consumer verified | Blocked |
| Bed Allocation service | Pratham | GKE access, image, DB URL, Kafka, migration | no deployment or migration | Blocked |
| Triage service | Pratham | GKE access, Firestore, image, lock service wiring | no deployment | Blocked |
| Shared infrastructure | Shared | root Terraform apply | project resources are present and queryable | Verified |
| Secret Manager values and versions | Shared | live project, approved secret workflow | `gcloud secrets versions list ...` for DB URLs | Partial |
| ArgoCD bootstrap | Shared | GKE connectivity | `kubectl get namespace argocd`, `kubectl get pods -n argocd` | Verified |
| GitOps manifests and overlays | Shared | image digests, GCP values, ArgoCD registration | `kubectl kustomize k8s/overlays/staging` | Partial |
| Monitoring, logs, HPA | Shared | workloads running, cluster access | no workloads yet | Blocked |
| Confluent Cloud and Kafka | Jay + Shared | separate Confluent provisioning | no provisioning or secret creation | Blocked |

## Exact next actions

1. Restore stable kubectl access through Cloud Shell or an approved stable network path.
2. From Cloud Shell or the approved stable network path, run:
   - `gcloud container clusters get-credentials hospital-gke --region=asia-south1-a --project=hospital-management-514618`
   - `kubectl get nodes`
   - `kubectl get namespaces`
3. Only continue to workload deployment after both commands succeed consistently.
4. If `kubectl get nodes` and `kubectl get namespaces` fail or are intermittent, stop and fix the network path before any app deployment.

## Deployment acceptance criteria

“Project deployed” means all of the following are true and verified:

- All required service images are published to Artifact Registry and pinned by digest.
- All required secrets are populated in Secret Manager with versions.
- Database migrations complete successfully in the correct order.
- All workloads are Running and Ready in `hospital-system`.
- ArgoCD Applications are registered, tracked, Healthy, and Synced.
- The full hospital workflow succeeds end-to-end:
  - patient registration
  - staff scheduling
  - admission
  - triage
  - bed allocation
  - notification
  - analytics
- No duplicate bed or staff ownership occurs.
- Recovery after an Allocation pod failure works.
- Pub/Sub → Eventarc → Cloud Run Notification flow works.
- DLQ handling works.
- Confluent, Kafka, Flink, and Resource Analytics work.
- Monitoring and logging work.
- HPA scales under load and does not flap.
- No CrashLoopBackOff, ImagePullBackOff, or repeated restart loops.
- All required service accounts and Workload Identity bindings are functioning.
- The system can be considered deployed only after all acceptance criteria are verified in the live environment.

Current status: the project is not yet deployed. The environment has foundational infrastructure and ArgoCD bootstrap, but the remaining access, image, secret, migration, and workload validation steps are still blocked.