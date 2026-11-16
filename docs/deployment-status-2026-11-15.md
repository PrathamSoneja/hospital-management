# Live deployment report — hospital-management on GCP (Confluent excluded) — 2026-11-15

Project `hospital-management-514618` · cluster `hospital-gke` (zonal, `--location=asia-south1-a`) · namespace
`hospital-system`. **Snapshot of 2026-11-15.** The release changes described here were merged to `master` in
PR #18 (`ff5cdc9`). At the time of this snapshot no Argo CD Application was registered, so **no GKE service was
deployed**. Current steps: `docs/COMPLETION-GUIDE.md`.

Labels: CLOUD-VERIFIED / LOCAL-VERIFIED / MOCK-TESTED / EMULATOR-TESTED / BLOCKED / DEFERRED / UNVERIFIED.

## 1. Services live

| Service | Owner | Argo CD Application | Pods | Image digest (built + pushed, not deployed) | Storage | Status |
|---|---|---|---|---|---|---|
| Patient Registry | Ruchir | `patient-registry` (auto-sync) | 0 | `patient-registry@sha256:194028b1…6d` | Cloud SQL `patients`, GCS `hospital-patient-docs-514618` | BLOCKED (Git handoff) |
| Staff Scheduler | Ruchir | `staff-scheduler` (auto-sync) | 0 | `staff-scheduler@sha256:688201fb…19` | Firestore | BLOCKED (Git handoff) |
| Lock Coordinator + ZooKeeper | Nisarg | `lock-coordinator` (auto-sync) | 0 | `lock-coordinator@sha256:744eb940…5b` | ZooKeeper PVCs | BLOCKED (Git handoff) |
| Bed Registry | Jay | `hospital-bed-registry-staging` (manual) | 0 | `bed-registry@sha256:4a5c7ca0…31` | Cloud SQL `beds` | BLOCKED (Terraform apply + Git handoff) |
| Triage | Pratham | `hospital-allocation-triage-staging` (manual) | 0 | `triage@sha256:cace6664…da` | Firestore | BLOCKED (Git handoff) |
| Bed Allocation | Pratham | same + `hospital-allocation-migrations-staging` | 0 | `bed-allocation@sha256:9d0bcb65…3e` | Cloud SQL `allocations`, Pub/Sub | BLOCKED (Git handoff; needs Bed Registry) |
| Resource Analytics (REST only) | Jay | `hospital-resource-analytics-staging` (manual) | 0 | `resource-analytics@sha256:e50d0d98…95` | Firestore | DEFERRED consumption; REST BLOCKED (Git handoff) |
| Notification | Nisarg | — (Cloud Run, Terraform) | Cloud Run Ready | `notification:initial` | — | PARTIAL: service Ready (CLOUD-VERIFIED), end-to-end UNVERIFIED |
| Grafana + GMP frontend | Pratham | `hospital-monitoring` (manual, new) | 0 | `grafana/grafana:11.6.0`, `gke.gcr.io/prometheus-engine/frontend:v0.15.3-gke.0` (tags verified to exist) | — | BLOCKED (Terraform apply + Git handoff) |

Full digests: `../hm-release-images.txt` (bed-registry rebuilt as tag `42fb8e6-local2` →
`sha256:4a5c7ca03726af5955acf9ba1b0f80c31dca8027e5c5da81eb26da5e468e3c31`; the `42fb8e6-local1` bed-registry image
`sha256:883dcb41…` is superseded). Public URL: none yet (Ingress prepared, §5). Argo CD: 6 pods Running,
**0 Applications**, only the `default` AppProject (CLOUD-VERIFIED, `kubectl -n argocd get applications,appprojects`).

## 2. Service linkage map (as configured; none verified live)

```
client ─HTTP(public, GET /healthz only)─▶ Ingress patient-registry-public ─▶ patient-registry-service:8000
Bed Allocation (REST :8000, Service bed-allocation-service)
  ├─REST──▶ Patient Registry  http://patient-registry-service:8000/api/v1   (admission lookup)
  ├─gRPC──▶ Triage             triage-service:50051                         (urgency, waitlist)
  ├─gRPC──▶ Lock Coordinator   lock-coordinator-service:50051 ─▶ ZooKeeper (3-node StatefulSet, leases + fencing)
  ├─gRPC──▶ Bed Registry       bed-registry-service:50053                   (allocate/release with fencing token)
  ├─gRPC──▶ Staff Scheduler    staff-scheduler-service:50051                (staff assignment)
  └─outbox (Postgres) ─▶ Pub/Sub bed-notifications ─▶ Eventarc hospital-notification-pubsub ─▶ Cloud Run
                                hospital-notification (DLQ: bed-notifications-dlq, 10 attempts)
       └─ Kafka bed-events rows: written, kept PENDING (DEFERRED, OUTBOX_DESTINATIONS=pubsub)
```

| Store | Product | Owner service | Use |
|---|---|---|---|
| `allocations` | Cloud SQL Postgres 16 (private IP) | Bed Allocation | allocations, outbox, stream counters (Alembic `20261031_04`) |
| `beds` | Cloud SQL | Bed Registry | beds, pool fencing tokens, operation log (`services.bed_registry.migrate`) |
| `patients` | Cloud SQL | Patient Registry | patients, admissions, document metadata |
| `(default)` | Firestore Native | Triage, Staff Scheduler, Analytics | triage records + `waitlist_admission_index`, staff shifts, `pool_stats` |
| `hospital-patient-docs-514618` | GCS | Patient Registry | patient document uploads |
| `hospital-flink-514618` | GCS | Flink (deferred) | job artifacts/checkpoints |

| Secret (Secret Manager) | Consumer (GSA) | Versions |
|---|---|---|
| `allocation-database-url`, `allocation-migration-database-url` | hospital-bed-allocation | 1 enabled each |
| `patient-registry-database-url` / `-migration-` | hospital-patient-registry / hospital-pr-migrator | 1 enabled each |
| `bed-registry-database-url` / `-migration-` | hospital-bed-registry / hospital-br-migrator | **not created** (Terraform, pending apply) |
| `kafka-allocation-sasl-*`, `confluent-api-*` | — | DEFERRED (Kafka) |

Workload Identity: 9 existing GSAs bound to `hospital-system/<ksa>` (CLOUD-VERIFIED). Pending in Terraform:
`hospital-br-migrator` (cloudsql.client + accessor on the migration secret) and `hospital-gmp-frontend`
(monitoring.viewer).

## 3. Verified integrations

| Integration | Evidence | Label |
|---|---|---|
| GKE API via IAM-gated DNS endpoint | `kubectl get nodes` through `gke-…asia-south1-a.gke.goog` after `--enable-dns-access` | CLOUD-VERIFIED |
| Images exist in Artifact Registry | `gcloud artifacts docker images list … --include-tags` (8 tags) | CLOUD-VERIFIED |
| Full required-integration suite (this diff) | `python -m tests.sandbox.cli verify` (sandbox Postgres, Firestore/Pub/Sub emulators, local ZooKeeper): 267 passed, 1 failed (`test_resolved_copy_passes_and_tags_are_rejected`: the test copied only `k8s/`, not the newly gated `monitoring/`); test fixed, file re-run 3/3 passed. Full suite not re-run after that one-line test fix | LOCAL/EMULATOR-TESTED |
| Bed Registry migration + least privilege | `scratchpad/check_bed_migration.py` on sandbox Postgres 16: migrate exit 0, idempotent re-run, runtime insert/select OK, runtime `CREATE TABLE` → InsufficientPrivilege | LOCAL-VERIFIED |
| Release gate (all 8 Argo CD paths) | `python scripts/check_release_config.py` → "no placeholders, digest-pinned images" | LOCAL-VERIFIED |
| Every live workflow check (admission → allocation, waitlist/promotion, Pub/Sub → Eventarc → Notification, DLQ, idempotency, concurrency, pod-kill recovery, HPA, dashboards, logs, public URL) | none run | BLOCKED |

## 4. Deferred Confluent path

Not deployed: Confluent Cloud, Kafka topics `bed-events`/`bed-stats`, API keys, Kafka secret versions, Flink job,
Analytics Kafka consumer. Allocation runs with `OUTBOX_DESTINATIONS=pubsub`: every business event still writes a
`kafka`/`bed-events` outbox row (stable event ID, per-pool sequence) that stays `PENDING` with attempt count 0 and
is counted in `outbox_pending_events{destination="kafka"}`; it is never marked delivered. To enable later:
provision Confluent (uncomment `module "confluent"` + outputs), add Kafka secret versions, set
`OUTBOX_DESTINATIONS=pubsub,kafka` and the `KAFKA_*` values, restore the two Kafka SecretProviderClass entries,
drop the staging patch removing `KAFKA_SASL_*_FILE`, add `KAFKA_BOOTSTRAP_SERVERS` to the Analytics overlay,
recalculate CPU quota, start Dataproc, submit `services/resource_analytics/flink_job.sql`. Backlogged events are
late for event-time windows: allow lateness or resolve them by operator decision.

## 5. Outstanding blockers

**Hard blockers**
1. *Terraform state.* `gs://hospital-management-514618-tf-state/default.tfstate` was last written 2026-11-14 05:49 UTC,
   but Terraform (1.16) re-created Dataproc at 18:27 the same day: the authoritative root state is elsewhere
   (likely Ruchir's machine). A read-only plan against the GCS copy (`-lock=false`, no plan file) showed
   unrelated changes, so it was stopped there (see §6 of the plan review below).
2. *Git.* Argo CD tracks `master`; the reviewed diff must be committed and merged by the team.

**Approval-required, in order**
1. Ruchir: `terraform plan` on the authoritative state with this diff; expected **in-scope** result only:
   +2 `google_sql_user` (bed_registry, bed_registry_migrator), +2 secrets, +2 write-only secret versions,
   +2 GSAs (br-migrator, gmp-frontend) with WI bindings and project roles, +2 secret accessors (14 adds);
   ~ `google_container_cluster.main` monitoring packages DEPLOYMENT/HPA/POD/CADVISOR (DNS endpoint shows no diff:
   the gcloud change is already represented); ~ node pool `max_node_count` 4 → 2. Anything else (e.g. Dataproc
   replacement from `service_account_scopes`, Cloud Run `scaling`, notification SA display name, Eventarc
   subscription create) is drift to be resolved separately, not applied with this change.
   Passwords: generate in the shell (`TF_VAR_bed_registry_db_password`, `TF_VAR_bed_registry_migrator_db_password`),
   ephemeral + write-only → not stored in plan or state; do not echo or write them to files.
2. Database owner (`postgres`), once, after apply — least privilege for the runtime role:
   `REVOKE cloudsqlsuperuser FROM bed_registry; ALTER ROLE bed_registry NOCREATEDB NOCREATEROLE;`
   (run in Cloud SQL Studio or an approved one-off admin Job; the instance has no public IP).
3. Team: review + merge `../hm-release` to `master` (and delete the temp files there).
4. Register the root Application (`gitops/bootstrap/root-application.yaml`; it and three child apps auto-sync
   with prune), then sync the manual apps in order: lock-coordinator → patient-registry, staff-scheduler →
   bed-registry (migration hook first) → allocation-migrations → allocation-triage → monitoring → analytics.
5. Test events to Pub/Sub; approved k6 load for HPA.

**Assignment gaps:** Kafka → Flink → results topic (DEFERRED); Notification is a Cloud Run service, not a function
(confirm acceptability); public URL limited to `GET /healthz` until HTTPS + auth; credential exposure in
`42fb8e6` unresolved (rotation not approved); `task_divison.txt` lists "Ruchi Kumar" (confirm spelling).

**Cost/capacity:** global vCPU quota 12 (now 4 used, Dataproc stopped). Planned 2 × e2-standard-4: requests at
minimum replicas 2.9 vCPU apps + ~0.9 system/Argo CD + ~0.2 monitoring/migrations of 7.84 allocatable (≈51 %);
at both HPA maxima ≈ 6.3 + 1.1 = 7.4 (≈94 % of requests — requested CPU, not measured utilisation; leaves little
rollout headroom during the HPA test). Approximate monthly list cost while running (asia-south1, ±15 %):
2 nodes ≈ $235–250, node disks ≈ $22, Cloud SQL 2 vCPU/7.5 GB ≈ $110–125, Cloud NAT ≈ $32+, Ingress forwarding
rule ≈ $18–20, stopped Dataproc disks (150 GB pd-standard) ≈ $6, Managed Prometheus ≈ $5–20, GKE zonal cluster
fee covered by the free tier ⇒ **≈ $430–500/month ≈ $14–17/day**. Remaining credits are not visible to this
account (no billing-account permission): read *Billing → Credits* (remaining value, expiry) and
*Billing → Reports* (month-to-date) and compare against these daily figures.

## 6. Rollback and cleanup

- Argo CD apps: *History and rollback* or revert the merge commit; images are digest-pinned.
- Bed Registry DB: drop tables via migrator, then `terraform` remove the two users/secrets (approval).
- Public Ingress: delete `public-ingress.yaml` from Git → Argo CD prunes the load balancer (ask first).
- After the demo, with approval: scale the node pool to 0, set Cloud SQL activation policy NEVER, keep Dataproc stopped.
- Never delete without approval: Cloud SQL instance/databases, buckets, Firestore, state bucket, Artifact Registry.

## 7. Cloud writes performed today (all approved)

1. `gcloud config set project hospital-management-514618` (local config).
2. `gcloud dataproc clusters stop hospital-dataproc-flink --region=asia-south1` → STOPPED; disks/buckets kept.
3. `gcloud container clusters update hospital-gke --location=asia-south1-a --enable-dns-access` (also in Terraform).
4. `gcloud components install gke-gcloud-auth-plugin` (local tool); `gcloud auth configure-docker asia-south1-docker.pkg.dev`.
5. Pushed 8 images to `hospital-repo` (7 × `42fb8e6-local1`, bed-registry `42fb8e6-local2`).
No Terraform apply, no state change, no Kubernetes/Argo CD write, no SQL change, no secret write, no credential rotation.
