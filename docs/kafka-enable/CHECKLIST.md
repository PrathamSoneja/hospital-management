# Kafka → Flink → results topic (requirement e) — enable checklist

Owners: **Jay** (Flink, Analytics), **Ruchir** (Confluent provisioning, Terraform). Mandatory for submission.
Nothing here is active until the steps below are approved and run. Today's Pub/Sub-only deployment keeps
working independently: Allocation writes `bed-events` outbox rows that stay `PENDING`.

Prepared in this branch:

| Item | Where | State |
|---|---|---|
| Flink SQL job, fixed | `services/resource_analytics/flink_job.sql` | LOCAL-REVIEWED (not run on Flink) |
| Secure submit script | `services/resource_analytics/submit_flink_job.sh` | `bash -n` OK; not run |
| Single-node Dataproc (2 vCPU) | `infra/terraform/modules/dataproc` (`worker_count = 0`) | `terraform validate` OK; replaces the stopped cluster |
| Enable patch (Terraform + GitOps) | `docs/kafka-enable/kafka-enable.patch` | applies cleanly; patched tree validates and renders |

Fixes made to the Flink job (each would have broken requirement e):
1. `event_time` is Python `isoformat()` (`…T12:00:00.123456+00:00`); Flink's JSON `TIMESTAMP(3)` parser rejects it and
   `json.ignore-parse-errors = true` silently dropped **every** event. It is now read as a string and converted
   to an event-time column (`event_ts`, watermark 5 s).
2. With the `flink-sql-connector-kafka` uber-jar the JAAS class must be the shaded
   `org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule`.
3. Allocation never emits `ALLOCATION_REQUESTED` or `ALLOCATION_CONFLICT`, so `requests`, `conflicts` and
   `contention_rate` were always 0. They are now derived from `BED_ALLOCATED` / `ALLOCATION_WAITLISTED` and the
   emitted `lock_contended` flag.
4. Deduplication uses `COUNT(DISTINCT event_id / allocation_id)` inside the window aggregation, which stays
   append-only for the Kafka sink (the earlier `ROW_NUMBER` view could not feed a window TVF reliably).
5. The submit script used a wrong project and bucket and uploaded the SQL **with credentials** to GCS. It now reads
   them from Secret Manager into memory and pipes the rendered SQL over IAP SSH into a 0600 temp file that is shredded.

## Day-2 steps (each needs approval)

1. **Capacity check.** Global vCPU quota 12: 2 GKE nodes (8) + single-node Dataproc (2) = 10. Never start the old
   3-node cluster next to 2 GKE nodes (14 > 12).
2. **Confluent account (Ruchir).** Create the Confluent Cloud organisation (card details entered by the account
   owner, not by Claude), a Cloud API key for Terraform, and set in Ruchir's local `staging.tfvars`
   (never in Git): `confluent_cloud_api_key`, `confluent_cloud_api_secret` (or `TF_VAR_*`),
   `confluent_region = "asia-south1"` (same region as GKE; the default `us-central1` adds latency), and
   `flink_operator_members = ["user:<jay's account>"]`.
3. **Apply the patch** on a branch: `git apply -p1 docs/kafka-enable/kafka-enable.patch`. It
   - enables `module "confluent"` and its outputs (environment, Basic cluster, topics `bed-events` and
     `bed-stats` with 3 partitions, per-component service accounts and API keys);
   - writes the Allocation, Analytics and Flink credentials into Secret Manager
     (`kafka-allocation-sasl-*` exist; adds `kafka-analytics-sasl-*`, `kafka-flink-sasl-*`). The values are
     stored in Terraform state because Confluent API secrets are already state attributes; the state bucket
     must stay restricted;
   - grants the Analytics GSA access to its own two secrets only, and the Flink operators to the Flink secrets;
   - adds firewall `hospital-vpc-allow-iap-ssh-dataproc` (35.235.240.0/20 → tcp:22, tag `dataproc`);
   - sets `OUTBOX_DESTINATIONS=pubsub,kafka` and the Kafka settings for Allocation, restores its two Kafka secret
     mounts, and gives Resource Analytics `KAFKA_BOOTSTRAP_SERVERS` plus its own credentials.
4. **Bootstrap address.** After `terraform apply`, take `terraform output confluent_bootstrap_endpoint`, strip
   `SASL_SSL://`, and replace `PLACEHOLDER_KAFKA_BOOTSTRAP` in `k8s/overlays/staging/allocation.properties` and
   `k8s/resource-analytics/overlays/gke/kustomization.yaml`. The release gate (and CI) blocks until that is done.
5. **Terraform (Ruchir).** `terraform plan -var-file staging.tfvars -out tfplan.bin`; expect Confluent resources,
   4 secrets + 6 versions, 4 secret IAM members, 1 firewall, and the Dataproc replacement (2 → 0 workers).
   Review, then apply.
6. **Connector jar (Jay).** Start Dataproc (`gcloud dataproc clusters start hospital-dataproc-flink --region=asia-south1`),
   read the Flink version on the master (`flink --version` via IAP SSH), stage the matching official
   `flink-sql-connector-kafka-<version>.jar` from Maven Central in `gs://hospital-flink-514618/jars/`, and
   record its SHA-1 against Maven Central's published checksum.
7. **Merge and sync.** Merge the branch to `master`; ArgoCD syncs Allocation (outbox now delivers the backlog in
   order), then sync `hospital-resource-analytics-staging`.
8. **Submit Flink (Jay).**
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=<host:9092> FLINK_KAFKA_CONNECTOR_JAR=gs://hospital-flink-514618/jars/<jar> \
     bash services/resource_analytics/submit_flink_job.sh
   ```

## Verification (all must be CLOUD-VERIFIED for requirement e)

| Check | Command / evidence | Expected |
|---|---|---|
| Kafka backlog drains | Grafana panel "Transactional Outbox Backlog" | `kafka` backlog falls to 0; `pubsub` unaffected |
| Events in `bed-events` | Confluent Cloud UI → topic → Messages | JSON matching `contracts/events/bed-event.schema.json` |
| Flink job running | Dataproc → component gateway → Flink UI | `hospital-bed-stats-1m` RUNNING, checkpoints completing in `gs://hospital-flink-514618/checkpoints` |
| Windowed results | Confluent UI → `bed-stats` | one record per active pool per minute, keyed by `pool_id` |
| Analytics consumes | `GET /api/v1/analytics/summary` (demo UI panel) | per-pool windows; Firestore `pool_stats` documents |
| Logs | Cloud Logging: `resource.type="cloud_dataproc_cluster"` and the Analytics pod | job and consumer logs present |

Backlogged events from the Pub/Sub-only period arrive late for their original windows. The watermark drops them,
unless an allowed-lateness change is made later. Record this as a known limitation, or resolve those rows by
operator decision before enabling.

Rollback: set `OUTBOX_DESTINATIONS=pubsub` again (events stay pending, nothing is lost), cancel the Flink job
(Flink UI or `yarn application -kill`), stop Dataproc. Destroying Confluent resources needs separate approval.
