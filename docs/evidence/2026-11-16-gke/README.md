# Cloud evidence — 2026-11-16 (GKE, Cloud Run functions, Confluent)

Captured from the live system with read-only commands, plus one synthetic demo run (pool `DEMO-C91B24`) and one
synthetic patient, admission and PDF through the public API. No credentials appear in these files.

| File | Shows | Requirement |
|---|---|---|
| `01-argocd-applications.txt` | Every Argo CD Application Synced/Healthy; the root app tracks `master` | d, ≥8 services |
| `02-cluster.txt` | 2 nodes, all pods Ready, HPAs for Allocation (2–10) and Triage (2–6), the public Ingress | c, ≥8 services |
| `03-public-url.txt` | Public Patient Registry: `/docs`, create/read patient, admission, idempotency rejection, PDF upload stored in GCS | public URL, b (REST), g |
| `04-demo-flow.txt` | Concurrent allocation (2 ALLOCATED, 1 WAITLISTED), idempotent replay, release, promotion | f, b (gRPC) |
| `05-allocation-logs.txt` | Allocation settled events for the pool | f, h (logs) |
| `06-triage-logs.txt` | Triage assessments and waitlist for the pool | f |
| `07-lock-logs.txt` | ZooKeeper-backed lock grants in order, increasing fencing tokens, queue wait times | f (S7 concept) |
| `08-notification-function.txt` | Cloud Run function `staff_notification` events; subscription DLQ and retry policy | b (serverless, Pub/Sub) |
| `09-kafka-outbox.txt` | Allocation publishing to Pub/Sub and Kafka with an empty outbox backlog; Analytics subscribed to `bed-stats` | b (Kafka), e |
| `10-firestore-indexes.txt` | Terraform-managed Firestore composite indexes READY | a, g |
| `11-hpa-load-test.txt` | k6 load test: Bed Allocation 2→5 and Triage 2→4 replicas, 100 % checks, p95 4.9 s (above the 3 s threshold) | c |
| `12-grafana-dashboard-load-test-1.png` | Grafana dashboard (last 6 h, load test at about 21:45–22:00 IST): RPS, 5xx error rate, p50/p95 latency, allocation outcomes, lock wait duration | h, c |
| `13-grafana-dashboard-load-test-2.png` | Grafana dashboard continued: outbox backlog, HPA current vs desired replicas, pod CPU and memory, Kubernetes available replicas and restarts | c, h |
| `14-demo-01` … `14-demo-06-*.png` | Demo console on GKE, pool `DEMO-ICU-1700`, step by step: all services up; 2 beds + on-shift nurse; 3 admissions (urgency 3, 2, 1); **concurrent allocate → 2 ALLOCATED, 1 WAITLISTED**; **Repeat → idempotent replay, same allocation**; **release → waitlisted admission promoted onto the freed bed** | f, b, c |
| `15-demo-console-request-log.json` | The console's request log and final state for that run | f |
| `17-grafana-lock-coordinator-after-netpol-fix.png` | Grafana after PR #29 (NetworkPolicies admit `gmp-system`): "Lock Coordinator: Acquire Outcomes" now shows granted acquires, and the Lock Coordinator appears in RPS and latency | f, h |
| `16-cloud-logging-all-services.txt` | Cloud Logging: entries from all eight services, the Cloud Run function and the Dataproc Flink job (JobManager/TaskManager, checkpoints) | h |
| `18-dataproc-cluster.jpg` | Single-node Dataproc cluster `hospital-dataproc-flink` (master `e2-standard-2`, 0 workers) in state `RUNNING` | e |
| `19-flink-dashboard.png` | Apache Flink Web UI on YARN showing job `hospital-bed-stats-1m` in state `RUNNING` with 0 canceled/failed | e |
| `20-flink-job-graph.jpg` | Flink streaming SQL execution graph: `bed_events → LocalWindowAggregate → GlobalWindowAggregate → bed_stats` | e |
| `21-flink-checkpoints.jpg` | Flink Checkpoints tab: 5/5 completed checkpoints stored in `gs://hospital-flink-514618/checkpoints/` (exactly-once mode) | e, g |
| `22-confluent-bed-events.jpg` | Confluent Cloud Managed Kafka: topic `bed-events` (3 partitions) with active consumer `flink-resource-analytics` | b (Kafka), e |
| `23-confluent-bed-stats.jpg` | Confluent Cloud Managed Kafka: results topic `bed-stats` (3 partitions) with active consumer `resource-analytics-consumer` | b (Kafka), e |
| `24-firestore-pool-stats.jpg` | Firestore collection `pool_stats`: 1-minute tumbling window metrics documents (`allocated`, `waitlisted`, `conflicts`, `contention_rate`, `average_lock_wait_ms`) persisted by Resource Analytics | e, g |
| `25-notification-real-events-logs.png` | Logs Explorer, Cloud Run function `hospital-notification-fn`, pool `DEMO-8E5B93`: five `staff_notification` entries from a real demo run (allocated, allocated, waitlisted, released, then the waitlisted admission gets the freed bed) | b (serverless, Pub/Sub) |
| `26-notification-streams-demo-pool.png` | Firestore `notification_streams/DEMO-8E5B93`: `highest_sequence: 5`, matching the five events, so no gap | b, g |
| `27-notification-handled-duplicate-rejected.png` | One synthetic event published twice to `bed-notifications` (pool `NISARG-TEST`): first `staff_notification`, then `notification_duplicate` (deduplicated on `event_id`). A malformed message logs `notification_rejected` (schema check) and is acknowledged, not retried | b |
| `28-firestore-notification-test-doc.png` | Firestore `notifications/0f0e0d0c-…-000000000001`: the stored synthetic event; the document ID is the `event_id` that deduplication checks | b, g |
| `29-firestore-notification-streams-test-pool.png` | Firestore `notification_streams/NISARG-TEST`: `highest_sequence: 1` | b, g |
| `30-firestore-notification-waitlisted-load-test.png` | Firestore `notifications` document from the k6 load test: `ALLOCATION_WAITLISTED`, pool `LOADTEST-A`, `ward: null`, `bed_id: null`, `stream_sequence: 283`, `request_id` `k6-…` | b, c, g |
| `31-notification-dlq-retry-policy.png` | The function's Eventarc Pub/Sub subscription: dead-letter topic `bed-notifications-dlq` after 10 delivery attempts, retry backoff 10–600 s | b |
| `32-lock-grants-increasing-tokens-load-test.png` | Logs Explorer, `lock_granted` for pool `LOADTEST-A` during the k6 load test: 1,482 grants; Triage and Bed Allocation take turns on one pool with increasing fencing tokens (`…391` → `…396` → `…397`) and about 5 s queue waits; `request_id` `k6-…` from the caller | f (S7 concept), c, h |
| `33-argocd-apps-synced-healthy-1.jpeg` | Argo CD UI: Bed Allocation + Triage, Bed Registry, demo UI, monitoring and Resource Analytics Applications `Healthy`/`Synced`, all tracking `master` of this repository (the load-test app shows the k6 run's failed latency threshold) | d, ≥8 services |
| `34-argocd-apps-synced-healthy-2.jpeg` | Argo CD UI continued: the root app-of-apps (`gitops/apps`) plus Lock Coordinator, Patient Registry and Staff Scheduler, all `Healthy`/`Synced` | d |
| `35-cloud-sql-patients-rows.jpeg` | Cloud SQL Studio, instance `hospital-postgres`, database `patients`: the five newest rows written by Patient Registry (a synthetic patient and k6 load-test patients) and the migrated tables | g, a |
| `36-terraform-state-list.txt` | `terraform state list` from the shared GCS backend: 161 resources across the network, GKE, Cloud SQL, Firestore, storage, IAM, notifications, Dataproc and Confluent modules | a |


ZooKeeper failure demo (leader loss, quorum loss) on the local 3-node ensemble: `../2026-11-13-local-zookeeper/`.
