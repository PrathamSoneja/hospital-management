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

Still to add here (screenshots): Flink job and `bed-stats` records (Jay); Nisarg's and Ruchir's screenshots (taken,
not yet committed); the demo console flow and a Cloud Logging query (Pratham).
