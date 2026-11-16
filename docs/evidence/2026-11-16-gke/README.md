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

Still to add (screenshots): Flink job and `bed-stats` records (Jay), notification synthetic checks (Nisarg), HPA
scale-out and Grafana dashboards (Pratham), `terraform state list` and the Argo CD UI (Ruchir).
