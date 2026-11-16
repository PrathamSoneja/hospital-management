# Remaining work before submission

Tracker for everything still to do. Update the **Status** column as items finish (`TODO` → `DONE`). Background and
the full system description: `docs/PROJECT.md`; how to run and present: `docs/RUN-DEMO-VIVA.md`.

Last updated: 2026-11-16.

## 1. Before submission

| # | Item | Owner | Status | How / notes |
|---|---|---|---|---|
| 1 | HPA load test evidence | Pratham | DONE | `docs/evidence/2026-11-16-gke/11-hpa-load-test.txt`: scale-out shown, all checks passed; the k6 p95 threshold (3 s) was missed (4.9 s), see §3 |
| 2 | Flink job running and evidence | Jay | TODO — **job not staying up**: each Flink YARN session received SIGTERM from outside ("closed externally", exit 143) at 16:14 and 17:14 UTC. Submit with the repo script from Cloud Shell (detached), confirm RUNNING after 3+ minutes, then capture | Screenshots: Flink UI job RUNNING with checkpoints, Confluent `bed-events` and `bed-stats` messages, Firestore `pool_stats`, Dataproc logs. Guide §B.5 |
| 3 | Notification function checks | Nisarg | TAKEN — add the screenshots to `docs/evidence/2026-11-16-gke/` | Synthetic handled / duplicate / malformed events on pool `NISARG-TEST`; DLQ configuration. Guide §B.4 |
| 4 | Grafana dashboard screenshots | Pratham | DONE | `12-` and `13-grafana-dashboard-load-test-*.png`. The Lock Coordinator panel had no data because its NetworkPolicies blocked Managed Prometheus (`gmp-system`); fixed in PR #29, verified in `17-grafana-lock-coordinator-after-netpol-fix.png` |
| 4b | Demo console flow and a Cloud Logging query (all services + the Dataproc job) | Pratham | DONE | `14-demo-0*.png`, `15-demo-console-request-log.json`, `16-cloud-logging-all-services.txt` |
| 5 | Platform evidence | Ruchir | TAKEN — add the screenshots to `docs/evidence/2026-11-16-gke/` | `terraform state list`, Argo CD UI (all Synced/Healthy), Staff Scheduler data in Firestore, Cloud SQL rows. Guide §B.1 |
| 6 | Export diagrams | Anyone | TODO | Paste the Mermaid blocks of `docs/PROJECT.md` §4 into mermaid.live, export PNG |
| 7 | Update the design document | Nisarg (author) | TODO | Add the diagrams and `docs/PROJECT.md` §13 "Differences from the design document" to `cloud_proj_plan.docx` |
| 8 | Confirm `task_divison.txt` | Everyone | DONE | Member A corrected to "Ruchir Kumar" (roster and detail section); IDs unchanged |
| 9 | Viva preparation | Everyone | TODO | Each member: their two requirements and two services, `docs/RUN-DEMO-VIVA.md` Part C |
| 10 | Rehearse the demo once end to end | Everyone | TODO | `docs/RUN-DEMO-VIVA.md` Part B, about 20 minutes |
| 11 | Submit on Nalanda | Pratham | TODO | One submission per group: GitHub link, design document, `task_divison.txt` |

## 2. Before the demo day (operations)

| Item | Owner | Notes |
|---|---|---|
| Check credits | Ruchir | *Billing → Credits*: GCP and Confluent trial balance and expiry |
| Start Dataproc and the Flink job | Jay | Dataproc is stopped between sessions; resubmit the job after a restart (`RUN-DEMO-VIVA.md` §A.5) |
| Everything Synced/Healthy | Pratham | `kubectl -n argocd get applications`; sync anything OutOfSync after a master merge |
| Public URL reachable | Pratham | `http://35.244.157.132/docs` (the IP can change if the Ingress is recreated) |

## 3. Optional improvements (not required by the brief)

| Item | Notes |
|---|---|
| HTTPS for the public URL | Needs a domain, a Terraform static IP and a Google-managed certificate |
| Remove the untagged `hospital-vpc-allow-iap-ssh` firewall rule | The tagged Dataproc rule already covers IAP SSH; remove it from Terraform after checking why it was added |
| Replace the shared WARP addresses in `master_authorized_networks` | Use personal /32s, or rely only on the DNS endpoint |
| Service account display-name drift | `hospital-notification` is named by two modules and flips on each apply; harmless |
| Allocation migration identity | Give the migration Job its own service account like Patient Registry's |
| Load-test p95 | p95 was 4.9 s with 40 VUs on 2 pools (writes serialise on each pool's lock). Spreading the load over more pools (`POOL_IDS`) or fewer VUs per pool should bring it under 3 s; do not just raise the threshold |

## 4. After grading (cost)

| Item | Owner |
|---|---|
| Stop Dataproc: `gcloud dataproc clusters stop hospital-dataproc-flink --region=asia-south1` | Jay |
| Delete the Confluent cluster (Standard cluster bills hourly) | Ruchir |
| Remove the public Ingress through Git (Argo CD prunes the load balancer) | Pratham |
| Scale the GKE node pool to 0 and set Cloud SQL activation policy to NEVER | Ruchir |
| Never delete databases, buckets or the Terraform state | — |

## 5. Done (for reference)

All eight services deployed on GKE / Cloud Run through Terraform and Argo CD; public URL live; ZooKeeper locking with
fencing verified under concurrency; Pub/Sub → Cloud Run function with DLQ; Kafka enabled with Confluent and the
outbox delivering; Firestore indexes; least-privilege database roles; credential rotation and removal of leaked
files; `default` network deleted; CI with release gate and automatic image promotion; HPA scale-out demonstrated.
