# Demo script (about 20 minutes, four presenters)

Synthetic data only. Before you start, every Application in `kubectl -n argocd get applications` is
Synced/Healthy and Grafana shows data. Each presenter shows their two services and answers their own viva
questions.

## 0. Setup (5 minutes before)

```bash
gcloud container clusters get-credentials hospital-gke --location=asia-south1-a --project=hospital-management-514618 --dns-endpoint
```
```bash
kubectl -n hospital-system port-forward svc/demo-ui 8080:80
```
```bash
kubectl -n hospital-system port-forward svc/grafana 3000:3000
```
Open http://localhost:8080 (demo console) and http://localhost:3000 (Grafana, "Hospital overview").
Use a fresh pool ID each run, e.g. `DEMO-ICU-<time>`.

## 1. Platform: Ruchir (IaC, GitOps; Patient Registry, Staff Scheduler)

1. `infra/terraform`: modules for the VPC, GKE, Cloud SQL, Firestore, GCS, Pub/Sub, Eventarc, Cloud Run, IAM and
   Dataproc. Show `terraform state list | head` from the authoritative state.
2. Argo CD UI or `kubectl -n argocd get applications`: every service comes from Git `master`; there is no `kubectl apply`.
3. Demo console step 1, "Create beds + on-shift nurse": Staff Scheduler (Firestore) gets a nurse with a shift.
4. Step 2, "Admit synthetic patient": Patient Registry writes to Cloud SQL and returns an admission ID.
   Expected: the status strip shows all services green.
5. Public URL: `curl http://<ingress-ip>/healthz` returns `{"status":"ok"}`; any other path returns 404.

## 2. Allocation and scaling: Pratham (Bed Allocation, Triage; HPA, observability)

1. Admit 3 patients with urgency 3, 2 and 1 into a pool with 2 beds. Click "Allocate all pending concurrently".
   Expected: 2 ALLOCATED, 1 WAITLISTED, no bed used twice.
2. Click "Repeat" on an allocated row. Expected: the log shows "idempotent replay: same allocation …".
3. Waitlist panel: the waitlisted admission appears. Release one allocation. Expected: within about 1 s the
   waitlisted admission becomes ALLOCATED on the freed bed (promotion).
4. HPA (approval needed): sync `hospital-load-test`, then run `kubectl -n hospital-system get hpa -w`.
   Expected: bed-allocation and triage replicas rise above 2 under sustained load; Grafana shows RPS, p95 latency
   and "HPA current vs desired".
5. Resilience: `kubectl -n hospital-system delete pod -l app=bed-allocation-service --wait=false` during
   allocations. Expected: requests are retried or recovered; no allocation stays RECOVERING after recovery.
6. Logs: Cloud Logging query `resource.type="k8s_container" jsonPayload.event="allocation_settled"`.

## 3. Concept and events: Nisarg (Lock Coordinator + ZooKeeper; Notification)

1. `kubectl -n hospital-system get pods -l app=zookeeper` shows 3 ensemble members;
   `kubectl -n hospital-system get pods -l app=lock-coordinator` shows 2 replicas.
2. During the concurrent allocation: the Lock Coordinator logs show queued acquires with increasing fencing tokens.
   Grafana: "Lock Coordinator: acquire outcomes".
3. Notification: Cloud Run → `hospital-notification` → Logs show `staff_notification` lines for each allocation
   and release. Explain Pub/Sub → Eventarc, event-ID dedup, and the DLQ after 10 attempts.

## 4. Data and analytics: Jay (Bed Registry; Resource Analytics + Flink)

1. Bed Registry fencing: a stale token is rejected (`docs/member_d_viva_qa.md`, "STALE FENCING REJECTION").
2. Phase 2 (after Confluent): Confluent topic `bed-events` receives allocation events; the Flink job
   `hospital-bed-stats-1m` is RUNNING in the Flink UI; `bed-stats` gets one record per pool per minute; the demo
   console's analytics panel shows the windows.

## Reset after each run

Use a new pool ID; old synthetic rows are harmless. To stop the load test, run
`kubectl -n hospital-system delete job hpa-load-test` (Argo CD shows the hook as deleted; it does not re-run
until the next sync).

## Viva quick answers (everyone)

- **Why ZooKeeper?** Ordered ephemeral sequential nodes give fair queuing and automatic lease loss when a holder dies.
- **What if a lease expires mid-operation?** The fencing token makes Bed Registry and Staff Scheduler reject the stale writer.
- **Why an outbox?** No lost or phantom events: state and events commit in one transaction; delivery is at-least-once.
- **Why GitOps?** The cluster equals Git; rollbacks are Git reverts; images are digest-pinned and checked in CI.
