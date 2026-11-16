# Completion guide: from this branch to a submission-ready, demo-ready project

Branch `release/gcp-only` (PR #20, which includes `master` up to PR #19; contents in §5). Nothing in this branch is
deployed until it is merged to `master` and synced by Argo CD. Detail lives in the linked documents; this page is
the order of work.

## 1. Requirement status (brief CS/SS G527, scenario S7)

| Req | What the brief needs | Status today | What closes it |
|---|---|---|---|
| ≥8 services | 8 microservices | Code + images ready (8 + demo UI); only Notification live | Steps B–E |
| Public URL | a web service by public URL | Patient Registry: Ingress routes `/api/v1`, `/docs`, `/healthz`; not live | Step E; optional HTTPS: step H |
| a | all infra via IaC | Applied by Ruchir; this branch adds Bed Registry DB, monitoring IAM, node cap, single-node Dataproc | Step B |
| b | serverless async function | Cloud Run function `hospital-notification-fn` (PR #19) on Pub/Sub via Eventarc; not applied | Steps B, I |
| b | REST + gRPC + MQ | REST/gRPC/Pub/Sub verified locally; Kafka phase 2 | Steps E, G |
| c | GKE stateless + 2 HPAs + load test | HPAs, in-cluster k6 Job ready (seeded run passed locally) | Steps E, F |
| d | GitOps only | Argo CD installed; 0 Applications | Steps D–E |
| e | Flink on Dataproc, Kafka in → window → results topic | Job fixed, patch applied on `feature/kafka-confluent`; **Confluent free trial today** | Step G |
| f | ZooKeeper concept implemented + demoed | Verified locally with the real Lock Coordinator | Steps E–F |
| g | 3 storage products, real use | Cloud SQL, Firestore, GCS provisioned; use verified locally | Step F |
| h | Prometheus/Grafana dashboards, central logs incl. analytics | GMP + Grafana manifests ready; packages in Terraform | Steps B, E, F |
| Deliverables | design doc, diagrams, code, IaC, manifests, `task_divison.txt` | Design doc exists + as-built supplement; `task_divison.txt` present | Step J |

## 2. Steps in order

**A. Pratham — push `release/gcp-only` (PR #20).** Master is merged in, the CI conflict is resolved, the public
Ingress is widened and `temp_*.txt` are removed (§5).
CI should now pass the "Render, schema-validate and release-gate every Argo CD path" step, which passed locally.
Docs under `docs/` are tracked normally now (the blanket `docs/*.md` ignore rule was removed).

**A2. Repository admin — enable CI image publishing (once).** In GitHub, open *Settings → Secrets and variables →
Actions → Variables* and add:
- `GCP_WIF_PROVIDER` = `projects/964539385707/locations/global/workloadIdentityPools/hospital-github/providers/github-actions`
- `GCP_WIF_SERVICE_ACCOUNT` = `hospital-github-actions@hospital-management-514618.iam.gserviceaccount.com`

These are identifiers, not secrets: Workload Identity Federation needs no key, and the provider trusts only this
repository's `master`. Until both are set, `publish-gke-images` is skipped, not failed. Once set, every merge to
`master` builds all 8 GitOps images from that commit, pins their digests, runs the release gate, and commits
`chore(gitops): promote images …` to `master`.

That bot commit uses `GITHUB_TOKEN`, so it starts no new run. If `master` is branch-protected, allow
`github-actions[bot]` to push, or the commit step fails. The three auto-sync Applications (Lock Coordinator,
Patient Registry, Staff Scheduler) then roll out automatically; the others show OutOfSync until synced.

**B. Ruchir — Terraform from his laptop (authoritative state).** First do §3 S5 (confirm and consolidate the
state), and run the password rotation §3 S2 in the same plan/apply. In PowerShell:
```powershell
$env:TF_VAR_bed_registry_db_password = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$env:TF_VAR_bed_registry_migrator_db_password = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
cd infra\terraform; terraform plan -var-file staging.tfvars -out tfplan.bin
```
Expected:
- **Adds:** 2 SQL users, 2 secrets + 2 write-only versions, 2 service accounts with Workload Identity and
  roles, 2 secret accessors.
- **Changes:** GKE monitoring packages; node pool max 4 → 2.
- **Replacement:** the stopped Dataproc cluster (now single-node).
- **Stop if:** anything else is replaced or destroyed.

Apply in the same window (`terraform apply tfplan.bin`), then
`Remove-Item Env:TF_VAR_bed_registry_*; Remove-Item tfplan.bin`.

**B2. Ruchir — Notification function cut-over (PR #19, two applies).**
- Remove `notification_image` from `staging.tfvars`; the variable no longer exists.
- The step B apply runs with `eventarc_subscription_id = ""`. It creates `hospital-notification-fn` with its own
  Eventarc trigger and removes the old Cloud Run service and its trigger.
- Read the new trigger's subscription:
  ```powershell
  gcloud eventarc triggers describe <terraform output notification_function_trigger> --location asia-south1 --format="value(transport.pubsub.subscription)"
  ```
- Set `eventarc_subscription_id` to that value and apply again. This imports the subscription and adds the
  dead-letter policy (`bed-notifications-dlq` after 10 attempts).

**C. Ruchir — one SQL statement as `postgres`** (Cloud SQL Studio, database `beds`):
`REVOKE cloudsqlsuperuser FROM bed_registry; ALTER ROLE bed_registry NOCREATEDB NOCREATEROLE;`

**D. Team — merge to `master`** after CI is green and §3 S1–S2 are done. Merge Nisarg's two PRs (lock logging; member-C docs) first. `temp_*.txt` are already removed in PR #20 (§3 S4).
Rotation is required whether the repository is public or private: the files are in its history.

**E. Pratham (with approval) — Argo CD deployment, in dependency order.**
1. Register the root Application:
   ```bash
   kubectl apply -f gitops/bootstrap/root-application.yaml
   ```
   This creates the AppProject and all child Applications. Lock Coordinator, Patient Registry and
   Staff Scheduler auto-sync.
2. Sync the manual apps in this order: `hospital-bed-registry-staging` (its migration hook runs first) →
   `hospital-allocation-migrations-staging` → `hospital-allocation-triage-staging` → `hospital-monitoring` →
   `hospital-resource-analytics-staging` → `hospital-demo-ui`.

   For example: `kubectl -n argocd patch application <name> --type merge -p '{"operation":{"sync":{}}}'`
3. Verify each app is Synced/Healthy, pods are Ready, and the public URL responds through the Ingress IP
   (`/healthz`, and `/docs` loads in a browser)
   (`kubectl -n hospital-system get ingress`).

**F. Pratham (with approval) — live verification and evidence** (watch capacity, §3 S8; then §3 S7):
- run the demo console flow (`docs/demo-script.md` §2);
- check Notification logs;
- sync `hospital-load-test` for the HPA demo, watching `kubectl get hpa -w` and Grafana;
- kill an Allocation pod mid-flow;
- run Cloud Logging queries;
- confirm SQL, Firestore and GCS rows and objects exist.

Save screenshots into `docs/evidence/`.

**G. Jay + Ruchir + Pratham — Kafka/Flink (today, Confluent free trial):** the patch is already applied on branch
`feature/kafka-confluent`. Follow `docs/kafka-enable/CHECKLIST.md` from step 2 (step 3 is done).

**H. Optional, approval needed — HTTPS for the public URL:** `docs/proposals/public-endpoint-https.md` §2 (needs a
domain and a Terraform static IP). The availability-endpoint alternative in §1 was not chosen.

**I. Nisarg — requirement (b) verification:** after step B2, run the checks in
`docs/proposals/serverless-function-handoff.md` (event handled, duplicate skipped, malformed message acknowledged,
DLQ after 10 attempts) and save the evidence.

**J. Everyone — deliverables:**
- **Design document:** export the diagrams from `docs/design-as-built.md` (mermaid.live → PNG). Add §4
  "Differences from the plan" to `cloud_proj_plan.docx` (its byte-identical copy `docs/Untitled.docx` was removed).
- **`task_divison.txt`:** confirm member A's name ("Ruchi Kumar" vs Ruchir) and the IDs. The brief's filename is
  `task_divison.txt`; its byte-identical copy `task_division.txt` was removed.
- **Viva notes:** each member prepares their two services' viva answers. Member D already has
  `docs/member_d_viva_qa.md`; the others can use `docs/demo-script.md` and the design document.
- **Submit on Nalanda:** one submission per group, with the GitHub link.

**K. After the demo (with approval):** scale the node pool to 0, stop Cloud SQL (activation policy NEVER), stop
Dataproc, delete the public Ingress. Never delete databases, buckets or state.

## 3. Security and credential cleanup

**Why now.** Commit `42fb8e6` (`ruchir_kumar`, on GitHub) added `temp_db.txt` (144 B, with a database-URL/password
shape), `temp_key.txt` (16 B) and `temp_secret.txt` (64 B; the shape of a Confluent Cloud API key/secret). Their
contents were never opened by Claude. Treat every Cloud SQL password and the Confluent key as exposed.

No GKE workload uses the database yet, so rotating **before step E** costs no downtime. Do S1–S4 together with
step B. Every item needs the owner's own action or approval; Claude does not rotate credentials.

**S1. Confluent key (Ruchir, Confluent Cloud console).** *Administration → Cloud API keys*:
1. Delete the key whose ID matches the `confluent-api-key` secret.
2. Create a new key only when provisioning (Kafka checklist, step 2).
3. Add it as a new version of `confluent-api-key` / `confluent-api-secret`, and disable the old versions:
   `gcloud secrets versions disable <n> --secret=confluent-api-key`.

The key is invalid once it's deleted; confirm in the console that it no longer appears.

**S2. Cloud SQL passwords (Ruchir, Terraform; same window as step B).**
1. **New passwords.** For each exposed user (`postgres`, `patient_registry`, `patient_registry_migrator`,
   `allocation_app`, `allocation_migrator`), generate a new password in memory:
   ```powershell
   $env:TF_VAR_<name>_db_password = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
   ```
   For `postgres` the variable is `TF_VAR_db_admin_password`.
2. **Bump versions.** Increment each matching `*_db_password_version` (e.g. 1 → 2) so the write-only
   password is re-sent.
3. **Plan.** `terraform plan` should show only in-place updates of those `google_sql_user` resources
   (plus step B's changes). Apply.
4. **Plan file.** These existing variables are `sensitive` but **not ephemeral**, so their values are stored
   inside `tfplan.bin`. Delete the plan file right after the apply and never commit or share it. Optionally, mark
   them `ephemeral = true` like the Bed Registry variables.
5. **Secret versions.** Add new versions for the URLs Ruchir created by hand, piping each value through stdin
   and never typing it into a file or the shell history:
   - `cloudsql-db-password`
   - `patient-registry-database-url`, `patient-registry-migration-database-url`
   - `allocation-database-url`, `allocation-migration-database-url`

   Use `postgresql+psycopg2://<user>:<url-encoded password>@127.0.0.1:5432/<db>` with
   `gcloud secrets versions add <secret> --data-file=-`, then disable the old versions.
6. **Check.** In Cloud SQL Studio, signing in with an old password must fail and the new one must work (Ruchir
   types these himself).
7. **Clear the session:**
   ```powershell
   Remove-Item Env:TF_VAR_*password*
   Remove-Item tfplan.bin, tfplan.txt -ErrorAction SilentlyContinue
   ```

**S3. Ruchir's laptop.**
- **Copies of the leaked files:** delete any local copies of `temp_db.txt`, `temp_key.txt` and `temp_secret.txt`,
  plus shell or editor histories that contain the values.
- **Passwords in `staging.tfvars`:** remove the password lines and supply them as `TF_VAR_*` at apply time
  instead. The file is ignored by Git, but it is plaintext on disk.
- **Old plan files:** keep no `tfplan.bin` or `*.tfstate` copies outside the state bucket.

**S4. Repository.**
- **Remove the files:** done in PR #20 (`git rm --sparse temp_db.txt temp_key.txt temp_secret.txt`). The values stay in
  the history of `42fb8e6`; rotation (S1–S2) is what neutralises them.
- **Rewriting history** (`git filter-repo`, then a force-push, after which every clone must re-clone) is a
  separate team decision, not needed once S1–S2 are done.
- **Ignore rules:** `.gitignore` now also ignores `temp_*.txt`, `tfplan*`, and service-account key/PEM files.

**S5. Terraform state (Ruchir, before or with step B; state changes need approval).**
The GCS object `gs://hospital-management-514618-tf-state/default.tfstate` was last written 2026-11-14 05:49 UTC,
but later applies (Dataproc at 18:27) went somewhere else.
1. **Find the authoritative state.** On Ruchir's laptop run `terraform -chdir=infra/terraform state list | Measure-Object`.
   Then check `infra/terraform/.terraform/terraform.tfstate` (backend type, bucket, prefix) and whether a local
   `terraform.tfstate` exists.
2. **Archive the stale copy:** `gcloud storage cp gs://…/default.tfstate gs://…/archive/default-2026-11-14.tfstate`.
   Keep the bucket restricted: state contains sensitive attributes.
3. **Use one shared backend.** Point the backend at the bucket with an explicit prefix
   (`backend.hcl`: `bucket = "hospital-management-514618-tf-state"`, `prefix = "hospital-management/root"`), then run
   `terraform init -migrate-state -backend-config=backend.hcl` from the authoritative copy. After that, everyone
   plans against the same state.
4. **Resolve the drift** the stale plan showed, as separate reviewed changes:
   - Dataproc `service_account_scopes` (that replacement is now intended by the single-node change);
   - Cloud Run `scaling` block;
   - the notification service account's display name;
   - the Eventarc subscription import.

**S6. Network hardening (approval needed).**
- **Default VPC.** The unused `default` network allows SSH (22) and RDP (3389) from `0.0.0.0/0` and has no VMs.
  It's not managed by Terraform. Delete it:
  ```bash
  gcloud compute firewall-rules delete default-allow-ssh default-allow-rdp default-allow-icmp default-allow-internal --project=hospital-management-514618
  ```
  ```bash
  gcloud compute networks delete default --project=hospital-management-514618
  ```
- **GKE allowlist.** Once every member has used the DNS endpoint (`get-credentials … --dns-endpoint`), replace the
  two shared WARP addresses in `master_authorized_networks` with freshly verified personal /32s, through
  Terraform (the variable requires at least one entry; never `0.0.0.0/0`).

**S7. Database least privilege for the existing users (Ruchir as `postgres`, after step E's migrations succeed).**
API-created users belong to `cloudsqlsuperuser`.
1. **Check the grants first.** `allocation_app` receives DML from Allocation migration `20261031_04`; confirm
   Patient Registry's migration grants its runtime role the same. In each database:
   ```sql
   SELECT has_table_privilege('<role>', '<table>', 'INSERT');
   ```
2. **Then remove the extra privileges:**
   ```sql
   REVOKE cloudsqlsuperuser FROM allocation_app;
   REVOKE cloudsqlsuperuser FROM patient_registry;
   ALTER ROLE allocation_app NOCREATEDB NOCREATEROLE;
   ALTER ROLE patient_registry NOCREATEDB NOCREATEROLE;
   ```
3. **Re-test** the demo flow. Revoking before the grants are confirmed would break the service.

Optional: give the Allocation migration Job its own GSA/KSA (like `hospital-pr-migrator`), so the runtime
identity can no longer read `allocation-migration-database-url`.

**S8. Capacity watch during the HPA test (Pratham).** At both HPA maxima, two nodes are about 94 % *requested* CPU.
During the load run, watch:
```bash
kubectl get pods -A --field-selector=status.phase=Pending
```
```bash
kubectl describe nodes | grep -A6 "Allocated resources"
```
If pods stay Pending, stop the test and report the shortfall. Do not add a third node without approval.

## 4. Cost guard

About $14–17/day while everything runs (2 nodes, Cloud SQL, NAT, load balancer). Check *Billing → Credits* (remaining
value and expiry) before Step E. Run the system only for testing and demo windows; Step K stops most of the cost.

## 5. Changes in PR #20 since `c904760`

| Area | Files |
|---|---|
| Merge of `master` | PR #19 (Notification as a Cloud Run function); the CI publish job no longer builds a Notification image |
| Public URL | `k8s/overlays/gke/patient-registry/public-ingress.yaml` routes `/api/v1`, `/docs`, `/openapi.json`, `/healthz` |
| Leaked files | `temp_db.txt`, `temp_key.txt`, `temp_secret.txt` removed (rotation in §3 is still required) |
| CI publishing | `scripts/pin_image_digest.py`, `tests/test_pin_image_digest.py` |
| CI fix | `.github/workflows/ci.yml` (render + kubeconform + release gate for every Argo CD path; the gate must pass) |
| Demo UI | `services/demo_ui/{main.py,static/index.html,Dockerfile,__init__.py}`, `k8s/demo-ui/*`, `gitops/apps/demo-ui.yaml`, `tests/test_demo_ui.py`; image `demo-ui@sha256:0787f2c8…f9c2` |
| Load test | `k6/sustained-multi-pool.js` (optional setup seeding), `k6/kustomization.yaml`, `k6/load-test-job.yaml`, `gitops/apps/load-test.yaml` |
| Flink | `services/resource_analytics/flink_job.sql`, `submit_flink_job.sh` |
| Dataproc | `infra/terraform/modules/dataproc/{main,variables}.tf` (single node) |
| Release gate | `scripts/check_release_config.py` (+ `k8s/demo-ui`, `k6`), `tests/test_release_config.py` |
| Build hygiene | `.dockerignore`, `.gitignore` (temp files, plan files, key files) |
| Docs | this guide, `docs/design-as-built.md`, `docs/demo-script.md`, `docs/kafka-enable/*`, `docs/proposals/*` |
