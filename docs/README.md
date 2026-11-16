# Documents

## Current — use these

| Document | Purpose | Main readers |
|---|---|---|
| [`261_CC_Assignment_new (1).pdf`](<261_CC_Assignment_new (1).pdf>) | The assignment brief; everything is measured against it | everyone |
| [`COMPLETION-GUIDE.md`](COMPLETION-GUIDE.md) | Requirement status with evidence, what was done, remaining steps with owners, deployment lessons, security status, cost | everyone |
| [`../cloud_proj_plan.docx`](../cloud_proj_plan.docx) | Design document (deliverable) | everyone |
| [`design-as-built.md`](design-as-built.md) | Diagrams of the deployed system and differences from the design document | everyone |
| [`demo-script.md`](demo-script.md) | Demo order per member, expected outputs, reset, viva quick answers | everyone |
| [`contracts.md`](contracts.md) | REST, gRPC and event contracts | everyone |
| [`decisions.md`](decisions.md) | Architecture decisions and defaults | everyone |
| [`deployment-status-2026-11-16.md`](deployment-status-2026-11-16.md) | Live system on 2026-11-16: endpoints, services, image digests, verified flow, infrastructure, cost | everyone |
| [`evidence/2026-11-16-gke/`](evidence/2026-11-16-gke/README.md) | Cloud evidence files mapped to the brief's requirements | everyone |
| [`kafka-enable/CHECKLIST.md`](kafka-enable/CHECKLIST.md) | Requirement (e): Flink submission and verification (Confluent and Kafka are enabled; the patch was applied in PR #22) | Jay |
| [`proposals/serverless-function-handoff.md`](proposals/serverless-function-handoff.md) | Requirement (b): Notification as a Cloud Run function (implemented in PR #19); verification checks | Nisarg |
| [`proposals/public-endpoint-https.md`](proposals/public-endpoint-https.md) + [`public-availability-endpoint.patch`](proposals/public-availability-endpoint.patch) | Optional HTTPS for the public URL (§2). The availability endpoint (§1 and its patch) was not chosen | Ruchir |
| [`member_c_demo_runbook.md`](member_c_demo_runbook.md), [`member_c_viva_qa.md`](member_c_viva_qa.md) | Member C demo and viva preparation | Nisarg |
| [`member_d_demo_runbook.md`](member_d_demo_runbook.md), [`member_d_viva_qa.md`](member_d_viva_qa.md) | Member D demo and viva preparation | Jay |
| [`../task_divison.txt`](../task_divison.txt) | Deliverable: IDs, names, tasks (the brief's exact filename) | everyone |

## Historical — do not follow (each has a SUPERSEDED banner)

| Document | Why it is superseded |
|---|---|
| [`gcp-integration-runbook.md`](gcp-integration-runbook.md) | Pre-deployment plan; refers to removed `k8s/argocd/` and to placeholders now resolved |
| [`precloud-status.md`](precloud-status.md) | Pre-GCP test and load evidence |
| [`handoff.md`](handoff.md) | Local development handoff, last updated 2026-11-06 |
| [`../updates.md`](../updates.md) | Ruchir's status notes from before the GCP release |
| [`deployment-status-2026-11-15.md`](deployment-status-2026-11-15.md) | Snapshot from before the Argo CD deployment |

Removed during cleanup (still in Git history): `Untitled.docx` and `../task_division.txt` (byte-identical copies), and
`proposed-fixes/staff-scheduler-transaction-get.patch` (superseded by Ruchir's fix in `7e70be1`).
