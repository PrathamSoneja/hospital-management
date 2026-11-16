# Documents

## Current — use these

| Document | Purpose | Main readers |
|---|---|---|
| [`261_CC_Assignment_new (1).pdf`](<261_CC_Assignment_new (1).pdf>) | The assignment brief; everything is measured against it | everyone |
| [`COMPLETION-GUIDE.md`](COMPLETION-GUIDE.md) | Requirement status, remaining steps in order with owners, security and credential cleanup, cost | everyone |
| [`../cloud_proj_plan.docx`](../cloud_proj_plan.docx) | Design document (deliverable) | everyone |
| [`design-as-built.md`](design-as-built.md) | Diagrams of the deployed system and differences from the design document | everyone |
| [`demo-script.md`](demo-script.md) | Demo order per member, expected outputs, reset, viva quick answers | everyone |
| [`contracts.md`](contracts.md) | REST, gRPC and event contracts | everyone |
| [`decisions.md`](decisions.md) | Architecture decisions and defaults | everyone |
| [`deployment-status-2026-11-15.md`](deployment-status-2026-11-15.md) | Snapshot of the GCP inventory, linkage map and cost on 2026-11-15 | Ruchir, Pratham |
| [`kafka-enable/CHECKLIST.md`](kafka-enable/CHECKLIST.md) + [`kafka-enable.patch`](kafka-enable/kafka-enable.patch) | Requirement (e): Confluent, Kafka, Flink, results topic | Jay, Ruchir |
| [`proposals/serverless-function-handoff.md`](proposals/serverless-function-handoff.md) | Requirement (b): Notification as a Cloud Run function (implemented in PR #19); verification checks | Nisarg |
| [`proposals/public-endpoint-https.md`](proposals/public-endpoint-https.md) + [`public-availability-endpoint.patch`](proposals/public-availability-endpoint.patch) | Optional HTTPS for the public URL (§2). The availability endpoint (§1 and its patch) was not chosen | Ruchir |
| [`member_d_demo_runbook.md`](member_d_demo_runbook.md), [`member_d_viva_qa.md`](member_d_viva_qa.md) | Member D demo and viva preparation | Jay |
| [`../task_divison.txt`](../task_divison.txt) | Deliverable: IDs, names, tasks (the brief's exact filename) | everyone |

## Historical — do not follow (each has a SUPERSEDED banner)

| Document | Why it is superseded |
|---|---|
| [`gcp-integration-runbook.md`](gcp-integration-runbook.md) | Pre-deployment plan; refers to removed `k8s/argocd/` and to placeholders now resolved |
| [`precloud-status.md`](precloud-status.md) | Pre-GCP test and load evidence |
| [`handoff.md`](handoff.md) | Local development handoff, last updated 2026-11-06 |
| [`../updates.md`](../updates.md) | Ruchir's status notes from before the GCP release |

Removed during cleanup (still in Git history): `Untitled.docx` and `../task_division.txt` (byte-identical copies), and
`proposed-fixes/staff-scheduler-transaction-get.patch` (superseded by Ruchir's fix in `7e70be1`).
