# Architectural Decisions and Defaults

This document records sensible architectural decisions, technical trade-offs, and defaults chosen for local development and distributed correctness. It explicitly distinguishes local development defaults from team-approved contracts.

---

## 1. Uniform REST Error Envelope
- **Decision**: All REST endpoints must return a unified JSON error structure:
  ```json
  {
    "error": {
      "code": "INVALID_ARGUMENT",
      "message": "Human readable explanation",
      "request_id": "req-12345",
      "retryable": false
    }
  }
  ```
- **Rationale**: Prevents framework-specific leaks (e.g. default FastAPI 422 unprocessable entity responses) and guarantees uniform client error handling.
- **Status**: Team standard.

---

## 7. Service Directory and Python Package Naming
- **Decision**: In Kubernetes, Docker, and Terraform configs, services are referenced using kebab-case (`bed-allocation`, `triage`). In Python source trees, directory names use snake_case (`services/bed_allocation/`, `services/triage/`) because hyphens are illegal identifier syntax in Python `import` statements.
- **Status**: Local development baseline.

---

## 2. Waitlist Priority and Starvation Policy
- **Decision**: Waitlist priority sorting order:
  1. `triage_level` ASC (1 = Resuscitation/most critical, 5 = Non-urgent)
  2. `queued_at` ASC (FIFO arrival within the same urgency level)
  3. `entry_id` ASC (Deterministic tie-breaking)
- **Policy on Urgency Update while Waitlisted**:
  - Updating triage level atomically updates the triage record and active queue entry while holding the same pool lock used by allocation and promotion.
  - The existing `entry_id` and `queued_at` are preserved.
  - If promotion has already committed, the update must not recreate a queue entry. Lock acquisition is not nested.
- **Policy on Pool Change while Waitlisted**:
  - Direct pool changes are **strictly rejected (HTTP 409)** while a patient has an active waitlist entry. The active allocation/waitlist request must be explicitly cancelled before re-requesting in another pool.
- **Status**: Approved by Pratham Soneja on 2026-10-29.

---

## 3. Distributed Locking & Fencing Tokens
- **Decision**:
  - Lock granularity is strictly **per-pool** (e.g., `WARDA-ICU`).
  - ZooKeeper Lock Coordinator issues a monotonically increasing 64-bit integer fencing token on lock acquisition.
  - Downstream services (Bed Registry, Staff Scheduler, Triage) accept `MutationContext` containing `(pool_id, lock_id, owner_id, fencing_token)`.
  - Mutation handlers reject operations if `fencing_token < last_seen_fencing_token`.
  - Triage queue mutations atomically compare/update the recorded pool fence and operation deduplication state with the queue mutation. Lower tokens are rejected; equal tokens require the recorded lock identity to match.
  - Coordinator ownership validation is an external check and is not atomic with the Firestore commit. A lease can change after validation; the Firestore fence prevents a lower token from committing after a higher token has been recorded, but does not make the lease check and commit one transaction.
- **Local Development Default**:
  - In unit tests and standalone local runs, an injectable `LockCoordinator` test double simulates sequential lock leases and validates monotonic tokens.
  - In extended integration, a real ZooKeeper ensemble and gRPC Lock Coordinator service are used.
- **Status**: Provisional contract agreed with Nisarg Vagh (Member C).

---

## 4. No-Staff Capacity and Failure Compensation Policy
- **Decision**:
  - Waitlist only when bed or staff capacity shortage is confirmed.
  - A reserved bed must be confirmed ownership-safely compensated before the request can become `WAITLISTED`. Validation errors, arbitrary assignment errors, timeouts, and uncertain cleanup do not become waitlist outcomes.
  - Uncertain downstream outcomes persist `RECOVERING`, `pending_action`, and stable operation IDs for reconciliation. Retried requests do not start a duplicate workflow.
  - A durably accepted unresolved create or release returns HTTP 202 with `allocation_id`, `status=RECOVERING`, `pending_action`, and a `Location` URL for `GET /api/v1/allocations/{allocation_id}`. GET exposes the recovery representation. If durable acceptance itself is uncertain, return an error rather than promise recovery.
  - Release is terminal only after required cleanup is confirmed. Do not promote using uncertain capacity. After release is confirmed, promotion failure does not reverse the release; preserve the waitlist entry and retry promotion safely.
  - `FAILED` is terminal only after confirming the allocation owns no active downstream resources.
- **Status**: Approved by Pratham Soneja on 2026-10-29.

---

## 5. Storage Layer Selection & Multi-Replica Correctness
- **Decision**:
  - **Triage Service**: Uses Google Cloud Firestore in Native mode (or local Firestore emulator). Provides atomic transactions for waitlist enqueue/dequeue. For isolated local unit tests without emulators, an in-memory repository is selectable via `TRIAGE_STORAGE_BACKEND=memory`. In-memory mode is prohibited for multi-replica GKE deployments.
  - **Bed Allocation Service**: Uses Cloud SQL for PostgreSQL (local PostgreSQL container). Manages transactional state, idempotency records, and transactional outbox table in atomic relational transactions.
- **Status**: Approved architecture mapping.

## 8. Active Admission Uniqueness and Migrations
- **Decision**:
  - A PostgreSQL partial unique index on `allocations.admission_id` applies to every status except `RELEASED`, `CANCELLED`, and `FAILED`. The exact active predicate is `status NOT IN ('RELEASED', 'CANCELLED', 'FAILED')`, covering `REQUESTED`, `PROCESSING`, `ALLOCATED`, `WAITLISTED`, `RELEASING`, `CANCELLING`, and `RECOVERING`.
  - `FAILED` is terminal only after confirmed absence of active ownership.
  - A matching unique-index violation returns HTTP 409 `ACTIVE_ALLOCATION_EXISTS`; same-key idempotent replay is checked before active admission conflict.
  - Alembic manages schema migrations. Before index creation, report duplicate active allocations and their IDs; never delete or rewrite them automatically.
- **Status**: Approved by Pratham Soneja on 2026-10-29.

## 9. Triage Idempotency and Queue Mutation Transactions
- **Decision**:
  - Persist the REST PUT idempotency fingerprint and result atomically with the triage record and any active waitlist update in Firestore.
  - Queue mutations atomically enforce pool fencing, operation deduplication, and queue state. Firestore transaction callbacks remain retry-safe and contain no external side effects.
  - Validate lock ownership through the Coordinator, while documenting that this external validation is not atomic with the Firestore commit and therefore has a lease-validation race.
- **Status**: Approved by Pratham Soneja on 2026-10-29; implementation pending.

## 10. Allocation Durable Idempotency and Recovery
- **Decision**: Allocation claims are scoped by HTTP method, operation, target, and idempotency key; a canonical request fingerprint prevents reuse with a different payload. The claim and stable allocation/action identifiers are persisted before remote mutations. Matching in-progress retries return 202 with the same allocation and its GET location; only completed outcomes are cached as final responses.
- **Recovery**: Persist each remote operation intent before the call and its confirmed or uncertain outcome afterward. Reuse operation IDs, claim expired work exclusively, and never hold a PostgreSQL transaction across network calls. Unknown outcomes are reconciled; compensation is limited to owned resources. Release is durable before promotion, and promotion failure preserves the release and queue retry path.
- **Status**: Approved by Pratham Soneja on 2026-10-29. Step 2 implementation and verification are in progress; current create-path evidence and outstanding release/cancel recovery gaps are in `docs/handoff.md`.

---

## 6. Outbox Pattern & Messaging Integration
- **Decision**:
  - Allocation state mutations atomically persist outbox events into the `outbox_events` PostgreSQL table.
  - Distributed locks are **never held** while awaiting external broker network ACKs (Pub/Sub or Kafka).
  - Outbox publisher runs asynchronously, tracking delivery status per destination (`pubsub` and `kafka`) with exponential backoff and retry counters.
  - Message keys for Kafka are keyed strictly by `pool_id` to preserve per-pool partition order.
- **Status**: Team standard.

---

## 7. Service Directory and Python Package Naming
- **Decision**: In Kubernetes, Docker, and Terraform configs, services are referenced using kebab-case (`bed-allocation`, `triage`). In Python source trees, directory names use snake_case (`services/bed_allocation/`, `services/triage/`) because hyphens are illegal identifier syntax in Python `import` statements.
- **Status**: Local development baseline.
