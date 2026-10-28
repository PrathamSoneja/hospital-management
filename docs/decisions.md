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

## 2. Waitlist Priority and Starvation Policy
- **Decision**: Waitlist priority sorting order:
  1. `triage_level` ASC (1 = Resuscitation/most critical, 5 = Non-urgent)
  2. `queued_at` ASC (FIFO arrival within the same urgency level)
  3. `entry_id` ASC (Deterministic tie-breaking)
- **Policy on Urgency Update while Waitlisted**:
  - Updating triage level updates the waitlist entry's `triage_level` immediately.
  - The original `queued_at` timestamp is **preserved** to ensure fair FIFO ordering among patients sharing the newly assigned urgency tier.
- **Policy on Pool Change while Waitlisted**:
  - Direct pool changes are **strictly rejected (HTTP 409)** while a patient has an active waitlist entry. The active allocation/waitlist request must be explicitly cancelled before re-requesting in another pool.
- **Status**: Provisional default, documented for Team Review.

---

## 3. Distributed Locking & Fencing Tokens
- **Decision**:
  - Lock granularity is strictly **per-pool** (e.g., `WARDA-ICU`).
  - ZooKeeper Lock Coordinator issues a monotonically increasing 64-bit integer fencing token on lock acquisition.
  - Downstream services (Bed Registry, Staff Scheduler, Triage) accept `MutationContext` containing `(pool_id, lock_id, owner_id, fencing_token)`.
  - Mutation handlers reject operations if `fencing_token < last_seen_fencing_token`.
- **Local Development Default**:
  - In unit tests and standalone local runs, an injectable `LockCoordinator` test double simulates sequential lock leases and validates monotonic tokens.
  - In extended integration, a real ZooKeeper ensemble and gRPC Lock Coordinator service are used.
- **Status**: Provisional contract agreed with Nisarg Vagh (Member C).

---

## 4. No-Staff Capacity and Failure Compensation Policy
- **Decision**:
  - If suitable beds exist in the pool but the Staff Scheduler reports **zero staff capacity**, the allocation request transitions to **WAITLISTED** (HTTP 201) instead of returning an error, as nursing capacity is dynamic.
  - If a bed reservation succeeds but subsequent staff assignment fails (e.g. concurrent claim or transient failure):
    1. The bed reservation is **compensated** via `ReleaseBed` passing `allocation_id` to ensure ownership safety (never blindly releasing a bed that may have been reallocated).
    2. The allocation record records the failed step.
    3. The request is placed into the waitlist.
  - If a downstream call times out with an **uncertain outcome**:
    - The allocation record enters `RECOVERING` state with a durable operation record. A reconciliation worker queries the downstream dependency using the stable `operation_id` before deciding whether to roll forward or compensate.
- **Status**: Local development baseline; pending review by Ruchi Kumar (Member A).

---

## 5. Storage Layer Selection & Multi-Replica Correctness
- **Decision**:
  - **Triage Service**: Uses Google Cloud Firestore in Native mode (or local Firestore emulator). Provides atomic transactions for waitlist enqueue/dequeue. For isolated local unit tests without emulators, an in-memory repository is selectable via `TRIAGE_STORAGE_BACKEND=memory`. In-memory mode is prohibited for multi-replica GKE deployments.
  - **Bed Allocation Service**: Uses Cloud SQL for PostgreSQL (local PostgreSQL container). Manages transactional state, idempotency records, and transactional outbox table in atomic relational transactions.
- **Status**: Approved architecture mapping.

---

## 6. Outbox Pattern & Messaging Integration
- **Decision**:
  - Allocation state mutations atomically persist outbox events into the `outbox_events` PostgreSQL table.
  - Distributed locks are **never held** while awaiting external broker network ACKs (Pub/Sub or Kafka).
  - Outbox publisher runs asynchronously, tracking delivery status per destination (`pubsub` and `kafka`) with exponential backoff and retry counters.
  - Message keys for Kafka are keyed strictly by `pool_id` to preserve per-pool partition order.
- **Status**: Team standard.
