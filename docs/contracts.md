# Hospital Bed Management - API and Event Contracts

This document specifies the interface contracts, communication protocols, tracing rules, idempotency requirements, and event schemas for the entire distributed hospital management platform.

---

## 1. Network & Protocol Conventions

| Component | Target Environment | Host / Address | Protocol | Ingress Exposure |
|---|---|---|---|---|
| Triage REST | Local Host / GKE Pod | `0.0.0.0:8001` (local) / `8000` (container) | HTTP/1.1 REST | Internal ClusterIP |
| Triage gRPC | Local Host / GKE Pod | `0.0.0.0:50052` (local) / `50051` (container) | HTTP/2 gRPC | Internal ClusterIP only |
| Bed Allocation REST | Local Host / GKE Pod | `0.0.0.0:8002` (local) / `8000` (container) | HTTP/1.1 REST | Public Ingress via `/api/v1` |
| Lock Coordinator gRPC | Local Host / GKE Pod | `0.0.0.0:50051` | HTTP/2 gRPC | Internal ClusterIP only |
| Bed Registry gRPC | Local Host / GKE Pod | `0.0.0.0:50053` | HTTP/2 gRPC | Internal ClusterIP only |
| Staff Scheduler gRPC | Local Host / GKE Pod | `0.0.0.0:50054` | HTTP/2 gRPC | Internal ClusterIP only |
| Patient Registry REST | Local Host / GKE Pod | `0.0.0.0:8000` | HTTP/1.1 REST | Internal ClusterIP |

All network addresses must be configured via environment variables. Hostnames must never be hardcoded.

---

## 2. Shared Request Headers & Context

- **`X-Request-ID`**: Per-attempt trace identifier for distributed request tracking across all services and logs.
- **`Idempotency-Key`**: Header supplied by clients on state-changing REST endpoints (`POST`, `PUT`). Identifies a logical action.
- **`operation_id`**: Internal identifier for a specific step in an allocation or compensation workflow (e.g., `alloc-001:reserve-bed`, `alloc-001:assign-staff`). Retries reuse the identical `operation_id`.
- **Fencing Tokens**: Issued by ZooKeeper Lock Coordinator. Formatted as `uint64` in Protobuf and decimal strings in JSON APIs.

---

## 3. Uniform REST Error Format

All error responses strictly follow this format:
```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable description",
    "request_id": "req-001",
    "retryable": false
  }
}
```

### Status Code & Error Code Mappings
| Scenario | HTTP Status | gRPC Status | Error Code | Retryable |
|---|---|---|---|---|
| Schema / Field validation error | 400 | `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | No |
| Resource not found | 404 | `NOT_FOUND` | `NOT_FOUND` | No |
| Other conflicting state | 409 | `FAILED_PRECONDITION` | `STATE_CONFLICT` | No |
| Partial unique active-admission index violation | 409 | n/a | `ACTIVE_ALLOCATION_EXISTS` | No |
| Idempotency Key reused with different payload | 409 | `ALREADY_EXISTS` | `IDEMPOTENCY_MISMATCH` | No |
| Downstream service unreachable | 503 | `UNAVAILABLE` | `SERVICE_UNAVAILABLE` | Yes |
| Deadline / Timeout exceeded | 504 | `DEADLINE_EXCEEDED` | `DEADLINE_EXCEEDED` | Yes |

*Note: Waitlisting is a successful business outcome (HTTP 201), never an error.*

If an allocation create or release has been durably accepted but remains unresolved, return HTTP 202 with `allocation_id`, `status: RECOVERING`, and `pending_action`; include a `Location` header pointing to `GET /api/v1/allocations/{allocation_id}`. GET returns the same recovery state. Do not return 202 if durable acceptance is uncertain. Retries with the same idempotency key must not start another workflow.

Triage `PUT /api/v1/triage/{admission_id}` requires `Idempotency-Key`. The durable Firestore claim is scoped to method, operation, admission target, and hashed key; its canonical fingerprint covers method, operation, admission ID, triage level, and pool ID. Matching completed retries replay the original serialized `TriageRecord` without a lock or state mutation. Changed payload under the same scope returns 409. The first successful PUT fixes `pool_id` for this project version; pool changes return `409 POOL_CHANGE_NOT_SUPPORTED`.

Only `RELEASED`, `CANCELLED`, and `FAILED` are terminal for admission uniqueness. `FAILED` is terminal only after confirming there is no active downstream ownership. A matching PostgreSQL partial unique-index violation maps to `ACTIVE_ALLOCATION_EXISTS`; idempotency replay is checked first.

---

## 4. Protobuf Shared Contracts (`hospital.v1`)

```protobuf
message MutationContext {
  string operation_id = 1;
  string request_id = 2;
  string pool_id = 3;
  string lock_id = 4;
  string owner_id = 5;
  uint64 fencing_token = 6;
}
```

Every state mutation RPC (`TriageService.Enqueue`, `TriageService.RemoveEntry`, `BedService.AllocateBed`, `StaffScheduler.AssignStaff`) requires a valid `MutationContext`. Receivers reject invocations if the fencing token is stale or ownership does not match.

---

## 5. Event Specifications

### Notification Event (`bed-notifications` on Google Pub/Sub)
Triggered by Bed Allocation state changes, delivered to Cloud Run serverless function via Eventarc.
- **Payload Schema**: [`contracts/events/notification.schema.json`](file:///contracts/events/notification.schema.json)
- **Supported Event Types**: `BED_ALLOCATED`, `ALLOCATION_WAITLISTED`, `BED_RELEASED`, `ALLOCATION_CONFLICT`.

### Bed Event (`bed-events` on Kafka)
Triggered for stream processing by Apache Flink on Dataproc.
- **Message Key**: `pool_id`
- **Payload Schema**: [`contracts/events/bed-event.schema.json`](file:///contracts/events/bed-event.schema.json)
- **Occupancy Deltas**:
  - `ALLOCATION_REQUESTED`: `0`
  - `BED_ALLOCATED`: `+1`
  - `ALLOCATION_WAITLISTED`: `0`
  - `BED_RELEASED`: `-1`
  - `ALLOCATION_CONFLICT`: `0`

### Bed Stats (`bed-stats` on Kafka)
Output of Flink windowed aggregation over `[start, end)`.
- **Payload Schema**: [`contracts/events/bed-stats.schema.json`](file:///contracts/events/bed-stats.schema.json)
