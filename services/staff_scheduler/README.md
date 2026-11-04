# Staff Scheduler

Staff Scheduler provides REST roster/shift management and gRPC capacity, assignment, and release methods. The gRPC service implements the existing [`StaffScheduler` protobuf](../../contracts/proto/staff.proto), uses REST on container port `8000`, and gRPC on container port `50051`.

## Firestore data layout

- `staff/{staff_id}` contains staff role, active status, eligible `pool_ids`, `max_concurrent_assignments`, and an atomic active-assignment counter.
- `staff/{staff_id}/shifts/{shift_id}` contains timezone-aware `starts_at`, `ends_at`, and `active`.
- `staff_assignments/{assignment_id}` stores active and released assignment records.
- `staff_operation_results/{sha256(operation_id)}` makes gRPC assignment and release retries idempotent.
- `staff_pool_fence/{pool_id}` prevents stale mutation fencing tokens from writing.

A staff member is assignable only if they are an active nurse assigned to the pool, currently on an active shift, and below their concurrency limit. Candidate staff are chosen in staff ID order. A Firestore transaction reads candidate staff, their shifts, and the pool fence before atomically updating the chosen staff's counter, assignment, operation result, and fencing record.

## Configuration

- `FIRESTORE_PROJECT_ID`: GCP project containing the Native Firestore database.
- `FIRESTORE_EMULATOR_HOST`: optional local-only Firestore emulator address.
- `STAFF_SCHEDULER_REST_HOST` / `STAFF_SCHEDULER_REST_PORT`: REST bind address / port.
- `STAFF_SCHEDULER_GRPC_HOST` / `STAFF_SCHEDULER_GRPC_PORT`: gRPC bind address / port.
- `START_GRPC`: set `true` to run the gRPC server alongside REST (default `true`).

GKE workloads authenticate with Workload Identity; no service-account key files are used. Memory storage is available only through explicit dependency injection in tests; runtime uses Firestore.

REST request and response schemas are described in [`contracts/openapi/staff.openapi.yaml`](../../contracts/openapi/staff.openapi.yaml). Staff creation and shift upserts require `Idempotency-Key`; errors use the shared JSON envelope and request IDs are propagated in logs and HTTP response headers.

Run locally with the `application` Compose profile and the Firestore emulator. Docker builds this service only; Kubernetes deployment manifests remain under the GitOps owner's scope.
