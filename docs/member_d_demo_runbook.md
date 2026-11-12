# Member D (Jay Patel — 2026H1030072P) Demonstration & Viva Runbook

This document is your step-by-step demonstration guide for the **12 Marks Individual Contribution Viva**.

---

## 1. Responsibilities Overview

| Item | Requirement | Deliverable in Codebase |
|---|---|---|
| **Service 2** | Bed Registry Service | `services/bed_registry/` (gRPC + REST + Cloud SQL) |
| **Service 8** | Resource Analytics Service | `services/resource_analytics/` (Flink SQL + Firestore) |
| **Task (e)** | Stream Processing (Flink on Dataproc) | `infra/terraform/modules/dataproc/`, `flink_job.sql`, `submit_flink_job.sh` |
| **Task (g)** | 3 Distinct Cloud Storage Products | Cloud SQL (`beds`), Firestore (`pool_stats`), GCS (`flink` bucket) |

---

## 2. Test Verification

Run all Member D unit & integration tests:
```powershell
python -m pytest tests/test_bed_registry.py tests/test_resource_analytics.py -v
```

---

## 3. Live Demonstration Walkthrough for Viva

### Demo 1: Bed Registry & Distributed Lock Fencing Protection
**What to explain to the evaluator:**
> *"In a distributed system, an allocator replica may pause due to GC or network delay while holding a lock lease. ZooKeeper issues a monotonically increasing fencing token (`czxid`). My Bed Registry checks every write: if an allocator tries to write with a token lower than the highest token seen for that pool, Bed Registry rejects the write to prevent double-allocation or race conditions."*

**Commands to run:**
1. Start the Bed Registry Service:
   ```powershell
   $env:START_GRPC="true"
   python -m services.bed_registry.main
   ```
2. In a second terminal, inspect current fencing token for pool `WARDA-ICU`:
   ```bash
   curl http://localhost:8003/api/v1/pools/WARDA-ICU/fencing-token
   ```
3. Allocate a bed with a valid fencing token (e.g., token = `50`):
   ```bash
   curl -X POST http://localhost:8003/api/v1/beds/bed-warda-icu-01/allocate \
     -H "Content-Type: application/json" \
     -d '{"operation_id": "alloc-01", "bed_id": "bed-warda-icu-01", "allocation_id": "adm-01", "fencing_token": 50, "pool_id": "WARDA-ICU"}'
   ```
   *Expected response:* `{"success": true, "bed": {"status": "OCCUPIED", ...}}`
4. Demonstrate fencing rejection by sending a delayed request with an old token (`40`):
   ```bash
   curl -X POST http://localhost:8003/api/v1/beds/bed-warda-icu-02/allocate \
     -H "Content-Type: application/json" \
     -d '{"operation_id": "alloc-stale", "bed_id": "bed-warda-icu-02", "allocation_id": "adm-02", "fencing_token": 40, "pool_id": "WARDA-ICU"}'
   ```
   *Expected response:* `HTTP 409 Conflict` with error:
   `"Stale fencing token 40; pool WARDA-ICU highest token is 50"`

---

### Demo 2: Real-Time Stream Processing with Apache Flink on Dataproc (Task e)
**What to explain to the evaluator:**
> *"Bed events (`ALLOCATION_REQUESTED`, `BED_ALLOCATED`, `ALLOCATION_WAITLISTED`, `BED_RELEASED`) are streamed to Kafka topic `bed-events`. On Google Cloud Dataproc, my Apache Flink SQL job performs stateful 1-minute tumbling window aggregations with event-time watermarks, calculating total requests, allocations, waitlisted contention, and average lock wait latency, then publishes the results to Kafka topic `bed-stats`."*

**Code artifacts to show:**
1. Show the Flink SQL streaming pipeline:
   [`services/resource_analytics/flink_job.sql`](../services/resource_analytics/flink_job.sql)
2. Show the Dataproc job submission script:
   [`services/resource_analytics/submit_flink_job.sh`](../services/resource_analytics/submit_flink_job.sh)
3. Show the Dataproc Terraform cluster module:
   [`infra/terraform/modules/dataproc/main.tf`](../infra/terraform/modules/dataproc/main.tf)

---

### Demo 3: Three Distinct Cloud Storage Products (Task g)
**What to explain to the evaluator:**
> *"We utilize three distinct managed cloud storage products matching data access patterns:*
> *1. **Cloud SQL (PostgreSQL 16)**: Relational inventory tables `beds` and `pool_fencing_tokens`.*
> *2. **Google Cloud Firestore (NoSQL Native)**: Document collection `pool_stats` for real-time window metrics.*
> *3. **Google Cloud Storage (GCS)**: Object store bucket for Flink job JARs and streaming state checkpoints.*

**Endpoints to show:**
* Real-time pool metrics: `GET http://localhost:8008/api/v1/analytics/pools/WARDA-ICU/latest`
* Hospital-wide analytics summary: `GET http://localhost:8008/api/v1/analytics/summary`
