# Member D (Jay Patel — 2026H1030072P) Viva Q&A & Architecture Defense Guide

This guide prepares you for the **12 Marks Individual Demonstration and Viva** in BITS Pilani CS/SS G527 (Cloud Computing).

---

## 1. Core Architecture Questions & Model Answers

### Q1: Why do we need "Fencing Tokens" in Bed Registry? Isn't a distributed lock enough?
> **Answer:**
> "A distributed lock alone is not sufficient in asynchronous distributed systems because of the **stale client problem** (documented by Martin Kleppmann).
>
> Suppose Allocator Replica A acquires the lock for pool `WARDA-ICU` with a 5-second lease. If Replica A experiences a major **Garbage Collection (GC) pause** or a network partition lasting 6 seconds, the Lock Coordinator's reaper expires the lease and deletes the ephemeral znode.
>
> The Lock Coordinator then grants the lock to Replica B with a new, higher ZooKeeper transaction ID (`czxid = 50`). Replica B allocates Bed 01 to Patient B.
>
> When Replica A unfreezes, it does not realize its lease expired and attempts to write Bed 01 for Patient A. Without fencing tokens, Bed 01 would be double-allocated!
>
> With **Fencing Tokens**, my Bed Registry stores the highest fencing token seen for that pool (`50`). When Replica A's stale write arrives carrying token `45`, my Bed Registry detects that `45 < 50` and **rejects the write with HTTP 409 / gRPC FAILED_PRECONDITION**, preserving system correctness."

---

### Q2: How does your Flink stream processing job handle out-of-order Kafka events? (Task e)
> **Answer:**
> "In cloud networks, message events often arrive out-of-order due to network jitter or retries.
>
> My Flink SQL job uses **Event-Time Processing** rather than Processing-Time. The tumbling window is determined by the `event_time` field inside the event payload, not the wall-clock time when Dataproc receives it.
>
> To handle late-arriving events, I configured an event-time watermark:
> ```sql
> WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
> ```
> This tells Flink to tolerate delays of up to 5 seconds. If an event timestamped `12:00:58` arrives at `12:01:02`, Flink correctly assigns it to the `[12:00, 12:01)` window before the window closes."

---

### Q3: Why did you choose three distinct storage engines instead of storing everything in PostgreSQL? (Task g)
> **Answer:**
> "Each storage engine was selected based on access patterns, scaling requirements, and cost:
>
> 1. **Cloud SQL (PostgreSQL 16)**: Used for **Bed Registry**.
>    - *Why*: Relational model requiring strict ACID transactions, table constraints, row-level locks (`SELECT FOR UPDATE`), and transactional consistency for inventory and fencing tokens.
> 2. **Google Cloud Firestore (Native NoSQL)**: Used for **Resource Analytics**.
>    - *Why*: High-throughput append and lookup of semi-structured time-series windows (`pool_stats/{pool}_{window_start}`). It scales horizontally without capacity planning and allows $O(1)$ fast document lookups for dashboards.
> 3. **Google Cloud Storage (GCS)**: Used for **Artifacts & Flink Checkpoints**.
>    - *Why*: Inexpensive, highly durable object store specifically optimized for large binary blobs (Flink job JARs and streaming checkpoint savepoints every 30 seconds)."

---

### Q4: Walk me through the Flink SQL aggregation query in `flink_job.sql`.
> **Answer:**
> "The streaming query runs on Dataproc with three key components:
> 1. **Tumbling Window**:
>    `FROM TABLE(TUMBLE(TABLE bed_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))`
>    Divides the unbounded Kafka stream into discrete, non-overlapping 1-minute intervals.
> 2. **Group By**:
>    `GROUP BY pool_id, window_start, window_end`
>    Groups metrics by each individual hospital ward/pool.
> 3. **Metrics Calculation**:
>    - `requests`: Sum of `ALLOCATION_REQUESTED` events.
>    - `allocated`: Sum of `BED_ALLOCATED` events.
>    - `waitlisted`: Sum of `ALLOCATION_WAITLISTED` events.
>    - `contention_rate`: Computed as `(waitlisted + conflicts) / requests`, indicating what percentage of patients could not immediately get a bed.
>    - `average_lock_wait_ms`: Average latency spent waiting for the distributed lock.
>
> The results are continuously emitted to Kafka topic `bed-stats`."

---

## 2. Quick Demo Commands Cheat Sheet

### Step 1: Run Unit Tests
```powershell
python -m pytest tests/test_bed_registry.py tests/test_resource_analytics.py -v
```
*(All 15 tests pass in ~1.5 seconds)*

### Step 2: Show Bed Registry in Action
Start the Bed Registry service:
```powershell
$env:START_GRPC="true"
python -m services.bed_registry.main
```
In another terminal:
```bash
# 1. Inspect beds in WARDA-ICU
curl http://localhost:8003/api/v1/beds?pool_id=WARDA-ICU

# 2. Check current highest fencing token
curl http://localhost:8003/api/v1/pools/WARDA-ICU/fencing-token

# 3. Successful allocation with valid token (e.g., token 100)
curl -X POST http://localhost:8003/api/v1/beds/bed-warda-icu-01/allocate \
  -H "Content-Type: application/json" \
  -d '{"operation_id":"op-1","bed_id":"bed-warda-icu-01","allocation_id":"adm-1","fencing_token":100,"pool_id":"WARDA-ICU"}'

# 4. STALE FENCING REJECTION (Key Viva Criterion): Attempt allocation with stale token 50
curl -X POST http://localhost:8003/api/v1/beds/bed-warda-icu-02/allocate \
  -H "Content-Type: application/json" \
  -d '{"operation_id":"op-stale","bed_id":"bed-warda-icu-02","allocation_id":"adm-2","fencing_token":50,"pool_id":"WARDA-ICU"}'
# => Returns HTTP 409 Conflict: "Stale fencing token 50; pool WARDA-ICU highest token is 100"
```

### Step 3: Show Resource Analytics in Action
Start the Resource Analytics service:
```powershell
python -m services.resource_analytics.main
```
In another terminal:
```bash
# Query current summary
curl http://localhost:8008/api/v1/analytics/summary

# Query pool latest stats
curl http://localhost:8008/api/v1/analytics/pools/WARDA-ICU/latest
```
