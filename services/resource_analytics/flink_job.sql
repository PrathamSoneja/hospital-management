-- ============================================================================
-- Apache Flink SQL job for Hospital Resource Analytics (requirement e)
-- Owner: Member D (Jay Patel). Runs on the Dataproc Flink component (image 2.1).
--
-- bed-events (Kafka, produced by Bed Allocation's outbox)
--   -> stateful 1-minute tumbling event-time windows per pool
--   -> bed-stats (separate Kafka results topic) -> Resource Analytics -> Firestore pool_stats
--
-- Event facts (services/bed_allocation/service.py, contracts/events/bed-event.schema.json):
--   * Allocation emits BED_ALLOCATED, ALLOCATION_WAITLISTED and BED_RELEASED (no REQUESTED/CONFLICT).
--   * event_time is Python isoformat() in UTC, e.g. 2026-11-15T12:00:00.123456+00:00, which Flink's JSON
--     TIMESTAMP parser does not accept; it is read as STRING and converted to an event-time column.
--   * Delivery is at-least-once: every count is COUNT(DISTINCT ...) so a redelivered event_id is counted
--     once, and the job stays an append-only window aggregation (required by the Kafka sink).
-- Credentials are substituted at submit time on the Dataproc master only (submit_flink_job.sh); this
-- template never contains them. The JAAS class is the one shaded inside flink-sql-connector-kafka.
-- ============================================================================

CREATE TABLE bed_events (
    schema_version VARCHAR,
    event_id VARCHAR,
    stream_sequence BIGINT,
    event_type VARCHAR,
    event_time VARCHAR,
    request_id VARCHAR,
    allocation_id VARCHAR,
    admission_id VARCHAR,
    patient_id VARCHAR,
    pool_id VARCHAR,
    bed_id VARCHAR,
    lock_wait_ms INT,
    lock_contended BOOLEAN,
    allocation_wait_ms INT,
    occupancy_delta INT,
    -- 'YYYY-MM-DDTHH:MM:SS...' -> TIMESTAMP(3) (second precision is enough for 1-minute windows)
    event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(event_time FROM 1 FOR 19), 'T', ' ')),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bed-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-resource-analytics-group',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE bed_stats (
    schema_version VARCHAR,
    pool_id VARCHAR,
    window_start VARCHAR,
    window_end VARCHAR,
    requests INT,
    allocated INT,
    waitlisted INT,
    conflicts INT,
    occupancy_at_window_end INT,
    average_lock_wait_ms DOUBLE,
    average_allocation_wait_ms DOUBLE,
    contention_rate DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'bed-stats',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";',
    'key.format' = 'raw',
    'key.fields' = 'pool_id',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

-- Metric definitions (per pool, per 1-minute event-time window):
--   requests   distinct allocations that settled in the window (allocated or waitlisted)
--   allocated  distinct BED_ALLOCATED events (includes promotions from the waitlist)
--   waitlisted distinct ALLOCATION_WAITLISTED events
--   conflicts  distinct allocations whose pool lease was contended (lock_contended = true)
--   occupancy_at_window_end  net bed change in the window (allocated - released)
--   contention_rate = conflicts / requests, clamped to [0, 1]
INSERT INTO bed_stats
SELECT
    '1.0.0' AS schema_version,
    pool_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_end,
    CAST(requests AS INT),
    CAST(allocated AS INT),
    CAST(waitlisted AS INT),
    CAST(conflicts AS INT),
    CAST(allocated - released AS INT) AS occupancy_at_window_end,
    average_lock_wait_ms,
    average_allocation_wait_ms,
    CASE WHEN requests > 0 THEN LEAST(1.0, CAST(conflicts AS DOUBLE) / CAST(requests AS DOUBLE)) ELSE 0.0 END
        AS contention_rate
FROM (
    SELECT
        pool_id, window_start, window_end,
        COUNT(DISTINCT CASE WHEN event_type IN ('BED_ALLOCATED', 'ALLOCATION_WAITLISTED') THEN allocation_id END) AS requests,
        COUNT(DISTINCT CASE WHEN event_type = 'BED_ALLOCATED' THEN event_id END) AS allocated,
        COUNT(DISTINCT CASE WHEN event_type = 'ALLOCATION_WAITLISTED' THEN event_id END) AS waitlisted,
        COUNT(DISTINCT CASE WHEN event_type = 'BED_RELEASED' THEN event_id END) AS released,
        COUNT(DISTINCT CASE WHEN lock_contended THEN allocation_id END) AS conflicts,
        AVG(CAST(lock_wait_ms AS DOUBLE)) AS average_lock_wait_ms,
        AVG(CAST(allocation_wait_ms AS DOUBLE)) AS average_allocation_wait_ms
    FROM TABLE(TUMBLE(TABLE bed_events, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
    GROUP BY pool_id, window_start, window_end
);
