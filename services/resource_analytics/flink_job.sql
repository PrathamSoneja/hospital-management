-- ============================================================================
-- Apache Flink SQL Stream Processing Job for Hospital Resource Analytics
-- Owned by Member D (Jay Patel) - CS/SS G527 Task (e)
-- Runs on Google Cloud Dataproc Flink Cluster
-- ============================================================================

-- 1. Source Table: Kafka topic 'bed-events'
CREATE TABLE bed_events (
    schema_version VARCHAR,
    event_id VARCHAR,
    stream_sequence BIGINT,
    event_type VARCHAR,
    event_time TIMESTAMP(3),
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
    -- Event time watermark definition (5-second tolerance for out-of-order arrival)
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bed-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-resource-analytics-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. Sink Table: Kafka topic 'bed-stats'
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
    'format' = 'json'
);

-- 3. Stateful 1-minute Tumbling Window Aggregation
INSERT INTO bed_stats
SELECT
    '1.0.0' AS schema_version,
    pool_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS window_end,
    CAST(SUM(CASE WHEN event_type = 'ALLOCATION_REQUESTED' THEN 1 ELSE 0 END) AS INT) AS requests,
    CAST(SUM(CASE WHEN event_type = 'BED_ALLOCATED' THEN 1 ELSE 0 END) AS INT) AS allocated,
    CAST(SUM(CASE WHEN event_type = 'ALLOCATION_WAITLISTED' THEN 1 ELSE 0 END) AS INT) AS waitlisted,
    CAST(SUM(CASE WHEN event_type = 'ALLOCATION_CONFLICT' THEN 1 ELSE 0 END) AS INT) AS conflicts,
    CAST(SUM(occupancy_delta) AS INT) AS occupancy_at_window_end,
    AVG(CAST(lock_wait_ms AS DOUBLE)) AS average_lock_wait_ms,
    AVG(CAST(allocation_wait_ms AS DOUBLE)) AS average_allocation_wait_ms,
    CASE 
        WHEN SUM(CASE WHEN event_type = 'ALLOCATION_REQUESTED' THEN 1 ELSE 0 END) > 0 THEN
            CAST(SUM(CASE WHEN event_type IN ('ALLOCATION_WAITLISTED', 'ALLOCATION_CONFLICT') THEN 1 ELSE 0 END) AS DOUBLE) /
            CAST(SUM(CASE WHEN event_type = 'ALLOCATION_REQUESTED' THEN 1 ELSE 0 END) AS DOUBLE)
        ELSE 0.0
    END AS contention_rate
FROM TABLE(
    TUMBLE(TABLE bed_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY pool_id, window_start, window_end;

