#!/bin/bash
# ============================================================================
# Submit Flink SQL Job on Google Cloud Dataproc
# Owned by Member D (Jay Patel) - CS/SS G527 Task (e)
# Platform: Confluent Cloud Managed Kafka (SASL_SSL / PLAIN)
# ============================================================================

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-hospital-bed-mgmt-dev}"
REGION="${REGION:-asia-south1}"
CLUSTER_NAME="${DATAPROC_CLUSTER_NAME:-hospital-dataproc-flink}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-pkc-xxxx.asia-south1.gcp.confluent.cloud:9092}"
KAFKA_SASL_USERNAME="${KAFKA_SASL_USERNAME:-}"
KAFKA_SASL_PASSWORD="${KAFKA_SASL_PASSWORD:-}"
JOB_BUCKET="${FLINK_JOB_BUCKET:-${PROJECT_ID}-flink-artifacts}"

echo "Submitting Flink SQL Job to Dataproc Cluster: ${CLUSTER_NAME} in ${REGION} (Confluent Cloud Kafka)..."

# 1. Substitute Environment Variables into SQL template
SQL_FILE="/tmp/flink_job_rendered.sql"
sed \
    -e "s|\${KAFKA_BOOTSTRAP_SERVERS}|${KAFKA_BOOTSTRAP_SERVERS}|g" \
    -e "s|\${KAFKA_SASL_USERNAME}|${KAFKA_SASL_USERNAME}|g" \
    -e "s|\${KAFKA_SASL_PASSWORD}|${KAFKA_SASL_PASSWORD}|g" \
    services/resource_analytics/flink_job.sql > "${SQL_FILE}"

# 2. Upload SQL file to GCS Job Bucket
gsutil cp "${SQL_FILE}" "gs://${JOB_BUCKET}/jobs/flink_job.sql"

# 3. Submit Dataproc Flink Job with state checkpoints and connector properties
gcloud dataproc jobs submit flink \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --cluster="${CLUSTER_NAME}" \
    --filename="gs://${JOB_BUCKET}/jobs/flink_job.sql" \
    --properties="state.checkpoints.dir=gs://${JOB_BUCKET}/checkpoints,execution.checkpointing.interval=60000,execution.checkpointing.min-pause=10000"

echo "Flink SQL Job submitted successfully to Dataproc pointing to Confluent Cloud."

