#!/bin/bash
# ============================================================================
# Submit Flink SQL Job on Google Cloud Dataproc
# Owned by Member D (Jay Patel) - CS/SS G527 Task (e)
# ============================================================================

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-hospital-bed-mgmt-dev}"
REGION="${REGION:-asia-south1}"
CLUSTER_NAME="${DATAPROC_CLUSTER_NAME:-hospital-dataproc-flink}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka.confluent.cloud:9092}"

echo "Submitting Flink SQL Job to Dataproc Cluster: ${CLUSTER_NAME} in ${REGION}..."

# 1. Substitute Environment Variables into SQL template
SQL_FILE="/tmp/flink_job_rendered.sql"
sed "s|\${KAFKA_BOOTSTRAP_SERVERS}|${KAFKA_BOOTSTRAP_SERVERS}|g" services/resource_analytics/flink_job.sql > "${SQL_FILE}"

# 2. Upload SQL file to GCS Job Bucket
JOB_BUCKET="${PROJECT_ID}-flink-artifacts"
gsutil cp "${SQL_FILE}" "gs://${JOB_BUCKET}/jobs/flink_job.sql"

# 3. Submit Dataproc Flink Job
gcloud dataproc jobs submit flink \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --cluster="${CLUSTER_NAME}" \
    --filename="gs://${JOB_BUCKET}/jobs/flink_job.sql"

echo "Flink SQL Job submitted successfully."

