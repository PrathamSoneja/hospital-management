#!/bin/bash
# ============================================================================
# Submit the Flink SQL job (services/resource_analytics/flink_job.sql) to the Dataproc Flink cluster.
# Owner: Member D (Jay Patel) - CS/SS G527 requirement (e). Run only after approval, once Confluent is
# provisioned (RUNBOOK.md §A.5).
#
# Security: the Kafka credentials are read from Secret Manager into shell variables, substituted into the
# SQL in memory and piped over an IAP SSH tunnel into a 0600 file on the master, which is shredded after
# submission. Nothing containing credentials is written to GCS or the local disk, and the script never
# echoes them (no `set -x`).
#
# Prerequisites (CHECKLIST.md): cluster RUNNING; Kafka topics bed-events/bed-stats; secret versions
# kafka-flink-sasl-username/-password; the Kafka SQL connector jar staged at $FLINK_KAFKA_CONNECTOR_JAR
# (matching the cluster's Flink version); firewall rule for IAP SSH (35.235.240.0/20 -> tcp:22, tag dataproc);
# the operator has roles/iap.tunnelResourceAccessor and compute OS Login / SSH access to the master.
# ============================================================================
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-hospital-management-514618}"
REGION="${REGION:-asia-south1}"
CLUSTER_NAME="${DATAPROC_CLUSTER_NAME:-hospital-dataproc-flink}"
JOB_BUCKET="${FLINK_JOB_BUCKET:-hospital-flink-514618}"
: "${KAFKA_BOOTSTRAP_SERVERS:?set KAFKA_BOOTSTRAP_SERVERS (Terraform output confluent_bootstrap_endpoint, host:9092)}"
: "${FLINK_KAFKA_CONNECTOR_JAR:?set FLINK_KAFKA_CONNECTOR_JAR=gs://${JOB_BUCKET}/jars/flink-sql-connector-kafka-<version>.jar}"
USER_SECRET="${KAFKA_FLINK_USERNAME_SECRET:-kafka-flink-sasl-username}"
PASS_SECRET="${KAFKA_FLINK_PASSWORD_SECRET:-kafka-flink-sasl-password}"

SQL_TEMPLATE="$(dirname "$0")/flink_job.sql"
MASTER="${CLUSTER_NAME}-m"
ZONE="$(gcloud dataproc clusters describe "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT_ID" \
        --format='value(config.gceClusterConfig.zoneUri.basename())')"
STATE="$(gcloud dataproc clusters describe "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT_ID" --format='value(status.state)')"
[ "$STATE" = "RUNNING" ] || { echo "cluster $CLUSTER_NAME is $STATE; start it first (approval required)" >&2; exit 1; }

KAFKA_SASL_USERNAME="$(gcloud secrets versions access latest --secret="$USER_SECRET" --project="$PROJECT_ID")"
KAFKA_SASL_PASSWORD="$(gcloud secrets versions access latest --secret="$PASS_SECRET" --project="$PROJECT_ID")"

render() {
  # Session settings first: YARN session target, exactly-once checkpoints in GCS, readable job name.
  cat <<EOF
SET 'execution.target' = 'yarn-session';
SET 'pipeline.name' = 'hospital-bed-stats-1m';
-- --- VIVA (Member D - Jay): Exactly-once state checkpoints persisted to GCS every 60s ---
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 'gs://${JOB_BUCKET}/checkpoints';
EOF
  # Plain string substitution (no sed, so credentials with special characters are safe).
  python3 - "$SQL_TEMPLATE" <<'PY'
import os, sys
text = open(sys.argv[1], encoding="utf-8").read()
for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"):
    value = os.environ[name]
    if '"' in value or "'" in value:
        sys.exit(f"{name} contains a quote character; refusing to render")
    text = text.replace("${" + name + "}", value)
sys.stdout.write(text)
PY
}
export KAFKA_BOOTSTRAP_SERVERS KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD

REMOTE='set -euo pipefail
umask 077
JAR=/tmp/flink-sql-connector-kafka.jar
gsutil -q cp "'"$FLINK_KAFKA_CONNECTOR_JAR"'" "$JAR"
SQL=$(mktemp /tmp/bed-stats-XXXXXX.sql)
trap "shred -u \"$SQL\" 2>/dev/null || rm -f \"$SQL\"" EXIT
cat > "$SQL"
if ! yarn application -list -appStates RUNNING 2>/dev/null | grep -q "hospital-flink-session"; then
  /usr/lib/flink/bin/yarn-session.sh -d -nm hospital-flink-session >/tmp/yarn-session.log 2>&1
  sleep 15
fi
# ---------------------------------------------------------------------------
# Flink 1.15 sql-client cancels submitted jobs during its shutdown
# (LocalExecutor.closeSession -> cancelOperation for every running job).
# Workaround: run it in the background, poll until the streaming job
# appears on the session cluster, then SIGKILL the client before its
# teardown can issue the cancel REST call.
# ---------------------------------------------------------------------------
/usr/lib/flink/bin/sql-client.sh -j "$JAR" -f "$SQL" >/tmp/sql-client.log 2>&1 &
CLIENT_PID=$!
echo "Waiting for job hospital-bed-stats-1m to reach RUNNING on the session cluster..."
for i in $(seq 1 90); do
  sleep 2
  if /usr/lib/flink/bin/flink list -r 2>/dev/null | grep -q "hospital-bed-stats-1m"; then
    echo "Job is RUNNING. Force-killing SQL client to prevent teardown cancel..."
    kill -9 $CLIENT_PID 2>/dev/null || true
    wait $CLIENT_PID 2>/dev/null || true
    echo "Done. The streaming job will continue on the YARN session cluster."
    /usr/lib/flink/bin/flink list -r
    exit 0
  fi
done
echo "ERROR: job did not appear within 3 minutes; check /tmp/sql-client.log and YARN logs."
cat /tmp/sql-client.log 2>/dev/null || true
kill $CLIENT_PID 2>/dev/null || true
exit 1'

echo "Submitting hospital-bed-stats-1m to ${CLUSTER_NAME} (${ZONE}) through IAP..."
render | gcloud compute ssh "$MASTER" --zone="$ZONE" --project="$PROJECT_ID" --tunnel-through-iap --command="$REMOTE"
unset KAFKA_SASL_USERNAME KAFKA_SASL_PASSWORD
echo "Submitted. Verify: Flink UI (Dataproc component gateway) shows job hospital-bed-stats-1m RUNNING with checkpoints;"
echo "bed-stats receives one record per pool per minute; Resource Analytics /api/v1/analytics/summary shows the windows."
