# Handoff for Nisarg — requirement (b) "serverless function" (pending your review)

**Status: PENDING.** Today's release keeps Notification unchanged. Nothing has been converted, applied or rerouted.

## What exists

- `services/notification` is a FastAPI app on **Cloud Run (service)**, `hospital-notification`, image
  `notification:initial`. It is triggered by **Eventarc** `hospital-notification-pubsub` from Pub/Sub topic
  `bed-notifications`, with dead-letter topic `bed-notifications-dlq` after 10 attempts.
- `POST /` decodes the push envelope (`handler.decode_push`) and validates the event against
  `contracts/events/notification.schema.json`. It records the notification in Firestore with event-ID
  deduplication and per-pool sequence-gap detection (`store.py`), then logs the staff message.
  - Malformed messages return 200, so they are acknowledged and never retried.
  - Firestore errors return 503, so Pub/Sub redelivers.
- IaC: `infra/terraform/modules/notifications/main.tf` (`google_cloud_run_v2_service.notification`,
  `google_eventarc_trigger.notifications`, the imported subscription with its DLQ policy).

## The question

The brief says "serverless function (e.g. AWS Lambda, Google Cloud Functions…)". A Cloud Run service is
serverless and event-driven, but it is not deployed as a *function*. Either:
- (A) confirm with the instructor that this counts, and record the answer in the design document; or
- (B) deploy the same logic as a **Cloud Run function** (Cloud Functions 2nd gen), which is the smallest
  change that removes the doubt.

## Option B — proposed changes (separate branch, after your review)

1. **Code.** Add `services/notification/function.py` with a CloudEvent entrypoint that reuses the existing logic:
   ```python
   import functions_framework
   from services.notification.handler import BadMessage, render, validate
   from services.notification.store import FirestoreNotificationStore
   STORE = FirestoreNotificationStore()            # same Firestore collections and dedup as today

   @functions_framework.cloud_event
   def handle_bed_notification(cloud_event):
       event = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))
       try:
           validate(event)
       except BadMessage:
           return                                   # acknowledge: a malformed message never succeeds
       STORE.record(event, render(event))           # raising (e.g. Firestore unavailable) => Pub/Sub retries
   ```
   Add `functions-framework` to the function's requirements. Cloud Functions builds from source, so ship
   `services/notification`, `shared/` and `contracts/events/notification.schema.json` in the source zip.
2. **Terraform**, in `modules/notifications`:
   - `google_storage_bucket_object` for the source zip;
   - `google_cloudfunctions2_function` `hospital-notification-fn`: runtime python311, entry point
     `handle_bed_notification`, `service_config.service_account_email` = the existing notification GSA,
     `max_instance_count` 3, `min_instance_count` 0;
   - `event_trigger`: `google.cloud.pubsub.topic.v1.messagePublished` on `bed-notifications`,
     `retry_policy = RETRY_POLICY_RETRY`, trigger service account = the existing Eventarc GSA;
   - the dead-letter policy on the function's trigger subscription, mirroring the current
     `eventarc_subscription` handling;
   - then remove the old Eventarc trigger, so each message is handled once, not twice.
3. **Cut-over.** Deploy the function, verify it, then delete the old trigger and service in the same reviewed
   apply. Running both at once would double-process events; dedup prevents duplicate records, but it is wasteful.

## Verification (CLOUD-VERIFIED needed)

1. Publish one synthetic event, or allocate in the demo UI. Expect the function log `staff_notification`
   and a new Firestore notification document.
2. Re-publish the same `event_id`. Expect `notification_duplicate` and no second document.
3. Publish a malformed message. Expect `notification_rejected`, acknowledged, no retries.
4. Temporarily deny the function Firestore access (approval needed). After 10 attempts the message appears on
   `bed-notifications-dlq`.
5. Check Cloud Logging for the function's logs (`resource.type="cloud_run_revision"` with the function's service name).

Cost: within the Cloud Run functions free tier at demo volume. Rollback: re-apply the previous Terraform (Cloud Run
service + Eventarc trigger).
