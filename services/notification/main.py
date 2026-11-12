"""Notification service (Nisarg, task b): Cloud Run handler for bed events pushed by Eventarc from Pub/Sub."""
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from services.notification.handler import BadMessage, decode_push, render, validate
from services.notification.metrics import count, count_missing
from services.notification.store import FirestoreNotificationStore
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

SERVICE = "notification-service"
logger = setup_logger(SERVICE)


def build_app(store):
    app = FastAPI(title="Hospital Notification Service", version="1.0.0")
    setup_observability_middleware(app, SERVICE, ready_check=store.check)

    # Eventarc delivers each Pub/Sub message as a POST to "/".
    @app.post("/")
    async def receive(request: Request):
        try:
            body = await request.json()
        except ValueError:
            body = None
        event = None
        try:
            event, _ = decode_push(body)
            validate(event)
        except BadMessage as error:
            count(SERVICE, event, "rejected")
            # Acknowledge (2xx) on purpose: a malformed message would fail on every retry.
            logger.error("notification_rejected", extra={"event": "notification_rejected"})
            return JSONResponse({"status": "rejected", "reason": str(error)}, status_code=200)

        text = render(event)
        log_fields = {"request_id": event["request_id"], "allocation_id": event["allocation_id"],
                      "pool_id": event["pool_id"]}
        try:
            outcome = store.record(event, text)
        except Exception:
            # Firestore trouble is temporary: answer 503 so Pub/Sub delivers this message again.
            logger.exception("notification_store_unavailable",
                             extra={"event": "notification_store_unavailable", **log_fields})
            count(SERVICE, event, "retry")    
            return JSONResponse({"status": "retry"}, status_code=503)

        if not outcome.is_new:
            count(SERVICE, event, "duplicate")
            logger.info("duplicate notification skipped", extra={"event": "notification_duplicate", **log_fields})
            return {"status": "duplicate", "event_id": event["event_id"]}

        if outcome.missing:
            count_missing(SERVICE, event["pool_id"], outcome.missing)
            logger.warning(f"{outcome.missing} notification(s) missing before sequence {event['stream_sequence']}",
                           extra={"event": "notification_gap", **log_fields})
        logger.info(text, extra={"event": "staff_notification", **log_fields})
        count(SERVICE, event, "delivered")
        return {"status": "delivered", "event_id": event["event_id"], "message": text,
                "missing_before": outcome.missing, "late": outcome.late}

    return app


if __name__ == "__main__":
    project = os.environ.get("FIRESTORE_PROJECT_ID") or os.environ["GOOGLE_CLOUD_PROJECT"]
    # Cloud Run tells the container which port to listen on.
    uvicorn.run(build_app(FirestoreNotificationStore(project)), host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))