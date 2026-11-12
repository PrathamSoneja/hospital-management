"""Notification service (Nisarg, task b): Cloud Run handler for bed events pushed by Eventarc from Pub/Sub."""
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from services.notification.handler import BadMessage, decode_push, render, validate
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

SERVICE = "notification-service"
logger = setup_logger(SERVICE)


def build_app():
    app = FastAPI(title="Hospital Notification Service", version="1.0.0")
    setup_observability_middleware(app, SERVICE)

    # Eventarc delivers each Pub/Sub message as a POST to "/".
    @app.post("/")
    async def receive(request: Request):
        try:
            body = await request.json()
        except ValueError:
            body = None
        try:
            event, _ = decode_push(body)
            validate(event)
        except BadMessage as error:
            # Acknowledge (2xx) on purpose: a malformed message would fail on every retry.
            logger.error("notification_rejected", extra={"event": "notification_rejected"})
            return JSONResponse({"status": "rejected", "reason": str(error)}, status_code=200)

        text = render(event)
        logger.info(text, extra={
            "event": "staff_notification",
            "request_id": event["request_id"],
            "allocation_id": event["allocation_id"],
            "pool_id": event["pool_id"],
        })
        return {"status": "delivered", "event_id": event["event_id"], "message": text}

    return app


app = build_app()

if __name__ == "__main__":
    # Cloud Run tells the container which port to listen on.
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))