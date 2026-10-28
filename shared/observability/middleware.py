"""
Pure ASGI observability middleware:
- Propagates X-Request-ID without BaseHTTPMiddleware thread/deadlock issues
- Emits Prometheus HTTP metrics
- Mounts /healthz, /readyz, and /metrics
"""
import time
import uuid
from typing import Callable
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from shared.observability.metrics import HTTP_REQUESTS_TOTAL, HTTP_REQUEST_DURATION_SECONDS

class ObservabilityASGIMiddleware:
    def __init__(self, app, service_name: str):
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract or generate X-Request-ID
        request_id = None
        for name, value in scope.get("headers", []):
            if name.lower() == b"x-request-id":
                request_id = value.decode("latin1")
                break
        if not request_id:
            request_id = f"req-{uuid.uuid4().hex[:12]}"

        if "state" not in scope:
            scope["state"] = {}
        scope["state"]["request_id"] = request_id

        start_time = time.perf_counter()
        status_code = [200]

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_code[0] = message.get("status", 200)
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode("latin1")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start_time
            endpoint = scope.get("path", "")
            method = scope.get("method", "GET")
            HTTP_REQUESTS_TOTAL.labels(
                service=self.service_name,
                method=method,
                endpoint=endpoint,
                status=str(status_code[0])
            ).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(
                service=self.service_name,
                method=method,
                endpoint=endpoint
            ).observe(duration)

def setup_observability_middleware(app: FastAPI, service_name: str, ready_check: Callable[[], bool] = lambda: True):
    # Add pure ASGI middleware
    app.add_middleware(ObservabilityASGIMiddleware, service_name=service_name)

    # Operational Health Endpoints (outside /api/v1)
    @app.get("/healthz", tags=["operational"])
    async def healthz():
        return {"status": "ok", "service": service_name}

    @app.get("/readyz", tags=["operational"])
    async def readyz():
        if ready_check():
            return {"status": "ready", "service": service_name}
        return Response(content='{"status": "not_ready"}', status_code=503, media_type="application/json")

    @app.get("/metrics", tags=["operational"])
    async def metrics():
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
