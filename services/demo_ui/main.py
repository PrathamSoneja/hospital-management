"""
Demo console: one private web page that drives the real services for the assignment demo.

It is not a ninth business service. It serves static/index.html and forwards an explicit allowlist of
REST calls to the existing services inside the cluster (no other path or method is forwarded). It has
no database and no credentials. Open it with `kubectl -n hospital-system port-forward svc/demo-ui 8080:80`;
it is never exposed publicly because it can write data and has no authentication.
"""
import os
import re
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("demo-ui")
STATIC = Path(__file__).parent / "static"

# service -> (env var, default in-cluster base URL)
SERVICES = {
    "patients": ("PATIENT_SERVICE_URL", "http://patient-registry-service:8000"),
    "staff": ("STAFF_SERVICE_URL", "http://staff-scheduler-service:8000"),
    "triage": ("TRIAGE_SERVICE_URL", "http://triage-service:8000"),
    "allocation": ("ALLOCATION_SERVICE_URL", "http://bed-allocation-service:8000"),
    "beds": ("BED_SERVICE_URL", "http://bed-registry-service:8000"),
    "analytics": ("ANALYTICS_SERVICE_URL", "http://resource-analytics-service:8008"),
}

SEGMENT = r"[A-Za-z0-9._:-]{1,128}"
# (service, method, path relative to the service root). Anything else is refused with 404.
ALLOWED = [
    ("patients", "POST", r"api/v1/patients"),
    ("patients", "GET", rf"api/v1/patients/{SEGMENT}"),
    ("patients", "POST", r"api/v1/admissions"),
    ("patients", "GET", rf"api/v1/admissions/{SEGMENT}"),
    ("staff", "POST", r"api/v1/staff"),
    ("staff", "GET", rf"api/v1/staff/{SEGMENT}"),
    ("staff", "PUT", rf"api/v1/staff/{SEGMENT}/shifts/{SEGMENT}"),
    ("staff", "GET", rf"api/v1/staff/{SEGMENT}/shifts"),
    ("triage", "PUT", rf"api/v1/triage/{SEGMENT}"),
    ("triage", "GET", rf"api/v1/triage/{SEGMENT}"),
    ("triage", "GET", rf"api/v1/waitlists/{SEGMENT}"),
    ("allocation", "POST", r"api/v1/allocations"),
    ("allocation", "GET", r"api/v1/allocations"),
    ("allocation", "GET", rf"api/v1/allocations/{SEGMENT}"),
    ("allocation", "POST", rf"api/v1/allocations/{SEGMENT}/release"),
    ("beds", "GET", r"api/v1/beds"),
    ("beds", "POST", r"api/v1/beds"),
    ("analytics", "GET", r"api/v1/analytics/summary"),
    ("analytics", "GET", rf"api/v1/analytics/pools/{SEGMENT}/latest"),
]
ALLOWED_RE = [(s, m, re.compile(rf"^{p}$")) for s, m, p in ALLOWED]
FORWARD_HEADERS = ("content-type", "idempotency-key", "x-request-id")


def is_allowed(service: str, method: str, path: str) -> bool:
    return any(s == service and m == method and rx.match(path) for s, m, rx in ALLOWED_RE)


def build_app(transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    bases = {name: os.environ.get(env, default).rstrip("/") for name, (env, default) in SERVICES.items()}
    client = httpx.AsyncClient(timeout=float(os.environ.get("DEMO_UI_TIMEOUT_SECONDS", "15")), transport=transport)
    app = FastAPI(title="Hospital Bed Management — Demo Console", version="1.0.0")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/api/status")
    async def status():
        """Readiness of every backing service (GET /readyz), for the status strip."""
        result = {}
        for name, base in bases.items():
            try:
                result[name] = (await client.get(f"{base}/readyz", timeout=3)).status_code == 200
            except httpx.HTTPError:
                result[name] = False
        return result

    @app.api_route("/proxy/{service}/{path:path}", methods=["GET", "POST", "PUT"])
    async def proxy(service: str, path: str, request: Request):
        if service not in bases or not is_allowed(service, request.method, path):
            return JSONResponse({"error": "not available in the demo console"}, status_code=404)
        headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD_HEADERS}
        try:
            upstream = await client.request(request.method, f"{bases[service]}/{path}", params=request.query_params,
                                      content=await request.body(), headers=headers)
        except httpx.HTTPError as error:
            logger.warning("demo upstream unreachable service=%s error_type=%s", service, type(error).__name__)
            return JSONResponse({"error": f"{service} unreachable"}, status_code=502)
        return Response(upstream.content, status_code=upstream.status_code,
                        media_type=upstream.headers.get("content-type", "application/json"))

    setup_observability_middleware(app, "demo-ui", ready_check=lambda: True)
    return app


if __name__ == "__main__":
    uvicorn.run(build_app(), host=os.environ.get("DEMO_UI_HOST", "0.0.0.0"), port=int(os.environ.get("DEMO_UI_PORT", "8080")))
