"""Demo console: forwards only the allowlisted REST calls, with the idempotency/request headers."""
import httpx
from fastapi.testclient import TestClient

from services.demo_ui.main import build_app, is_allowed


def _client(handler):
    return TestClient(build_app(transport=httpx.MockTransport(handler)))


def test_allowlist_covers_the_demo_flow_and_nothing_else():
    assert is_allowed("allocation", "POST", "api/v1/allocations")
    assert is_allowed("allocation", "POST", "api/v1/allocations/abc-1/release")
    assert is_allowed("triage", "GET", "api/v1/waitlists/DEMO-ICU")
    assert not is_allowed("allocation", "POST", "api/v1/allocations/abc-1/cancel")     # not part of the demo
    assert not is_allowed("beds", "POST", "api/v1/beds/b1/allocate")                    # fencing-token path stays internal
    assert not is_allowed("patients", "POST", "api/v1/patients/p1/documents")
    assert not is_allowed("allocation", "GET", "api/v1/allocations/../../metrics")
    assert not is_allowed("allocation", "DELETE", "api/v1/allocations")


def test_proxy_forwards_body_query_and_idempotency_headers_only():
    seen = {}

    def handler(request: httpx.Request):
        seen.update(url=str(request.url), key=request.headers.get("idempotency-key"),
                    rid=request.headers.get("x-request-id"), cookie=request.headers.get("cookie"), body=request.content)
        return httpx.Response(201, json={"allocation_id": "a1", "status": "ALLOCATED"})

    response = _client(handler).post("/proxy/allocation/api/v1/allocations?x=1", json={"admission_id": "adm", "pool_id": "P"},
                                     headers={"Idempotency-Key": "k1", "X-Request-ID": "r1", "Cookie": "s=1"})
    assert response.status_code == 201 and response.json()["status"] == "ALLOCATED"
    assert seen["url"] == "http://bed-allocation-service:8000/api/v1/allocations?x=1"
    assert (seen["key"], seen["rid"], seen["cookie"]) == ("k1", "r1", None)
    assert b'"admission_id"' in seen["body"]


def test_refused_paths_never_reach_a_service_and_upstream_errors_are_502():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ConnectError("down")

    client = _client(handler)
    assert client.post("/proxy/beds/api/v1/beds/b1/allocate", json={}).status_code == 404
    assert client.get("/proxy/unknown/api/v1/x").status_code == 404
    assert calls == []
    assert client.get("/proxy/beds/api/v1/beds").status_code == 502


def test_status_reports_each_service_readiness_and_page_is_served():
    def handler(request):
        return httpx.Response(200 if "triage" in request.url.host else 503)

    client = _client(handler)
    status = client.get("/api/status").json()
    assert status["triage"] is True and status["allocation"] is False and len(status) == 6
    page = client.get("/")
    assert page.status_code == 200 and "Bed Management" in page.text
