import logging

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.observability.middleware import setup_observability_middleware


def test_http_metrics_use_route_templates_not_resource_ids():
    app = FastAPI()
    setup_observability_middleware(app, "bounded-label-test")

    @app.get("/resources/{resource_id}")
    def read_resource(resource_id: str):
        return {"resource_id": resource_id}

    with TestClient(app) as client:
        assert client.get("/resources/one").status_code == 200
        assert client.get("/resources/two").status_code == 200
        body = client.get("/metrics").text

    assert 'endpoint="/resources/{resource_id}"' in body
    assert 'endpoint="/resources/one"' not in body
    assert 'endpoint="/resources/two"' not in body


def test_error_responses_are_logged_with_the_request_id_and_route_template(caplog):
    app = FastAPI()
    setup_observability_middleware(app, "error-log-test")

    @app.get("/resources/{resource_id}")
    def missing(resource_id: str):
        from fastapi import HTTPException
        raise HTTPException(status_code=404)

    with caplog.at_level("INFO", logger="error-log-test"), TestClient(app) as client:
        caplog.handler.setLevel("INFO")
        logging.getLogger("error-log-test").addHandler(caplog.handler)
        assert client.get("/resources/secret-id", headers={"X-Request-ID": "req-404"}).status_code == 404
        assert client.get("/healthz").status_code == 200
    errors = [r for r in caplog.records if getattr(r, "event", None) == "http_error_response"]
    assert [(r.request_id, r.getMessage()) for r in errors] == [("req-404", "GET /resources/{resource_id} -> 404")]
    assert "secret-id" not in caplog.text
