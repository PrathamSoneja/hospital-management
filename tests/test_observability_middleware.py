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
