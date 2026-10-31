import pytest
import httpx

from services.bed_allocation.clients.grpc_clients import HttpPatientRegistryClient
from shared.observability.errors import AppException


class StubResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


class StubHttpClient:
    def __init__(self, response):
        self.response = response
        self.request = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def get(self, url, headers):
        self.request = (url, headers)
        return self.response


def test_admission_adapter_uses_registry_identity_and_status(monkeypatch):
    client = StubHttpClient(StubResponse(200, {
        "admission_id": "adm-123",
        "patient_id": "pat-456",
        "status": "DISCHARGED",
    }))
    monkeypatch.setattr(httpx, "Client", lambda **_: client)

    result = HttpPatientRegistryClient("http://registry/api/v1").get_admission("adm-123", "req-1")

    assert result.model_dump() == {
        "admission_id": "adm-123", "patient_id": "pat-456", "status": "DISCHARGED"
    }
    assert client.request == (
        "http://registry/api/v1/admissions/adm-123", {"X-Request-ID": "req-1"}
    )


@pytest.mark.parametrize("payload", [
    {"admission_id": "adm-123", "status": "OPEN"},
    {"admission_id": "adm-other", "patient_id": "pat-456", "status": "OPEN"},
    {"admission_id": "adm-123", "patient_id": " ", "status": "OPEN"},
])
def test_admission_adapter_rejects_incomplete_or_mismatched_contract(monkeypatch, payload):
    client = StubHttpClient(StubResponse(200, payload))
    monkeypatch.setattr(httpx, "Client", lambda **_: client)

    with pytest.raises(AppException) as error:
        HttpPatientRegistryClient("http://registry/api/v1").get_admission("adm-123", "req-1")

    assert error.value.status_code == 503
    assert error.value.code == "UPSTREAM_CONTRACT_ERROR"


def test_admission_adapter_preserves_not_found(monkeypatch):
    monkeypatch.setattr(httpx, "Client", lambda **_: StubHttpClient(StubResponse(404, {})))

    assert HttpPatientRegistryClient("http://registry/api/v1").get_admission("missing", "req-1") is None
