from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from typing import Any

from services.patient_registry.main import create_app
from services.patient_registry.models import Base
from services.patient_registry.storage import DocumentStorage


class MemoryDocumentStorage(DocumentStorage):
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    def upload(self, object_name: str, content: bytes, content_type: str) -> None:
        if object_name in self.objects:
            raise RuntimeError("object already exists")
        self.objects[object_name] = (content, content_type)

    def download(self, object_name: str) -> bytes:
        return self.objects[object_name][0]

    def delete(self, object_name: str) -> None:
        self.objects.pop(object_name, None)


@pytest.fixture
def api() -> Iterator[tuple[TestClient, MemoryDocumentStorage]]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    storage = MemoryDocumentStorage()
    app = create_app(
        document_storage=storage,
        engine=engine,
    )
    assert app.state.session_factory is not None
    with TestClient(app) as client:
        yield client, storage
    engine.dispose()


def create_patient(
    client: TestClient,
    idempotency_key: str = "patient-create-1",
) -> dict[str, Any]:
    response = client.post(
        "/api/v1/patients",
        headers={"Idempotency-Key": idempotency_key},
        json={
            "first_name": "Asha",
            "last_name": "Shah",
            "date_of_birth": "1980-04-15",
            "phone": "+15555550100",
        },
    )
    assert response.status_code == 201
    return response.json()


def test_patient_and_admission_api_matches_allocation_client_contract(
    api: tuple[TestClient, MemoryDocumentStorage],
) -> None:
    client, _ = api
    patient = create_patient(client)

    get_patient_response = client.get(f"/api/v1/patients/{patient['patient_id']}")
    assert get_patient_response.status_code == 200
    assert get_patient_response.json()["patient_id"] == patient["patient_id"]

    admission_response = client.post(
        "/api/v1/admissions",
        headers={"Idempotency-Key": "admission-create-1"},
        json={"patient_id": patient["patient_id"]},
    )
    assert admission_response.status_code == 201
    admission = admission_response.json()
    assert admission["status"] == "OPEN"

    allocation_lookup = client.get(f"/api/v1/admissions/{admission['admission_id']}")
    assert allocation_lookup.status_code == 200
    assert allocation_lookup.json() == admission


def test_patient_creation_is_idempotent_and_rejects_key_reuse(
    api: tuple[TestClient, MemoryDocumentStorage],
) -> None:
    client, _ = api
    first = create_patient(client, "stable-key")
    replay = create_patient(client, "stable-key")

    assert replay["patient_id"] == first["patient_id"]

    mismatch = client.post(
        "/api/v1/patients",
        headers={"Idempotency-Key": "stable-key"},
        json={
            "first_name": "Different",
            "last_name": "Patient",
            "date_of_birth": "1980-04-15",
        },
    )
    assert mismatch.status_code == 409
    assert mismatch.json()["error"]["code"] == "IDEMPOTENCY_MISMATCH"


def test_patient_documents_are_private_and_retrievable(
    api: tuple[TestClient, MemoryDocumentStorage],
) -> None:
    client, storage = api
    patient = create_patient(client)
    content = b"%PDF-1.7\nexample referral"
    upload = client.post(
        f"/api/v1/patients/{patient['patient_id']}/documents",
        headers={"Idempotency-Key": "doc-upload-1"},
        data={"document_type": "REFERRAL_LETTER"},
        files={"file": ("referral.pdf", content, "application/pdf")},
    )
    assert upload.status_code == 201
    metadata = upload.json()
    assert metadata["size_bytes"] == len(content)
    assert len(storage.objects) == 1
    assert "referral.pdf" not in next(iter(storage.objects))

    download = client.get(
        f"/api/v1/patients/{patient['patient_id']}/documents/{metadata['document_id']}"
    )
    assert download.status_code == 200
    assert download.content == content
    assert download.headers["x-content-type-options"] == "nosniff"
    assert download.headers["cache-control"] == "private, no-store"


def test_document_upload_rejects_disallowed_and_mismatched_content(
    api: tuple[TestClient, MemoryDocumentStorage],
) -> None:
    client, storage = api
    patient = create_patient(client)
    url = f"/api/v1/patients/{patient['patient_id']}/documents"

    unsupported = client.post(
        url,
        headers={"Idempotency-Key": "doc-upload-svg"},
        data={"document_type": "ID_SCAN"},
        files={"file": ("scan.svg", b"<svg/>", "image/svg+xml")},
    )
    assert unsupported.status_code == 400
    assert unsupported.json()["error"]["code"] == "INVALID_ARGUMENT"

    mismatched = client.post(
        url,
        headers={"Idempotency-Key": "doc-upload-invalid"},
        data={"document_type": "ID_SCAN"},
        files={"file": ("scan.pdf", b"not a PDF", "application/pdf")},
    )
    assert mismatched.status_code == 400
    assert storage.objects == {}


def test_missing_admission_uses_uniform_error_response(
    api: tuple[TestClient, MemoryDocumentStorage],
) -> None:
    client, _ = api
    response = client.get("/api/v1/admissions/adm-missing", headers={"X-Request-ID": "req-test"})

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Admission was not found",
            "request_id": "req-test",
            "retryable": False,
        }
    }
