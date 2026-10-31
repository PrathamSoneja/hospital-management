"""FastAPI routes for patients, admissions, and patient documents."""

import logging
from collections.abc import Iterator

from fastapi import APIRouter, Depends, File, Form, Header, Request, UploadFile
from fastapi.responses import Response
from sqlalchemy.orm import Session

from services.patient_registry.db import session_scope
from services.patient_registry.schemas import (
    AdmissionCreate,
    AdmissionResponse,
    PatientCreate,
    PatientDocumentResponse,
    PatientDocumentType,
    PatientResponse,
)
from services.patient_registry.service import (
    MAX_DOCUMENT_BYTES,
    create_admission,
    create_patient,
    get_admission,
    get_patient,
    get_patient_document,
    upload_document,
)
from services.patient_registry.storage import DocumentStorage
from shared.observability.errors import AppException
from starlette import status

logger = logging.getLogger("patient-registry")
router = APIRouter(prefix="/api/v1")


def get_db_session(request: Request) -> Iterator[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise AppException(
            "SERVICE_UNAVAILABLE",
            "Patient Registry database is not configured",
            status.HTTP_503_SERVICE_UNAVAILABLE,
            retryable=True,
        )
    yield from session_scope(factory)


def get_document_storage(request: Request) -> DocumentStorage:
    storage = getattr(request.app.state, "document_storage", None)
    if storage is None:
        raise AppException(
            "SERVICE_UNAVAILABLE",
            "Patient document storage is not configured",
            status.HTTP_503_SERVICE_UNAVAILABLE,
            retryable=True,
        )
    return storage


def _validate_idempotency_key(key: str) -> str:
    normalized = key.strip()
    if not normalized or len(normalized) > 128:
        raise AppException(
            "INVALID_ARGUMENT",
            "Idempotency-Key must contain 1 to 128 characters",
            status.HTTP_400_BAD_REQUEST,
        )
    return normalized


@router.post("/patients", response_model=PatientResponse, status_code=status.HTTP_201_CREATED)
def create_patient_endpoint(
    request: Request,
    payload: PatientCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    session: Session = Depends(get_db_session),
) -> PatientResponse:
    factory = request.app.state.session_factory
    result = create_patient(session, factory, payload, _validate_idempotency_key(idempotency_key))
    logger.info(
        "Patient record created or replayed",
        extra={"event": "patient_record_written", "request_id": getattr(request.state, "request_id", None)},
    )
    return result


@router.get("/patients/{patient_id}", response_model=PatientResponse)
def get_patient_endpoint(
    patient_id: str,
    session: Session = Depends(get_db_session),
) -> PatientResponse:
    return get_patient(session, patient_id)


@router.post("/admissions", response_model=AdmissionResponse, status_code=status.HTTP_201_CREATED)
def create_admission_endpoint(
    request: Request,
    payload: AdmissionCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    session: Session = Depends(get_db_session),
) -> AdmissionResponse:
    result = create_admission(
        session,
        request.app.state.session_factory,
        payload,
        _validate_idempotency_key(idempotency_key),
    )
    logger.info(
        "Admission record created or replayed",
        extra={"event": "admission_record_written", "request_id": getattr(request.state, "request_id", None)},
    )
    return result


@router.get("/admissions/{admission_id}", response_model=AdmissionResponse)
def get_admission_endpoint(
    admission_id: str,
    session: Session = Depends(get_db_session),
) -> AdmissionResponse:
    return get_admission(session, admission_id)


@router.post(
    "/patients/{patient_id}/documents",
    response_model=PatientDocumentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_patient_document_endpoint(
    patient_id: str,
    request: Request,
    document_type: PatientDocumentType = Form(...),
    file: UploadFile = File(...),
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    session: Session = Depends(get_db_session),
    storage: DocumentStorage = Depends(get_document_storage),
) -> PatientDocumentResponse:
    content = await file.read(MAX_DOCUMENT_BYTES + 1)
    if len(content) > MAX_DOCUMENT_BYTES:
        raise AppException(
            "INVALID_ARGUMENT",
            f"Document exceeds the {MAX_DOCUMENT_BYTES // (1024 * 1024)} MiB limit",
            status.HTTP_400_BAD_REQUEST,
        )
    if not file.content_type:
        raise AppException(
            "INVALID_ARGUMENT",
            "Content-Type is required for the uploaded document",
            status.HTTP_400_BAD_REQUEST,
        )

    response = upload_document(
        session,
        request.app.state.session_factory,
        storage,
        patient_id,
        document_type,
        file.content_type,
        content,
        _validate_idempotency_key(idempotency_key),
    )
    logger.info(
        "Patient document uploaded",
        extra={
            "event": "patient_document_uploaded",
            "request_id": getattr(request.state, "request_id", None),
        },
    )
    return response


@router.get("/patients/{patient_id}/documents/{document_id}")
def get_patient_document_endpoint(
    patient_id: str,
    document_id: str,
    session: Session = Depends(get_db_session),
    storage: DocumentStorage = Depends(get_document_storage),
) -> Response:
    document, content = get_patient_document(session, storage, patient_id, document_id)
    extension = {
        "application/pdf": "pdf",
        "image/jpeg": "jpg",
        "image/png": "png",
    }[document.content_type]
    return Response(
        content=content,
        media_type=document.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{document.document_id}.{extension}"',
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, no-store",
        },
    )
