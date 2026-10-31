"""Patient Registry domain operations and transactional persistence."""

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import TypeVar
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from starlette import status

from services.patient_registry.models import (
    AdmissionModel,
    IdempotencyModel,
    PatientDocumentModel,
    PatientModel,
    utc_now,
)
from services.patient_registry.schemas import (
    AdmissionCreate,
    AdmissionResponse,
    PatientCreate,
    PatientDocumentResponse,
    PatientDocumentType,
    PatientResponse,
)
from services.patient_registry.storage import DocumentStorage
from shared.observability.errors import AppException

logger = logging.getLogger("patient-registry")

T = TypeVar("T", PatientResponse, AdmissionResponse, PatientDocumentResponse)
MAX_DOCUMENT_BYTES = 10 * 1024 * 1024
ALLOWED_CONTENT_TYPES = {
    "application/pdf": (".pdf", b"%PDF-"),
    "image/jpeg": (".jpg", b"\xff\xd8\xff"),
    "image/png": (".png", b"\x89PNG\r\n\x1a\n"),
}


def _request_fingerprint(payload: BaseModel | Mapping[str, object] | bytes) -> str:
    if isinstance(payload, bytes):
        canonical = payload
    elif isinstance(payload, BaseModel):
        canonical = json.dumps(
            payload.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    else:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _existing_idempotent_response(
    session: Session,
    endpoint: str,
    key: str,
    request_hash: str,
    response_type: type[T],
) -> T | None:
    record = session.scalar(
        select(IdempotencyModel).where(
            IdempotencyModel.endpoint == endpoint,
            IdempotencyModel.idempotency_key == key,
        )
    )
    if record is None:
        return None
    if record.request_hash != request_hash:
        raise AppException(
            "IDEMPOTENCY_MISMATCH",
            "Idempotency-Key was already used with a different request",
            status.HTTP_409_CONFLICT,
        )
    return response_type.model_validate(json.loads(record.response_body))


def _store_idempotency(
    session: Session,
    endpoint: str,
    key: str,
    request_hash: str,
    response: T,
) -> None:
    session.add(
        IdempotencyModel(
            endpoint=endpoint,
            idempotency_key=key,
            request_hash=request_hash,
            response_status=status.HTTP_201_CREATED,
            response_body=response.model_dump_json(),
        )
    )


def _load_winning_idempotent_response(
    session_factory: sessionmaker[Session],
    endpoint: str,
    key: str,
    request_hash: str,
    response_type: type[T],
) -> T | None:
    with session_factory() as session:
        return _existing_idempotent_response(
            session, endpoint, key, request_hash, response_type
        )


def create_patient(
    session: Session,
    session_factory: sessionmaker[Session],
    payload: PatientCreate,
    idempotency_key: str,
) -> PatientResponse:
    endpoint = "POST:/api/v1/patients"
    fingerprint = _request_fingerprint(payload)
    existing = _existing_idempotent_response(
        session, endpoint, idempotency_key, fingerprint, PatientResponse
    )
    if existing is not None:
        return existing

    patient = PatientModel(
        patient_id=f"pat-{uuid4().hex}",
        created_at=utc_now(),
        **payload.model_dump(),
    )
    response = PatientResponse.model_validate(patient)
    session.add(patient)
    _store_idempotency(session, endpoint, idempotency_key, fingerprint, response)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        replay = _load_winning_idempotent_response(
            session_factory, endpoint, idempotency_key, fingerprint, PatientResponse
        )
        if replay is not None:
            return replay
        raise
    return response


def get_patient(session: Session, patient_id: str) -> PatientResponse:
    patient = session.get(PatientModel, patient_id)
    if patient is None:
        raise AppException("NOT_FOUND", "Patient was not found", status.HTTP_404_NOT_FOUND)
    return PatientResponse.model_validate(patient)


def create_admission(
    session: Session,
    session_factory: sessionmaker[Session],
    payload: AdmissionCreate,
    idempotency_key: str,
) -> AdmissionResponse:
    endpoint = "POST:/api/v1/admissions"
    fingerprint = _request_fingerprint(payload)
    existing = _existing_idempotent_response(
        session, endpoint, idempotency_key, fingerprint, AdmissionResponse
    )
    if existing is not None:
        return existing

    if session.get(PatientModel, payload.patient_id) is None:
        raise AppException("NOT_FOUND", "Patient was not found", status.HTTP_404_NOT_FOUND)

    admission = AdmissionModel(
        admission_id=f"adm-{uuid4().hex}",
        patient_id=payload.patient_id,
        status="OPEN",
        admitted_at=utc_now(),
    )
    response = AdmissionResponse.model_validate(admission)
    session.add(admission)
    _store_idempotency(session, endpoint, idempotency_key, fingerprint, response)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        replay = _load_winning_idempotent_response(
            session_factory, endpoint, idempotency_key, fingerprint, AdmissionResponse
        )
        if replay is not None:
            return replay
        raise
    return response


def get_admission(session: Session, admission_id: str) -> AdmissionResponse:
    admission = session.get(AdmissionModel, admission_id)
    if admission is None:
        raise AppException("NOT_FOUND", "Admission was not found", status.HTTP_404_NOT_FOUND)
    return AdmissionResponse.model_validate(admission)


def validate_document(content_type: str, content: bytes) -> str:
    extension_and_signature = ALLOWED_CONTENT_TYPES.get(content_type)
    if extension_and_signature is None:
        raise AppException(
            "INVALID_ARGUMENT",
            "Only PDF, JPEG, and PNG documents are accepted",
            status.HTTP_400_BAD_REQUEST,
        )
    extension, signature = extension_and_signature
    if not content.startswith(signature):
        raise AppException(
            "INVALID_ARGUMENT",
            "The uploaded file content does not match its declared type",
            status.HTTP_400_BAD_REQUEST,
        )
    if len(content) > MAX_DOCUMENT_BYTES:
        raise AppException(
            "INVALID_ARGUMENT",
            f"Document exceeds the {MAX_DOCUMENT_BYTES // (1024 * 1024)} MiB limit",
            status.HTTP_400_BAD_REQUEST,
        )
    if not content:
        raise AppException("INVALID_ARGUMENT", "Document must not be empty", status.HTTP_400_BAD_REQUEST)
    return extension


def upload_document(
    session: Session,
    session_factory: sessionmaker[Session],
    storage: DocumentStorage,
    patient_id: str,
    document_type: PatientDocumentType,
    content_type: str,
    content: bytes,
    idempotency_key: str,
) -> PatientDocumentResponse:
    extension = validate_document(content_type, content)
    endpoint = f"POST:/api/v1/patients/{patient_id}/documents"
    fingerprint = _request_fingerprint(
        {
            "patient_id": patient_id,
            "document_type": document_type.value,
            "content_type": content_type,
            "content_sha256": hashlib.sha256(content).hexdigest(),
        }
    )
    existing = _existing_idempotent_response(
        session, endpoint, idempotency_key, fingerprint, PatientDocumentResponse
    )
    if existing is not None:
        return existing
    if session.get(PatientModel, patient_id) is None:
        raise AppException("NOT_FOUND", "Patient was not found", status.HTTP_404_NOT_FOUND)

    document_id = f"doc-{uuid4().hex}"
    object_name = f"patient-documents/{document_id}{extension}"
    storage.upload(object_name, content, content_type)

    document = PatientDocumentModel(
        document_id=document_id,
        patient_id=patient_id,
        document_type=document_type.value,
        storage_object=object_name,
        content_type=content_type,
        size_bytes=len(content),
        uploaded_at=utc_now(),
    )
    response = PatientDocumentResponse.model_validate(document)
    session.add(document)
    _store_idempotency(session, endpoint, idempotency_key, fingerprint, response)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        try:
            storage.delete(object_name)
        except AppException:
            logger.exception(
                "Could not remove an unreferenced uploaded document",
                extra={"event": "orphan_document_cleanup_failed"},
            )
        replay = _load_winning_idempotent_response(
            session_factory, endpoint, idempotency_key, fingerprint, PatientDocumentResponse
        )
        if replay is not None:
            return replay
        raise
    except Exception:
        session.rollback()
        try:
            storage.delete(object_name)
        except AppException:
            logger.exception(
                "Could not remove an unreferenced uploaded document",
                extra={"event": "orphan_document_cleanup_failed"},
            )
        raise
    return response


def get_patient_document(
    session: Session,
    storage: DocumentStorage,
    patient_id: str,
    document_id: str,
) -> tuple[PatientDocumentResponse, bytes]:
    document = session.scalar(
        select(PatientDocumentModel).where(
            PatientDocumentModel.patient_id == patient_id,
            PatientDocumentModel.document_id == document_id,
        )
    )
    if document is None:
        raise AppException("NOT_FOUND", "Patient document was not found", status.HTTP_404_NOT_FOUND)
    return PatientDocumentResponse.model_validate(document), storage.download(document.storage_object)
