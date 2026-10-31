"""Validated REST request and response schemas for Patient Registry."""

from datetime import date, datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AdmissionStatus(StrEnum):
    OPEN = "OPEN"
    DISCHARGED = "DISCHARGED"
    CANCELLED = "CANCELLED"


class RegistryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    @field_validator("*", mode="before")
    @classmethod
    def normalize_naive_datetimes(cls, value: object) -> object:
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class PatientCreate(BaseModel):
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)
    date_of_birth: date
    phone: str | None = Field(default=None, max_length=32)


class PatientResponse(RegistryResponse):
    patient_id: str
    first_name: str
    last_name: str
    date_of_birth: date
    phone: str | None
    created_at: datetime


class AdmissionCreate(BaseModel):
    patient_id: str = Field(min_length=1, max_length=64)


class AdmissionResponse(RegistryResponse):
    admission_id: str
    patient_id: str
    status: AdmissionStatus
    admitted_at: datetime
    discharged_at: datetime | None


class PatientDocumentType(StrEnum):
    REFERRAL_LETTER = "REFERRAL_LETTER"
    ID_SCAN = "ID_SCAN"


class PatientDocumentResponse(RegistryResponse):
    document_id: str
    patient_id: str
    document_type: PatientDocumentType
    content_type: str
    size_bytes: int
    uploaded_at: datetime
