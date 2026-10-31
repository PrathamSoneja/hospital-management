"""SQLAlchemy models owned by the Patient Registry database."""

from datetime import date, datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base metadata for the Patient Registry's dedicated database."""


def new_patient_id() -> str:
    return f"pat-{uuid4().hex}"


def new_admission_id() -> str:
    return f"adm-{uuid4().hex}"


def new_document_id() -> str:
    return f"doc-{uuid4().hex}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PatientModel(Base):
    __tablename__ = "patients"

    patient_id: Mapped[str] = mapped_column(String(64), primary_key=True, default=new_patient_id)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    date_of_birth: Mapped[date] = mapped_column(Date, nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    admissions: Mapped[list["AdmissionModel"]] = relationship(
        back_populates="patient",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    documents: Mapped[list["PatientDocumentModel"]] = relationship(
        back_populates="patient",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class AdmissionModel(Base):
    __tablename__ = "admissions"

    admission_id: Mapped[str] = mapped_column(String(64), primary_key=True, default=new_admission_id)
    patient_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("patients.patient_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="OPEN")
    admitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    discharged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    patient: Mapped[PatientModel] = relationship(back_populates="admissions")

    __table_args__ = (
        Index("idx_admissions_patient_status", "patient_id", "status"),
    )


class PatientDocumentModel(Base):
    __tablename__ = "patient_documents"

    document_id: Mapped[str] = mapped_column(String(64), primary_key=True, default=new_document_id)
    patient_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("patients.patient_id", ondelete="CASCADE"),
        nullable=False,
    )
    document_type: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_object: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    patient: Mapped[PatientModel] = relationship(back_populates="documents")

    __table_args__ = (
        Index("idx_patient_documents_patient", "patient_id", "uploaded_at"),
    )


class IdempotencyModel(Base):
    __tablename__ = "idempotency_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    response_status: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    __table_args__ = (
        UniqueConstraint("endpoint", "idempotency_key", name="uq_patient_idempotency_endpoint_key"),
    )
