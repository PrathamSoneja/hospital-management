"""Application entry point for the Patient Registry REST service."""

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

from services.patient_registry.db import database_is_ready, get_engine, get_session_factory
from services.patient_registry.rest import router
from services.patient_registry.storage import DocumentStorage, GoogleCloudDocumentStorage
from shared.observability.errors import AppException, register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

SERVICE_NAME = "patient-registry"
logger = setup_logger(SERVICE_NAME)


def create_app(
    database_url: str | None = None,
    document_storage: DocumentStorage | None = None,
    storage_bucket: str | None = None,
    create_schema: bool = False,
    engine: Engine | None = None,
) -> FastAPI:
    """Build the application; production schema changes run through Alembic."""
    resolved_database_url = database_url or os.environ.get("PATIENT_REGISTRY_DATABASE_URL")
    owns_engine = engine is None
    engine = engine or (get_engine(resolved_database_url) if resolved_database_url else None)

    resolved_bucket = storage_bucket or os.environ.get("PATIENT_DOCUMENTS_BUCKET")
    if document_storage is None and resolved_bucket:
        document_storage = GoogleCloudDocumentStorage(
            resolved_bucket,
            project_id=os.environ.get("GCP_PROJECT_ID"),
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if engine is None:
            raise RuntimeError("PATIENT_REGISTRY_DATABASE_URL must be configured")
        if document_storage is None:
            raise RuntimeError("PATIENT_DOCUMENTS_BUCKET must be configured for Patient Registry")
        if create_schema:
            from services.patient_registry.models import Base

            Base.metadata.create_all(bind=engine)
        yield
        if owns_engine and engine is not None:
            engine.dispose()

    app = FastAPI(
        title="Hospital Patient Registry",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.engine = engine
    app.state.session_factory = get_session_factory(engine) if engine is not None else None
    app.state.document_storage = document_storage

    register_error_handlers(app, SERVICE_NAME)

    @app.exception_handler(SQLAlchemyError)
    async def database_exception_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        logger.exception(
            "Patient Registry database operation failed",
            extra={"event": "database_error", "request_id": request_id},
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": {
                    "code": "SERVICE_UNAVAILABLE",
                    "message": "Patient Registry database is temporarily unavailable",
                    "request_id": request_id,
                    "retryable": True,
                }
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        logger.exception(
            "Unhandled Patient Registry error",
            extra={"event": "unhandled_exception", "request_id": request_id},
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An unexpected internal error occurred",
                    "request_id": request_id,
                    "retryable": False,
                }
            },
        )

    setup_observability_middleware(
        app,
        SERVICE_NAME,
        ready_check=lambda: database_is_ready(engine) and document_storage is not None,
    )
    app.include_router(router)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "services.patient_registry.main:app",
        host=os.environ.get("PATIENT_REGISTRY_REST_HOST", "0.0.0.0"),
        port=int(os.environ.get("PATIENT_REGISTRY_REST_PORT", "8000")),
        reload=False,
    )
