"""
Triage Service Application Entrypoint.
Initializes FastAPI REST app and gRPC server.
"""
import os
import threading
import uvicorn
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from services.triage.models import TriageRecord
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.repository.firestore import FirestoreTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator, RemoteLockValidator
from services.triage.rest import router as triage_router, set_service
from services.triage.grpc_server import create_grpc_server
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("triage-service")

def build_app(backend: str = "memory", start_grpc: bool = False) -> FastAPI:
    backend_mode = os.environ.get("TRIAGE_STORAGE_BACKEND", backend)
    logger.info(f"Initializing Triage Service with backend: {backend_mode}")

    if backend_mode == "firestore":
        repo = FirestoreTriageRepository()
    else:
        repo = InMemoryTriageRepository()

    validator = MockFencingValidator(accept_all=True)
    domain_service = TriageDomainService(repo=repo, validator=validator)
    set_service(domain_service)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        grpc_server = None
        should_start_grpc = start_grpc or os.environ.get("START_GRPC", "false").lower() == "true"
        if should_start_grpc:
            grpc_port = int(os.environ.get("TRIAGE_GRPC_PORT", 50052))
            grpc_server = create_grpc_server(domain_service, port=grpc_port)
            grpc_server.start()
            logger.info(f"Triage gRPC server started on port {grpc_port}")
        yield
        if grpc_server:
            grpc_server.stop(grace=0.5)
            logger.info("Triage gRPC server stopped")

    app = FastAPI(
        title="Hospital Triage Service",
        version="1.0.0",
        lifespan=lifespan
    )

    register_error_handlers(app, "triage-service")
    setup_observability_middleware(app, "triage-service")
    app.include_router(triage_router)
    return app

app = build_app(start_grpc=False)

if __name__ == "__main__":
    rest_port = int(os.environ.get("TRIAGE_REST_PORT", 8001))
    host = os.environ.get("TRIAGE_REST_HOST", "0.0.0.0")
    os.environ["START_GRPC"] = "true"
    uvicorn.run("services.triage.main:app", host=host, port=rest_port, reload=False)
