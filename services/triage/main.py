"""
Triage Service Application Entrypoint.
Initializes FastAPI REST app and gRPC server.
"""
import os
import uvicorn
from contextlib import asynccontextmanager
import uuid
from fastapi import FastAPI
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.repository.firestore import FirestoreTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import RemoteLockValidator
from services.triage.lock_coordinator import GrpcLockCoordinatorClient
from services.triage.rest import router as triage_router, set_service
from services.triage.grpc_server import create_grpc_server
from shared.config.runtime import validate_cloud_runtime
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("triage-service")

def build_app(backend: str | None = None, start_grpc: bool = False) -> FastAPI:
    backend_mode = os.environ.get("TRIAGE_STORAGE_BACKEND", backend or "firestore").strip().lower()
    environment = os.environ.get("ENVIRONMENT", "production").strip().lower()
    fencing_mode = os.environ.get("TRIAGE_FENCING_MODE", "coordinator").strip().lower()
    if backend_mode not in {"firestore", "memory"}:
        raise ValueError(f"Unknown TRIAGE_STORAGE_BACKEND: {backend_mode}")
    if backend_mode == "memory" and environment != "test":
        raise ValueError("TRIAGE_STORAGE_BACKEND=memory is permitted only when ENVIRONMENT=test")
    if fencing_mode != "coordinator":
        raise ValueError(f"Unknown TRIAGE_FENCING_MODE: {fencing_mode}; only coordinator is supported")
    validate_cloud_runtime(["LOCK_COORDINATOR_ADDR"])

    logger.info("triage_configuration_loaded", extra={
        "event": "triage_configuration_loaded", "backend": backend_mode,
        "fencing_mode": fencing_mode,
    })
    repo = FirestoreTriageRepository() if backend_mode == "firestore" else InMemoryTriageRepository()
    lock_client = None
    target = os.environ.get("LOCK_COORDINATOR_ADDR", "").strip()
    if target:
        lock_client = GrpcLockCoordinatorClient(target)
    validator = RemoteLockValidator(lock_client.stub if lock_client else None)

    domain_service = TriageDomainService(
        repo=repo,
        validator=validator,
        lock_client=lock_client,
        owner_id=os.environ.get("TRIAGE_INSTANCE_ID", f"triage-{uuid.uuid4().hex[:12]}"),
    )
    set_service(domain_service)

    def ready_check():
        storage_ready = repo.check_ready(timeout_s=0.5) if hasattr(repo, "check_ready") else backend_mode == "memory"
        coordinator_ready = lock_client.check_ready(timeout_s=0.5) if lock_client else False
        return storage_ready and coordinator_ready

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        grpc_server = None
        should_start_grpc = start_grpc or os.environ.get("START_GRPC", "false").lower() == "true"
        if should_start_grpc:
            grpc_port = int(os.environ.get("TRIAGE_GRPC_PORT", 50052))
            grpc_server = create_grpc_server(domain_service, port=grpc_port)
            grpc_server.start()
            logger.info(f"Triage gRPC server started on port {grpc_port}")
        app.state.ready_check = ready_check
        yield
        if grpc_server:
            grpc_server.stop(grace=0.5)
            logger.info("Triage gRPC server stopped")
        if lock_client:
            lock_client.close()

    app = FastAPI(
        title="Hospital Triage Service",
        version="1.0.0",
        lifespan=lifespan
    )

    register_error_handlers(app, "triage-service")
    app.state.ready_check = ready_check
    setup_observability_middleware(app, "triage-service", ready_check=lambda: app.state.ready_check())
    app.include_router(triage_router)
    return app

app = build_app(start_grpc=False)

if __name__ == "__main__":
    rest_port = int(os.environ.get("TRIAGE_REST_PORT", 8001))
    host = os.environ.get("TRIAGE_REST_HOST", "0.0.0.0")
    os.environ["START_GRPC"] = "true"
    uvicorn.run("services.triage.main:app", host=host, port=rest_port, reload=False)
