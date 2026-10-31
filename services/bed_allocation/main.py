"""
Bed Allocation Service Application Entrypoint.
Initializes PostgreSQL database, dependency clients, outbox worker, and FastAPI REST server.
"""
import os
import threading
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlalchemy.orm import sessionmaker
from services.bed_allocation.clients.grpc_clients import (
    GrpcBedRegistryClient, GrpcLockCoordinatorClient, GrpcStaffSchedulerClient,
    GrpcTriageClient, HttpPatientRegistryClient
)
from services.bed_allocation.db import get_engine
from services.bed_allocation.outbox.dispatcher import OutboxDispatcher, OutboxWorker
from services.bed_allocation.outbox.publishers import build_event_publisher_from_env
from services.bed_allocation.rest import router as allocation_router, set_service
from services.bed_allocation.service import BedAllocationDomainService
from shared.config.runtime import ConfigError, bounded_int, runtime_profile, validate_cloud_runtime
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("bed-allocation-service")

def _required_setting(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be configured; no local fake fallback is available")
    return value


def build_app(database_url: str = None, clients: dict | None = None, event_publisher=None) -> FastAPI:
    endpoints = ["LOCK_SERVICE_ADDR", "BED_SERVICE_ADDR", "STAFF_SERVICE_ADDR", "TRIAGE_SERVICE_ADDR", "PATIENT_SERVICE_URL"]
    if os.environ.get("ENABLE_OUTBOX_WORKER", "false").lower() == "true":
        endpoints.append("KAFKA_BOOTSTRAP_SERVERS")
    profile = validate_cloud_runtime(endpoints)
    rpc_timeout_s = bounded_int("RPC_TIMEOUT_MS", 2000, 100, 30000) / 1000
    engine = get_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    if clients is None:
        logger.info("Initializing Bed Allocation Service with real dependency adapters")
        lock_client = GrpcLockCoordinatorClient(_required_setting("LOCK_SERVICE_ADDR"), rpc_timeout_s)
        bed_client = GrpcBedRegistryClient(_required_setting("BED_SERVICE_ADDR"), rpc_timeout_s)
        staff_client = GrpcStaffSchedulerClient(_required_setting("STAFF_SERVICE_ADDR"), rpc_timeout_s)
        triage_client = GrpcTriageClient(_required_setting("TRIAGE_SERVICE_ADDR"), rpc_timeout_s)
        patient_client = HttpPatientRegistryClient(_required_setting("PATIENT_SERVICE_URL"), rpc_timeout_s)
    else:
        if profile not in {"test", "local"}:
            raise ConfigError("Injected local/test adapters are not permitted in a cloud profile")
        required_clients = {"lock", "bed", "staff", "triage", "patient"}
        missing = sorted(required_clients - clients.keys())
        if missing:
            raise ValueError(f"Explicit client injection is missing: {', '.join(missing)}")
        logger.warning("Allocation is using explicitly injected local/test adapters")
        lock_client = clients["lock"]
        bed_client = clients["bed"]
        staff_client = clients["staff"]
        triage_client = clients["triage"]
        patient_client = clients["patient"]
    domain_service = BedAllocationDomainService(
        session_factory=session_factory,
        patient_client=patient_client,
        triage_client=triage_client,
        lock_client=lock_client,
        bed_client=bed_client,
        staff_client=staff_client,
        owner_id=os.environ.get("POD_NAME", "bed-alloc-instance-1"),
        lock_lease_ms=bounded_int("LOCK_LEASE_MS", 10000, 1000, 120000),
        lock_wait_timeout_ms=bounded_int("LOCK_WAIT_TIMEOUT_MS", 5000, 100, 60000),
    )
    set_service(domain_service)

    outbox_worker = None
    if os.environ.get("ENABLE_OUTBOX_WORKER", "false").lower() == "true":
        # Fail at startup on missing broker configuration; never publish to an in-memory stand-in.
        publisher = event_publisher if event_publisher is not None else build_event_publisher_from_env()
        dispatcher = OutboxDispatcher(
            session_factory, publisher,
            owner_id=f"{os.environ.get('POD_NAME', 'bed-alloc-instance-1')}:outbox",
            claim_seconds=int(os.environ.get("OUTBOX_CLAIM_SECONDS", "30")),
            max_attempts=int(os.environ.get("OUTBOX_MAX_ATTEMPTS", "12")),
            max_backoff_seconds=int(os.environ.get("OUTBOX_MAX_BACKOFF_SECONDS", "300")),
        )
        outbox_worker = OutboxWorker(dispatcher, poll_seconds=float(os.environ.get("OUTBOX_POLL_SECONDS", "1")))

    @asynccontextmanager
    async def lifespan(app):
        enabled = os.environ.get("ENABLE_RECOVERY_WORKER", "false").lower() == "true"
        stop_event = threading.Event()

        def recovery_loop():
            while not stop_event.wait(float(os.environ.get("RECOVERY_POLL_SECONDS", "2"))):
                try:
                    domain_service.recover_pending()
                except Exception as error:
                    logger.error(f"Recovery pass failed ({type(error).__name__})")

        worker = None
        if enabled:
            worker = threading.Thread(target=recovery_loop, name="allocation-recovery", daemon=True)
            worker.start()
        if outbox_worker is not None:
            outbox_worker.start()
        try:
            yield
        finally:
            stop_event.set()
            if outbox_worker is not None and not outbox_worker.stop(timeout=15):
                # Unfinished claims expire and are reclaimed by another replica.
                logger.warning("Outbox worker did not stop before the shutdown deadline")
            if worker is not None:
                worker.join(timeout=5)

    app = FastAPI(
        title="Hospital Bed Allocation Service",
        version="1.0.0",
        lifespan=lifespan,
    )

    register_error_handlers(app, "bed-allocation-service")
    setup_observability_middleware(app, "bed-allocation-service")
    app.include_router(allocation_router)
    return app

if __name__ == "__main__":
    rest_port = int(os.environ.get("ALLOCATION_REST_PORT", 8002))
    host = os.environ.get("ALLOCATION_REST_HOST", "0.0.0.0")
    uvicorn.run(build_app(), host=host, port=rest_port, reload=False)
