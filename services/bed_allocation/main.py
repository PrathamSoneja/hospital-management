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
from services.bed_allocation.clients.fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakeLockCoordinatorClient,
    FakePatientRegistryClient, FakeStaffSchedulerClient
)
from services.bed_allocation.clients.grpc_clients import (
    GrpcBedRegistryClient, GrpcLockCoordinatorClient, GrpcStaffSchedulerClient,
    GrpcTriageClient, HttpPatientRegistryClient
)
from services.bed_allocation.db import get_engine
from services.bed_allocation.outbox.dispatcher import FakeEventPublisher, OutboxDispatcher
from services.bed_allocation.rest import router as allocation_router, set_service
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("bed-allocation-service")

def build_app(database_url: str = None, use_fakes: bool = True) -> FastAPI:
    engine = get_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    use_fakes_env = os.environ.get("USE_FAKES", str(use_fakes)).lower() == "true"
    logger.info(f"Initializing Bed Allocation Service. USE_FAKES={use_fakes_env}")

    if use_fakes_env:
        patient_client = FakePatientRegistryClient()
        # Seed default test admissions
        patient_client.seed_admission("adm-101", "pat-101", "OPEN")
        patient_client.seed_admission("adm-102", "pat-102", "OPEN")
        patient_client.seed_admission("adm-closed", "pat-closed", "DISCHARGED")

        triage_repo = InMemoryTriageRepository()
        triage_domain = TriageDomainService(triage_repo, MockFencingValidator(True))
        triage_domain.set_triage("adm-101", 1, "WARDA-ICU")
        triage_domain.set_triage("adm-102", 2, "WARDA-ICU")
        triage_client = DirectTriageClient(triage_domain)

        lock_client = FakeLockCoordinatorClient()
        bed_client = FakeBedRegistryClient()
        for i in range(1, 11):
            bed_client.seed_bed(f"bed-icu-{i:02d}", "WARDA-ICU", "ICU-A")

        staff_client = FakeStaffSchedulerClient(initial_capacity=50)
        publisher = FakeEventPublisher()
    else:
        lock_addr = os.environ.get("LOCK_SERVICE_ADDR", "localhost:50051")
        bed_addr = os.environ.get("BED_SERVICE_ADDR", "localhost:50053")
        staff_addr = os.environ.get("STAFF_SERVICE_ADDR", "localhost:50054")
        triage_addr = os.environ.get("TRIAGE_SERVICE_ADDR", "localhost:50052")
        patient_url = os.environ.get("PATIENT_SERVICE_URL", "http://localhost:8000/api/v1")

        lock_client = GrpcLockCoordinatorClient(lock_addr)
        bed_client = GrpcBedRegistryClient(bed_addr)
        staff_client = GrpcStaffSchedulerClient(staff_addr)
        triage_client = GrpcTriageClient(triage_addr)
        patient_client = HttpPatientRegistryClient(patient_url)
        publisher = FakeEventPublisher() # Can be swapped with GCP Pub/Sub and Kafka producers

    dispatcher = OutboxDispatcher(session_factory, publisher)
    domain_service = BedAllocationDomainService(
        session_factory=session_factory,
        patient_client=patient_client,
        triage_client=triage_client,
        lock_client=lock_client,
        bed_client=bed_client,
        staff_client=staff_client,
        owner_id=os.environ.get("POD_NAME", "bed-alloc-instance-1")
    )
    set_service(domain_service)

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
        try:
            yield
        finally:
            stop_event.set()
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

app = build_app()

if __name__ == "__main__":
    rest_port = int(os.environ.get("ALLOCATION_REST_PORT", 8002))
    host = os.environ.get("ALLOCATION_REST_HOST", "0.0.0.0")
    uvicorn.run("services.bed_allocation.main:app", host=host, port=rest_port, reload=False)
