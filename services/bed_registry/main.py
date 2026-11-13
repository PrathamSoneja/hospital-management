"""
Bed Registry Application Entrypoint.
Owned by Member D (Jay Patel).
Initializes Database, FastAPI REST app, and gRPC Server.
"""
import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from services.bed_registry.db import init_engine, init_db, get_session_factory
from services.bed_registry.service import BedRegistryDomainService
from services.bed_registry.rest import router as bed_router, set_service
from services.bed_registry.grpc_server import create_grpc_server
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("bed-registry-service")

def seed_default_beds_if_empty(service: BedRegistryDomainService):
    """Seeds default beds for the standard scenario pools if database is empty."""
    default_pools = [
        ("WARDA-ICU", "ICU-A", 10),
        ("WARDA-HDU", "HDU-A", 10),
        ("WARDB-GEN", "GEN-B", 20),
        ("WARDB-ISO", "ISO-B", 5),
    ]
    total_seeded = 0
    for pool_id, ward, count in default_pools:
        existing = service.list_available_beds(pool_id)
        if len(existing) == 0:
            for i in range(1, count + 1):
                bed_id = f"bed-{pool_id.lower()}-{i:02d}"
                service.create_or_update_bed(
                    bed_id=bed_id,
                    pool_id=pool_id,
                    ward=ward,
                    status="AVAILABLE"
                )
                total_seeded += 1
    if total_seeded > 0:
        logger.info(f"Seeded {total_seeded} initial beds across pools.")

def build_app(start_grpc: bool = False, db_url: str | None = None, auto_seed: bool | None = None) -> FastAPI:
    # 1. Initialize DB Engine & Tables
    init_engine(db_url)
    init_db()

    session_factory = get_session_factory()
    domain_service = BedRegistryDomainService(session_factory)
    set_service(domain_service)

    # Auto-seed beds only if explicitly enabled via argument or AUTO_SEED_DEMO_BEDS=true
    should_seed = auto_seed if auto_seed is not None else (
        os.environ.get("AUTO_SEED_DEMO_BEDS", "false").lower() in ("true", "1")
    )
    if should_seed:
        try:
            seed_default_beds_if_empty(domain_service)
        except Exception as e:
            logger.warning(f"Could not auto-seed beds: {e}")

    def ready_check():
        try:
            # Check DB health by reading a pool token
            domain_service.get_highest_fencing_token("WARDA-ICU")
            return True
        except Exception:
            return False

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        grpc_server = None
        should_start_grpc = start_grpc or os.environ.get("START_GRPC", "false").lower() == "true"
        if should_start_grpc:
            grpc_port = int(os.environ.get("BED_REGISTRY_GRPC_PORT", 50053))
            grpc_server = create_grpc_server(domain_service, port=grpc_port)
            grpc_server.start()
            logger.info(f"Bed Registry gRPC server started on port {grpc_port}")

        app.state.ready_check = ready_check
        yield

        if grpc_server:
            grpc_server.stop(grace=0.5)
            logger.info("Bed Registry gRPC server stopped")

    app = FastAPI(
        title="Hospital Bed Registry Service",
        version="1.0.0",
        lifespan=lifespan
    )

    register_error_handlers(app, "bed-registry-service")
    app.state.ready_check = ready_check
    setup_observability_middleware(app, "bed-registry-service", ready_check=lambda: app.state.ready_check())
    app.include_router(bed_router)
    return app

app = build_app(start_grpc=False)

if __name__ == "__main__":
    rest_port = int(os.environ.get("BED_REGISTRY_REST_PORT", 8003))
    host = os.environ.get("BED_REGISTRY_REST_HOST", "0.0.0.0")
    os.environ["START_GRPC"] = "true"
    uvicorn.run("services.bed_registry.main:app", host=host, port=rest_port, reload=False)

