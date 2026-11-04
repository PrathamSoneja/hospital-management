"""Staff Scheduler process entry point; serves REST and gRPC in one container."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from services.staff_scheduler.grpc_server import create_grpc_server
from services.staff_scheduler.repository.base import StaffRepository
from services.staff_scheduler.repository.firestore import FirestoreStaffRepository
from services.staff_scheduler.service import StaffSchedulerService
from services.staff_scheduler.rest import router
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

SERVICE_NAME = "staff-scheduler"
logger = setup_logger(SERVICE_NAME)


def build_app(
    repository: StaffRepository | None = None,
    start_grpc: bool | None = None,
) -> FastAPI:
    """Create the application, allowing a test repository to be injected."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        active_repository = repository or FirestoreStaffRepository(
            project_id=os.environ.get("FIRESTORE_PROJECT_ID")
        )
        scheduler = StaffSchedulerService(active_repository)
        app.state.staff_repository = active_repository
        app.state.staff_scheduler = scheduler

        grpc_server = None
        should_start_grpc = (
            start_grpc
            if start_grpc is not None
            else os.environ.get("START_GRPC", "true").strip().lower() == "true"
        )
        if should_start_grpc:
            grpc_port = int(os.environ.get("STAFF_SCHEDULER_GRPC_PORT", "50051"))
            grpc_host = os.environ.get("STAFF_SCHEDULER_GRPC_HOST", "0.0.0.0")
            grpc_server = create_grpc_server(scheduler, host=grpc_host, port=grpc_port)
            grpc_server.start()
            logger.info(
                "Staff Scheduler gRPC server started",
                extra={"event": "grpc_server_started"},
            )
        yield
        if grpc_server is not None:
            grpc_server.stop(grace=1.0)
        close = getattr(active_repository, "close", None)
        if callable(close):
            close()

    app = FastAPI(
        title="Hospital Staff Scheduler",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.staff_repository = repository
    app.state.staff_scheduler = StaffSchedulerService(repository) if repository is not None else None
    register_error_handlers(app, SERVICE_NAME)
    setup_observability_middleware(
        app,
        SERVICE_NAME,
        ready_check=lambda: (
            app.state.staff_repository is not None
            and app.state.staff_repository.check_ready(timeout_s=0.5)
        ),
    )
    app.include_router(router)
    return app


app = build_app()


if __name__ == "__main__":
    uvicorn.run(
        "services.staff_scheduler.main:app",
        host=os.environ.get("STAFF_SCHEDULER_REST_HOST", "0.0.0.0"),
        port=int(os.environ.get("STAFF_SCHEDULER_REST_PORT", "8000")),
        reload=False,
    )
