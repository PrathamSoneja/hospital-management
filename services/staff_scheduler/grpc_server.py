"""gRPC StaffScheduler implementation for allocation-critical calls."""

from concurrent import futures
import logging
import os
import time

import grpc

from services.staff_scheduler.service import StaffSchedulerService
from shared.generated import staff_pb2, staff_pb2_grpc
from shared.observability.errors import AppException
from shared.observability.metrics import GRPC_REQUESTS_TOTAL, GRPC_REQUEST_DURATION_SECONDS

logger = logging.getLogger("staff-scheduler")


def _request_id(context, message_request_id: str = "") -> str:
    try:
        metadata = {item.key.lower(): item.value for item in context.invocation_metadata()}
    except (AttributeError, RuntimeError):
        metadata = {}
    return metadata.get("x-request-id") or message_request_id or "unknown"


def _grpc_code(exc: AppException) -> grpc.StatusCode:
    if exc.status_code == 409:
        return grpc.StatusCode.FAILED_PRECONDITION
    if exc.status_code == 503:
        return grpc.StatusCode.UNAVAILABLE
    if exc.status_code == 504:
        return grpc.StatusCode.DEADLINE_EXCEEDED
    if exc.status_code == 404:
        return grpc.StatusCode.NOT_FOUND
    return grpc.StatusCode.INVALID_ARGUMENT


class StaffSchedulerGrpcServicer(staff_pb2_grpc.StaffSchedulerServicer):
    def __init__(self, service: StaffSchedulerService) -> None:
        self.service = service

    def CheckCapacity(self, request, context):
        started = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.request_id)
        try:
            if not request.pool_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("pool_id is required")
                code = "INVALID_ARGUMENT"
                return staff_pb2.CheckCapacityResponse()
            count = self.service.check_capacity(request.pool_id)
            logger.info(
                "Staff capacity checked",
                extra={
                    "event": "staff_capacity_checked",
                    "request_id": request_id,
                    "pool_id": request.pool_id,
                    "available_count": count,
                },
            )
            return staff_pb2.CheckCapacityResponse(
                has_capacity=count > 0,
                available_count=count,
            )
        except AppException as exc:
            code = _grpc_code(exc).name
            context.set_code(_grpc_code(exc))
            context.set_details(exc.message)
            return staff_pb2.CheckCapacityResponse()
        except Exception as exc:
            logger.exception(
                "Staff capacity query failed",
                extra={"event": "staff_capacity_error", "request_id": request_id},
            )
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Staff capacity query failed")
            return staff_pb2.CheckCapacityResponse()
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="staff-scheduler", method="CheckCapacity", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(
                service="staff-scheduler", method="CheckCapacity"
            ).observe(time.perf_counter() - started)

    def AssignStaff(self, request, context):
        started = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.context.request_id)
        try:
            assignment = self.service.assign(request.context, request.allocation_id, request.bed_id)
            if assignment is None:
                return staff_pb2.AssignStaffResponse(
                    success=False,
                    error_message="STAFF_CAPACITY_EXHAUSTED",
                )
            return staff_pb2.AssignStaffResponse(
                success=True,
                staff_assignment_id=assignment.assignment_id,
                staff_id=assignment.staff_id,
            )
        except AppException as exc:
            grpc_code = _grpc_code(exc)
            code = grpc_code.name
            context.set_code(grpc_code)
            context.set_details(f"{exc.code}:{exc.message}")
            return staff_pb2.AssignStaffResponse(success=False, error_message=exc.code)
        except Exception:
            logger.exception(
                "Staff assignment failed",
                extra={
                    "event": "staff_assignment_error",
                    "request_id": request_id,
                    "allocation_id": request.allocation_id,
                    "pool_id": request.context.pool_id,
                },
            )
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Staff assignment failed")
            return staff_pb2.AssignStaffResponse(success=False, error_message="INTERNAL")
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="staff-scheduler", method="AssignStaff", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(
                service="staff-scheduler", method="AssignStaff"
            ).observe(time.perf_counter() - started)

    def ReleaseAssignment(self, request, context):
        started = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.context.request_id)
        try:
            success = self.service.release(
                request.context,
                request.staff_assignment_id,
                request.allocation_id,
            )
            return staff_pb2.ReleaseAssignmentResponse(
                success=success,
                error_message="" if success else "STAFF_ASSIGNMENT_NOT_FOUND",
            )
        except AppException as exc:
            grpc_code = _grpc_code(exc)
            code = grpc_code.name
            context.set_code(grpc_code)
            context.set_details(f"{exc.code}:{exc.message}")
            return staff_pb2.ReleaseAssignmentResponse(success=False, error_message=exc.code)
        except Exception:
            logger.exception(
                "Staff assignment release failed",
                extra={
                    "event": "staff_assignment_release_error",
                    "request_id": request_id,
                    "allocation_id": request.allocation_id,
                    "pool_id": request.context.pool_id,
                },
            )
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Staff assignment release failed")
            return staff_pb2.ReleaseAssignmentResponse(success=False, error_message="INTERNAL")
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="staff-scheduler", method="ReleaseAssignment", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(
                service="staff-scheduler", method="ReleaseAssignment"
            ).observe(time.perf_counter() - started)


def create_grpc_server(
    service: StaffSchedulerService,
    host: str | None = None,
    port: int | None = None,
) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    staff_pb2_grpc.add_StaffSchedulerServicer_to_server(
        StaffSchedulerGrpcServicer(service),
        server,
    )
    bind_host = host or os.environ.get("STAFF_SCHEDULER_GRPC_HOST", "0.0.0.0")
    bind_port = port or int(os.environ.get("STAFF_SCHEDULER_GRPC_PORT", "50051"))
    bound_port = server.add_insecure_port(f"{bind_host}:{bind_port}")
    if bound_port == 0:
        raise RuntimeError(f"Unable to bind Staff Scheduler gRPC server to {bind_host}:{bind_port}")
    return server
