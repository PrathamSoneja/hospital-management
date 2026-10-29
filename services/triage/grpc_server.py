"""
gRPC server implementation for TriageService.
Maps protobuf requests to TriageDomainService methods with proper error propagation.
"""
import grpc
import logging
from concurrent import futures
from shared.generated import triage_pb2, triage_pb2_grpc
from services.triage.service import TriageDomainService
from shared.observability.errors import AppException
from shared.observability.metrics import GRPC_REQUESTS_TOTAL, GRPC_REQUEST_DURATION_SECONDS
import time

logger = logging.getLogger("triage-service")

def _request_id(context, message_request_id: str = "") -> str:
    try:
        invocation_metadata = context.invocation_metadata()
    except (AttributeError, RuntimeError):
        invocation_metadata = ()
    metadata = dict((item.key.lower(), item.value) for item in invocation_metadata)
    return metadata.get("x-request-id") or message_request_id or "unknown"

class TriageGrpcServicer(triage_pb2_grpc.TriageServiceServicer):
    def __init__(self, service: TriageDomainService):
        self.service = service

    def GetTriage(self, request, context):
        start = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.request_id)
        logger.info("triage_grpc_get", extra={"event": "triage_grpc_get", "request_id": request_id})
        try:
            record = self.service.repo.get_triage(request.admission_id)
            if not record:
                return triage_pb2.GetTriageResponse(found=False)
            return triage_pb2.GetTriageResponse(
                found=True,
                triage=triage_pb2.TriageRecord(
                    admission_id=record.admission_id,
                    triage_level=record.triage_level,
                    pool_id=record.pool_id,
                    updated_at=record.updated_at
                )
            )
        except Exception as e:
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return triage_pb2.GetTriageResponse(found=False)
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="triage", method="GetTriage", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="triage", method="GetTriage").observe(time.perf_counter() - start)

    def Enqueue(self, request, context):
        start = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.context.request_id)
        logger.info("triage_grpc_enqueue", extra={"event": "triage_grpc_enqueue", "request_id": request_id, "pool_id": request.context.pool_id})
        try:
            entry, already_existed = self.service.enqueue(
                context=request.context,
                allocation_id=request.allocation_id,
                admission_id=request.admission_id
            )
            return triage_pb2.EnqueueWaitlistResponse(
                already_existed=already_existed,
                entry=triage_pb2.WaitlistEntry(
                    entry_id=entry.entry_id,
                    allocation_id=entry.allocation_id,
                    admission_id=entry.admission_id,
                    pool_id=entry.pool_id,
                    triage_level=entry.triage_level,
                    queued_at=entry.queued_at
                )
            )
        except AppException as e:
            grpc_code = {
                409: grpc.StatusCode.FAILED_PRECONDITION,
                503: grpc.StatusCode.UNAVAILABLE,
                504: grpc.StatusCode.DEADLINE_EXCEEDED,
            }.get(e.status_code, grpc.StatusCode.INVALID_ARGUMENT)
            code = grpc_code.name
            context.set_code(grpc_code)
            context.set_details(e.message)
            return triage_pb2.EnqueueWaitlistResponse()
        except Exception as e:
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return triage_pb2.EnqueueWaitlistResponse()
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="triage", method="Enqueue", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="triage", method="Enqueue").observe(time.perf_counter() - start)

    def PeekNext(self, request, context):
        start = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.request_id)
        logger.info("triage_grpc_peek_next", extra={"event": "triage_grpc_peek_next", "request_id": request_id, "pool_id": request.pool_id})
        try:
            entry = self.service.peek_next(request.pool_id)
            if not entry:
                return triage_pb2.PeekNextWaitlistResponse(found=False)
            return triage_pb2.PeekNextWaitlistResponse(
                found=True,
                entry=triage_pb2.WaitlistEntry(
                    entry_id=entry.entry_id,
                    allocation_id=entry.allocation_id,
                    admission_id=entry.admission_id,
                    pool_id=entry.pool_id,
                    triage_level=entry.triage_level,
                    queued_at=entry.queued_at
                )
            )
        except Exception as e:
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return triage_pb2.PeekNextWaitlistResponse(found=False)
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="triage", method="PeekNext", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="triage", method="PeekNext").observe(time.perf_counter() - start)

    def RemoveEntry(self, request, context):
        start = time.perf_counter()
        code = "OK"
        request_id = _request_id(context, request.context.request_id)
        logger.info("triage_grpc_remove_entry", extra={"event": "triage_grpc_remove_entry", "request_id": request_id, "pool_id": request.context.pool_id})
        try:
            success = self.service.remove_entry(
                context=request.context,
                allocation_id=request.allocation_id,
                reason=request.reason
            )
            return triage_pb2.RemoveWaitlistEntryResponse(success=success)
        except AppException as e:
            if e.code == "OUTCOME_UNKNOWN":
                code = "UNAVAILABLE"
                grpc_code = grpc.StatusCode.UNAVAILABLE
            elif e.status_code == 409:
                code = "FAILED_PRECONDITION"
                grpc_code = grpc.StatusCode.FAILED_PRECONDITION
            elif e.status_code == 503:
                code = "UNAVAILABLE"
                grpc_code = grpc.StatusCode.UNAVAILABLE
            else:
                code = "INVALID_ARGUMENT"
                grpc_code = grpc.StatusCode.INVALID_ARGUMENT
            context.set_code(grpc_code)
            context.set_details(f"{e.code}:{e.message}")
            return triage_pb2.RemoveWaitlistEntryResponse(success=False)
        except Exception as e:
            code = "INTERNAL"
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return triage_pb2.RemoveWaitlistEntryResponse(success=False)
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="triage", method="RemoveEntry", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="triage", method="RemoveEntry").observe(time.perf_counter() - start)

def create_grpc_server(service: TriageDomainService, port: int = 50052) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    triage_pb2_grpc.add_TriageServiceServicer_to_server(TriageGrpcServicer(service), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    return server
