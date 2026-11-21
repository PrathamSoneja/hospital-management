"""
gRPC server implementation for BedService.
Owned by Member D (Jay Patel).
Implements contracts/proto/bed.proto using shared.generated.bed_pb2 and bed_pb2_grpc.
"""
import time
import grpc
import logging
from concurrent import futures
from shared.generated import bed_pb2, bed_pb2_grpc
from services.bed_registry.service import BedRegistryDomainService
from shared.observability.metrics import GRPC_REQUESTS_TOTAL, GRPC_REQUEST_DURATION_SECONDS

logger = logging.getLogger("bed-registry-grpc")

def _request_id(context, message_request_id: str = "") -> str:
    try:
        invocation_metadata = context.invocation_metadata()
    except (AttributeError, RuntimeError):
        invocation_metadata = ()
    metadata = dict((item.key.lower(), item.value) for item in invocation_metadata)
    return metadata.get("x-request-id") or message_request_id or "unknown"

class BedGrpcServicer(bed_pb2_grpc.BedServiceServicer):
    def __init__(self, service: BedRegistryDomainService):
        self.service = service

    def ListAvailableBeds(self, request, context):
        start = time.perf_counter()
        code = "OK"
        req_id = _request_id(context, request.request_id)
        try:
            beds = self.service.list_available_beds(request.pool_id, request_id=req_id)
            pb_beds = [
                bed_pb2.Bed(
                    bed_id=b.bed_id,
                    pool_id=b.pool_id,
                    ward=b.ward,
                    status=b.status,
                    current_allocation_id=b.current_allocation_id or ""
                )
                for b in beds
            ]
            return bed_pb2.ListAvailableBedsResponse(beds=pb_beds)
        except Exception as e:
            code = "INTERNAL"
            logger.exception("error_in_list_available_beds", extra={"request_id": req_id})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return bed_pb2.ListAvailableBedsResponse(beds=[])
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="bed_registry", method="ListAvailableBeds", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="bed_registry", method="ListAvailableBeds").observe(time.perf_counter() - start)

    def GetBed(self, request, context):
        start = time.perf_counter()
        code = "OK"
        req_id = _request_id(context, request.request_id)
        try:
            b = self.service.get_bed(request.bed_id, request_id=req_id)
            if not b:
                return bed_pb2.GetBedResponse(found=False)
            return bed_pb2.GetBedResponse(
                found=True,
                bed=bed_pb2.Bed(
                    bed_id=b.bed_id,
                    pool_id=b.pool_id,
                    ward=b.ward,
                    status=b.status,
                    current_allocation_id=b.current_allocation_id or ""
                )
            )
        except Exception as e:
            code = "INTERNAL"
            logger.exception("error_in_get_bed", extra={"request_id": req_id})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return bed_pb2.GetBedResponse(found=False)
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="bed_registry", method="GetBed", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="bed_registry", method="GetBed").observe(time.perf_counter() - start)

    def AllocateBed(self, request, context):
        start = time.perf_counter()
        code = "OK"
        mutation_ctx = request.context
        req_id = _request_id(context, mutation_ctx.request_id if mutation_ctx else "")
        try:
            if not mutation_ctx:
                code = "INVALID_ARGUMENT"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("MutationContext is required")
                return bed_pb2.AllocateBedResponse(success=False, error_message="MutationContext is required")

            success, bed_dto, err = self.service.allocate_bed(
                operation_id=mutation_ctx.operation_id,
                pool_id=mutation_ctx.pool_id,
                bed_id=request.bed_id,
                allocation_id=request.allocation_id,
                fencing_token=mutation_ctx.fencing_token,
                request_id=req_id
            )

            if not success:
                if "Stale fencing token" in err:
                    code = "FAILED_PRECONDITION"
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    context.set_details(err)
                return bed_pb2.AllocateBedResponse(success=False, error_message=err)

            pb_bed = bed_pb2.Bed(
                bed_id=bed_dto.bed_id,
                pool_id=bed_dto.pool_id,
                ward=bed_dto.ward,
                status=bed_dto.status,
                current_allocation_id=bed_dto.current_allocation_id or ""
            ) if bed_dto else None

            return bed_pb2.AllocateBedResponse(
                success=True,
                bed=pb_bed,
                error_message=""
            )
        except Exception as e:
            code = "INTERNAL"
            logger.exception("error_in_allocate_bed", extra={"request_id": req_id})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return bed_pb2.AllocateBedResponse(success=False, error_message=str(e))
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="bed_registry", method="AllocateBed", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="bed_registry", method="AllocateBed").observe(time.perf_counter() - start)

    def ReleaseBed(self, request, context):
        start = time.perf_counter()
        code = "OK"
        mutation_ctx = request.context
        req_id = _request_id(context, mutation_ctx.request_id if mutation_ctx else "")
        try:
            if not mutation_ctx:
                code = "INVALID_ARGUMENT"
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("MutationContext is required")
                return bed_pb2.ReleaseBedResponse(success=False, error_message="MutationContext is required")

            success, err = self.service.release_bed(
                operation_id=mutation_ctx.operation_id,
                pool_id=mutation_ctx.pool_id,
                bed_id=request.bed_id,
                allocation_id=request.allocation_id,
                fencing_token=mutation_ctx.fencing_token,
                request_id=req_id
            )

            if not success:
                if "Stale fencing token" in err:
                    code = "FAILED_PRECONDITION"
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    context.set_details(err)
                return bed_pb2.ReleaseBedResponse(success=False, error_message=err)

            return bed_pb2.ReleaseBedResponse(success=True, error_message="")
        except Exception as e:
            code = "INTERNAL"
            logger.exception("error_in_release_bed", extra={"request_id": req_id})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return bed_pb2.ReleaseBedResponse(success=False, error_message=str(e))
        finally:
            GRPC_REQUESTS_TOTAL.labels(service="bed_registry", method="ReleaseBed", code=code).inc()
            GRPC_REQUEST_DURATION_SECONDS.labels(service="bed_registry", method="ReleaseBed").observe(time.perf_counter() - start)

def create_grpc_server(service: BedRegistryDomainService, port: int = 50053, max_workers: int = 10) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    bed_pb2_grpc.add_BedServiceServicer_to_server(BedGrpcServicer(service), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    return server

