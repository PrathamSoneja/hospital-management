"""
Shared, stateful stand-ins for teammate services that are not available locally.

One process serves, over the real wire contracts:
  gRPC  LockCoordinator  (contracts/proto/lock.proto)   - tests/support/stateful_lock.py model
  gRPC  BedService       (contracts/proto/bed.proto)
  gRPC  StaffScheduler   (contracts/proto/staff.proto)
  HTTP  Patient Registry GET /api/v1/admissions/{id}    (contracts/openapi/patient-registry-admissions.openapi.yaml)
  HTTP  /admin/*          seed, reset, state, fault injection (sandbox control only)

Every Allocation/Triage process in a sandbox points at the same instance, so the
Coordinator lease, bed ownership, and staff capacity are shared across processes.
Mutations are idempotent by MutationContext.operation_id and fenced by the
Coordinator's current lease. This is TEST-ONLY tooling: it is not the teammates'
services, proves nothing about ZooKeeper, and production code never imports it.

    python -m tests.support.sandbox_dependencies --grpc-port 57051 --http-port 57080
"""
import argparse
import json
import threading
import time
import uuid
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc

from shared.generated import bed_pb2, bed_pb2_grpc, lock_pb2, lock_pb2_grpc, staff_pb2, staff_pb2_grpc
from tests.support.stateful_lock import StatefulLockCoordinator, StatefulLockServicer

FAULT_MODES = {"unavailable", "lost_ack", "delay"}  # fail before apply; apply then fail; sleep then apply


class SharedState:
    def __init__(self):
        self.lock = threading.RLock()
        self.coordinator = StatefulLockCoordinator()
        self.validate = self.coordinator.validate  # replaced by the real Coordinator's Validate (--lock-addr)
        self.reset()

    def reset(self):
        with self.lock:
            self.coordinator.__init__()
            self.beds = {}            # bed_id -> {bed_id, pool_id, ward, status, current_allocation_id}
            self.staff_capacity = {}  # pool_id -> remaining assignable staff
            self.assignments = {}     # assignment_id -> {allocation_id, pool_id, staff_id, active}
            self.admissions = {}      # admission_id -> {admission_id, patient_id, status}
            self.operations = {}      # operation_id -> (fingerprint, response bytes, response type)
            self.faults = {}          # rpc name -> list of modes, consumed in order
            self.calls = {}           # rpc name -> count
            self.fence_rejections = 0
            self.delay_seconds = 3.0

    def seed(self, data: dict):
        with self.lock:
            for bed in data.get("beds", []):
                self.beds[bed["bed_id"]] = {"bed_id": bed["bed_id"], "pool_id": bed["pool_id"], "ward": bed["ward"],
                                            "status": "AVAILABLE", "current_allocation_id": ""}
            for pool_id, count in data.get("staff_capacity", {}).items():
                self.staff_capacity[pool_id] = int(count)
            for admission in data.get("admissions", []):
                self.admissions[admission["admission_id"]] = {
                    "admission_id": admission["admission_id"], "patient_id": admission["patient_id"],
                    "status": admission.get("status", "OPEN")}

    def add_fault(self, rpc: str, mode: str, count: int = 1):
        if mode not in FAULT_MODES:
            raise ValueError(f"unknown fault mode {mode}")
        with self.lock:
            self.faults.setdefault(rpc, []).extend([mode] * count)

    def take_fault(self, rpc: str):
        with self.lock:
            self.calls[rpc] = self.calls.get(rpc, 0) + 1
            pending = self.faults.get(rpc)
            return pending.pop(0) if pending else None

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "beds": list(self.beds.values()), "staff_capacity": dict(self.staff_capacity),
                "assignments": dict(self.assignments), "operation_ids": sorted(self.operations),
                "calls": dict(self.calls), "pending_faults": {k: list(v) for k, v in self.faults.items() if v},
                "fence_rejections": self.fence_rejections, "lock_acquisitions": self.coordinator.acquisitions,
            }


def _fenced(state: SharedState, context) -> bool:
    valid, _ = state.validate(context.lock_id, context.owner_id, context.pool_id, context.fencing_token)
    if not valid:
        state.fence_rejections += 1
    return valid


def _idempotent(state, context, rpc, fingerprint, response_type, apply, grpc_context):
    """Replay a recorded result for the same operation_id; reject reuse with different input."""
    fault = state.take_fault(rpc)
    if fault == "unavailable":
        grpc_context.abort(grpc.StatusCode.UNAVAILABLE, "injected: unavailable before apply")
    if fault == "delay":
        time.sleep(state.delay_seconds)
    with state.lock:
        recorded = state.operations.get(context.operation_id)
        if recorded:
            if recorded[0] != fingerprint:
                return response_type(success=False, error_message="IDEMPOTENCY_MISMATCH")
            return response_type.FromString(recorded[1])
        if not _fenced(state, context):
            return response_type(success=False, error_message="STALE_FENCING_TOKEN")
        response = apply()
        if response.success:
            state.operations[context.operation_id] = (fingerprint, response.SerializeToString())
    if fault == "lost_ack":
        grpc_context.abort(grpc.StatusCode.UNAVAILABLE, "injected: applied, acknowledgement lost")
    return response


class BedServicer(bed_pb2_grpc.BedServiceServicer):
    def __init__(self, state: SharedState):
        self.state = state

    def _bed(self, record):
        return bed_pb2.Bed(**record)

    def ListAvailableBeds(self, request, context):
        if self.state.take_fault("ListAvailableBeds") == "unavailable":
            context.abort(grpc.StatusCode.UNAVAILABLE, "injected")
        with self.state.lock:
            beds = sorted((b for b in self.state.beds.values() if b["pool_id"] == request.pool_id and b["status"] == "AVAILABLE"),
                          key=lambda b: b["bed_id"])
            return bed_pb2.ListAvailableBedsResponse(beds=[self._bed(b) for b in beds])

    def GetBed(self, request, context):
        with self.state.lock:
            record = self.state.beds.get(request.bed_id)
            return bed_pb2.GetBedResponse(found=record is not None, bed=self._bed(record) if record else None)

    def AllocateBed(self, request, context):
        def apply():
            bed = self.state.beds.get(request.bed_id)
            if bed is None or bed["pool_id"] != request.context.pool_id:
                return bed_pb2.AllocateBedResponse(success=False, error_message="BED_NOT_FOUND")
            if bed["status"] != "AVAILABLE" and bed["current_allocation_id"] != request.allocation_id:
                return bed_pb2.AllocateBedResponse(success=False, error_message="BED_OCCUPIED")
            bed.update(status="OCCUPIED", current_allocation_id=request.allocation_id)
            return bed_pb2.AllocateBedResponse(success=True, bed=self._bed(bed))
        return _idempotent(self.state, request.context, "AllocateBed", ("allocate", request.bed_id, request.allocation_id),
                           bed_pb2.AllocateBedResponse, apply, context)

    def ReleaseBed(self, request, context):
        def apply():
            bed = self.state.beds.get(request.bed_id)
            if bed is None:
                return bed_pb2.ReleaseBedResponse(success=False, error_message="BED_NOT_FOUND")
            if bed["current_allocation_id"] not in ("", request.allocation_id):
                return bed_pb2.ReleaseBedResponse(success=False, error_message="NOT_OWNER")
            bed.update(status="AVAILABLE", current_allocation_id="")
            return bed_pb2.ReleaseBedResponse(success=True)
        return _idempotent(self.state, request.context, "ReleaseBed", ("release", request.bed_id, request.allocation_id),
                           bed_pb2.ReleaseBedResponse, apply, context)


class StaffServicer(staff_pb2_grpc.StaffSchedulerServicer):
    def __init__(self, state: SharedState):
        self.state = state

    def CheckCapacity(self, request, context):
        if self.state.take_fault("CheckCapacity") == "unavailable":
            context.abort(grpc.StatusCode.UNAVAILABLE, "injected")
        with self.state.lock:
            count = self.state.staff_capacity.get(request.pool_id, 0)
            return staff_pb2.CheckCapacityResponse(has_capacity=count > 0, available_count=count)

    def AssignStaff(self, request, context):
        pool_id = request.context.pool_id

        def apply():
            if self.state.staff_capacity.get(pool_id, 0) <= 0:
                return staff_pb2.AssignStaffResponse(success=False, error_message="NO_CAPACITY")
            self.state.staff_capacity[pool_id] -= 1
            assignment_id = f"asg-{uuid.uuid4().hex[:12]}"
            staff_id = f"staff-{pool_id.lower()}-{len(self.state.assignments) + 1:04d}"
            self.state.assignments[assignment_id] = {"allocation_id": request.allocation_id, "pool_id": pool_id,
                                                     "staff_id": staff_id, "active": True}
            return staff_pb2.AssignStaffResponse(success=True, staff_assignment_id=assignment_id, staff_id=staff_id)
        return _idempotent(self.state, request.context, "AssignStaff", ("assign", request.allocation_id, request.bed_id),
                           staff_pb2.AssignStaffResponse, apply, context)

    def ReleaseAssignment(self, request, context):
        def apply():
            assignment = self.state.assignments.get(request.staff_assignment_id)
            if assignment is None or assignment["allocation_id"] != request.allocation_id:
                return staff_pb2.ReleaseAssignmentResponse(success=False, error_message="NOT_OWNER")
            if assignment["active"]:
                assignment["active"] = False
                self.state.staff_capacity[assignment["pool_id"]] = self.state.staff_capacity.get(assignment["pool_id"], 0) + 1
            return staff_pb2.ReleaseAssignmentResponse(success=True)
        return _idempotent(self.state, request.context, "ReleaseAssignment",
                           ("release-staff", request.staff_assignment_id, request.allocation_id),
                           staff_pb2.ReleaseAssignmentResponse, apply, context)


def _http_handler(state: SharedState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep sandbox logs free of per-request noise
            pass

        def _send(self, code, body):
            payload = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/healthz":
                return self._send(200, {"status": "ok"})
            if self.path == "/admin/state":
                return self._send(200, state.snapshot())
            prefix = "/api/v1/admissions/"
            if self.path.startswith(prefix):
                if state.take_fault("GetAdmission") == "unavailable":
                    return self._send(503, {"error": {"code": "SERVICE_UNAVAILABLE"}})
                with state.lock:
                    admission = state.admissions.get(self.path[len(prefix):])
                return self._send(200, admission) if admission else self._send(404, {"error": {"code": "NOT_FOUND"}})
            self._send(404, {"error": {"code": "NOT_FOUND"}})

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/admin/reset":
                state.reset()
            elif self.path == "/admin/seed":
                state.seed(body)
            elif self.path == "/admin/faults":
                try:
                    state.add_fault(body["rpc"], body["mode"], int(body.get("count", 1)))
                    if "delay_seconds" in body:
                        state.delay_seconds = float(body["delay_seconds"])
                except (KeyError, ValueError) as error:
                    return self._send(400, {"error": str(error)})
            else:
                return self._send(404, {"error": {"code": "NOT_FOUND"}})
            self._send(200, {"ok": True})
    return Handler


def serve(grpc_port: int, http_port: int, host: str = "127.0.0.1", lock_addr: str | None = None):
    state = SharedState()
    if lock_addr:  # leases come from the real Lock Coordinator; fence stand-in mutations against it
        coordinator = lock_pb2_grpc.LockCoordinatorStub(grpc.insecure_channel(lock_addr))

        def validate(lock_id, owner_id, pool_id, fencing_token):
            response = coordinator.Validate(lock_pb2.ValidateLockRequest(
                lock_id=lock_id, owner_id=owner_id, pool_id=pool_id, fencing_token=fencing_token), timeout=3)
            return response.valid, response.reason
        state.validate = validate
    # Acquire blocks a worker while waiting for the pool lease; size the pool so waiters cannot
    # starve the lease holder's Bed/Staff calls (a test-double artefact, not a production setting).
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=256))
    lock_pb2_grpc.add_LockCoordinatorServicer_to_server(StatefulLockServicer(state.coordinator), server)
    bed_pb2_grpc.add_BedServiceServicer_to_server(BedServicer(state), server)
    staff_pb2_grpc.add_StaffSchedulerServicer_to_server(StaffServicer(state), server)
    if server.add_insecure_port(f"{host}:{grpc_port}") == 0:
        raise SystemExit(f"cannot bind gRPC port {grpc_port}")
    server.start()
    http = ThreadingHTTPServer((host, http_port), _http_handler(state))
    print(f"sandbox-dependencies ready grpc={host}:{grpc_port} http={host}:{http_port}", flush=True)
    try:
        http.serve_forever()
    finally:
        server.stop(grace=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--grpc-port", type=int, required=True)
    parser.add_argument("--http-port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--lock-addr", help="validate fencing against this real Lock Coordinator")
    args = parser.parse_args()
    serve(args.grpc_port, args.http_port, args.host, args.lock_addr)
