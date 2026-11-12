"""Prometheus metrics for the Lock Coordinator. Pool IDs are a small fixed set, so labels stay bounded."""
import time

import grpc
from prometheus_client import Counter

from shared.observability.metrics import (
    GRPC_REQUEST_DURATION_SECONDS,
    GRPC_REQUESTS_TOTAL,
    LEASE_EXPIRATIONS_TOTAL,
    LOCK_WAIT_SECONDS,
)

SERVICE = "lock-coordinator"

LOCK_ACQUIRE_TOTAL = Counter(
    "lock_acquire_total",
    "Acquire calls per pool, by outcome (granted or timeout)",
    ["service", "pool_id", "outcome"],
)

ABANDONED_WAITERS_TOTAL = Counter(
    "lock_abandoned_waiters_total",
    "Waiters the reaper removed after they should have given up",
    ["service", "pool_id"],
)


def record_acquire(pool_id, outcome, wait_ms=None):
    """Count one Acquire, and how long it waited if it got the key."""
    LOCK_ACQUIRE_TOTAL.labels(service=SERVICE, pool_id=pool_id, outcome=outcome).inc()
    if wait_ms is not None:
        LOCK_WAIT_SECONDS.labels(service=SERVICE, pool_id=pool_id).observe(wait_ms / 1000)


def record_reap(pool_id, kind):
    """Count one znode the reaper deleted: an expired lease or an abandoned waiter."""
    if kind == "lease":
        LEASE_EXPIRATIONS_TOTAL.labels(service=SERVICE, pool_id=pool_id).inc()
    else:
        ABANDONED_WAITERS_TOTAL.labels(service=SERVICE, pool_id=pool_id).inc()


class MetricsInterceptor(grpc.ServerInterceptor):
    """Counts every gRPC call and times it, using the team's shared gRPC metrics."""

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        method = handler_call_details.method.rsplit("/", 1)[-1]
        inner = handler.unary_unary

        def measured(request, context):
            started = time.perf_counter()
            code = "OK"
            try:
                return inner(request, context)
            except Exception:
                status = context.code()
                code = status.name if status else "UNKNOWN"
                raise
            finally:
                GRPC_REQUESTS_TOTAL.labels(service=SERVICE, method=method, code=code).inc()
                GRPC_REQUEST_DURATION_SECONDS.labels(service=SERVICE, method=method).observe(
                    time.perf_counter() - started
                )

        return grpc.unary_unary_rpc_method_handler(
            measured,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )