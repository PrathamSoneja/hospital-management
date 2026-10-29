"""Canonical operation identity helpers shared by queue repositories."""
import hashlib
import json
from starlette import status
from shared.observability.errors import AppException


def remove_entry_fingerprint(allocation_id: str, entry_id: str | None, pool_id: str, reason: str) -> str:
    payload = {
        "action": "REMOVE_ENTRY",
        "allocation_id": allocation_id,
        "entry_id": entry_id,
        "pool_id": pool_id,
        "reason": reason,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def enqueue_fingerprint(allocation_id: str, admission_id: str, pool_id: str, triage_level: int) -> str:
    payload = {
        "action": "ENQUEUE", "allocation_id": allocation_id,
        "admission_id": admission_id, "pool_id": pool_id,
        "triage_level": triage_level,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_reused_enqueue(record: dict, context, entry) -> None:
    if (
        record.get("operation_id") != context.operation_id
        or record.get("action") != "ENQUEUE"
        or record.get("allocation_id") != entry.allocation_id
        or record.get("admission_id") != entry.admission_id
        or record.get("pool_id") != context.pool_id
        or record.get("input_fingerprint") != enqueue_fingerprint(
            entry.allocation_id, entry.admission_id, context.pool_id, entry.triage_level
        )
    ):
        raise AppException(
            "IDEMPOTENCY_MISMATCH",
            "operation_id was already used for a different queue action or target",
            status.HTTP_409_CONFLICT,
        )


def validate_reused_operation(record: dict, context, allocation_id: str, reason: str) -> None:
    if (
        record.get("operation_id") != context.operation_id
        or record.get("action") != "REMOVE_ENTRY"
        or record.get("allocation_id") != allocation_id
        or record.get("pool_id") != context.pool_id
        or record.get("reason") != reason
        or record.get("input_fingerprint") != remove_entry_fingerprint(
            allocation_id, record.get("entry_id"), context.pool_id, reason
        )
    ):
        raise AppException(
            "IDEMPOTENCY_MISMATCH",
            "operation_id was already used for a different queue action or target",
            status.HTTP_409_CONFLICT,
        )


def fence_for_new_mutation(context, existing: dict | None) -> dict:
    """Validate and return the canonical pool fence value for a new operation."""
    token = int(context.fencing_token)
    if token < 0:
        raise AppException("FAILED_PRECONDITION", "Invalid fencing token", status.HTTP_409_CONFLICT)
    canonical_token = str(token)
    if existing:
        previous_token_text = existing.get("fencing_token", "0")
        try:
            previous_token = int(previous_token_text)
        except (TypeError, ValueError) as error:
            raise AppException("FAILED_PRECONDITION", "Stored pool fence is invalid", status.HTTP_409_CONFLICT) from error
        if str(previous_token) != previous_token_text:
            raise AppException("FAILED_PRECONDITION", "Stored pool fence is not canonical", status.HTTP_409_CONFLICT)
        if token < previous_token or (
            token == previous_token
            and (context.lock_id != existing.get("lock_id") or context.owner_id != existing.get("owner_id"))
        ):
            raise AppException(
                "FAILED_PRECONDITION",
                "Mutation context invalid or lock fencing token superseded",
                status.HTTP_409_CONFLICT,
            )
    return {
        "pool_id": context.pool_id,
        "fencing_token": canonical_token,
        "lock_id": context.lock_id,
        "owner_id": context.owner_id,
    }


def original_context(context) -> dict:
    return {
        "lock_id": context.lock_id,
        "owner_id": context.owner_id,
        "fencing_token": str(int(context.fencing_token)),
    }
