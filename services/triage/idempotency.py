"""Canonical identities for the REST Triage PUT idempotency ledger."""
import hashlib
import json
import re

from shared.observability.errors import AppException
from starlette import status

_KEY_PATTERN = re.compile(r"^[\x21-\x7e]{1,200}$")


def build_put_claim(admission_id: str, idempotency_key: str, triage_level: int, pool_id: str) -> dict:
    if not idempotency_key or not _KEY_PATTERN.fullmatch(idempotency_key):
        raise AppException(
            "INVALID_ARGUMENT", "Idempotency-Key must contain 1-200 visible ASCII characters",
            status.HTTP_400_BAD_REQUEST,
        )
    method = "PUT"
    operation = "set_triage"
    key_hash = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()
    scope = json.dumps([method, operation, admission_id, key_hash], separators=(",", ":"), ensure_ascii=True)
    fingerprint_payload = {
        "method": method,
        "operation": operation,
        "target": admission_id,
        "triage_level": triage_level,
        "pool_id": pool_id,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    return {
        "claim_id": hashlib.sha256(scope.encode("ascii")).hexdigest(),
        "method": method,
        "operation": operation,
        "target_id": admission_id,
        "key_hash": key_hash,
        "fingerprint": fingerprint,
    }


def validate_put_claim(existing: dict, claim: dict) -> None:
    if any(existing.get(field) != claim[field] for field in (
        "method", "operation", "target_id", "key_hash", "fingerprint",
    )):
        raise AppException(
            "IDEMPOTENCY_MISMATCH", "Idempotency-Key was reused with a different Triage request",
            status.HTTP_409_CONFLICT,
        )
    if existing.get("state") != "COMPLETED" or not existing.get("response"):
        raise AppException(
            "SERVICE_UNAVAILABLE", "Stored Triage idempotency result is unresolved",
            status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True,
        )
