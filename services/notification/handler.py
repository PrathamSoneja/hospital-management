"""Turn a Pub/Sub push from Eventarc into a checked notification event, and word it for staff."""
import base64
import binascii
import json
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "events" / "notification.schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)
EVENT_TYPES = set(SCHEMA["properties"]["event_type"]["enum"])


class BadMessage(Exception):
    """The message can never be processed, so retrying it would not help."""


def decode_push(body):
    """Unwrap a Pub/Sub push body. Return the event and the message attributes."""
    try:
        message = body["message"]
        raw = base64.b64decode(message["data"], validate=True)
        event = json.loads(raw)
    except (KeyError, TypeError, binascii.Error, ValueError) as error:
        raise BadMessage(f"not a Pub/Sub push message: {error}") from error
    if not isinstance(event, dict):
        raise BadMessage("message data is not a JSON object")
    return event, message.get("attributes") or {}


def validate(event):
    """Check the event against contracts/events/notification.schema.json."""
    errors = sorted(VALIDATOR.iter_errors(event), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        where = ".".join(str(part) for part in first.path) or "event"
        raise BadMessage(f"{where}: {first.message}")


def render(event):
    """Word the notification for ward staff. Routing is by pool_id, never by a guessed ward."""
    pool = event["pool_id"]
    admission = event["admission_id"]
    bed = event.get("bed_id") or "a bed"
    ward = f" in ward {event['ward']}" if event.get("ward") else ""
    kind = event["event_type"]
    if kind == "BED_ALLOCATED":
        return f"[{pool}] Bed {bed}{ward} allocated to admission {admission}."
    if kind == "ALLOCATION_WAITLISTED":
        return f"[{pool}] Admission {admission} waitlisted: no bed free in this pool."
    if kind == "BED_RELEASED":
        return f"[{pool}] Bed {bed}{ward} released by admission {admission}."
    return f"[{pool}] Allocation {event['allocation_id']} for admission {admission} hit a conflict and needs review."