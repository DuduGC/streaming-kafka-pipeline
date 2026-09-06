import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID


ALLOWED_EVENT_TYPES = {"PLAY", "PAUSE", "BUFFERING", "ERROR", "COMPLETE"}
REQUIRED_FIELDS = ("event_id", "user_id", "content_id", "event_type", "timestamp")
MAX_MESSAGE_BYTES = 10_000
DATABASE_IDENTIFIER_MAX_LENGTH = 100
DATABASE_IDENTIFIER_FIELDS = ("user_id", "content_id")


def validate_event_message(payload: bytes) -> dict[str, str]:
    """Deserialize and validate one playback event message."""
    if not isinstance(payload, bytes):
        raise ValueError("message payload must be bytes")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ValueError(f"message exceeds {MAX_MESSAGE_BYTES} bytes")

    try:
        event: Any = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("message is not valid UTF-8 JSON") from error

    if not isinstance(event, dict):
        raise ValueError("message JSON root must be an object")

    validated: dict[str, str] = {}
    for field in REQUIRED_FIELDS:
        if field not in event:
            raise ValueError(f"required field is missing: {field}")
        value = event[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")
        validated[field] = value

    for field in DATABASE_IDENTIFIER_FIELDS:
        value = validated[field]
        if "\x00" in value:
            raise ValueError(f"{field} must not contain NUL")
        if any("\ud800" <= character <= "\udfff" for character in value):
            raise ValueError(f"{field} must contain valid Unicode")
        if len(value) > DATABASE_IDENTIFIER_MAX_LENGTH:
            raise ValueError(
                f"{field} must be at most {DATABASE_IDENTIFIER_MAX_LENGTH} characters"
            )

    try:
        UUID(validated["event_id"])
    except ValueError as error:
        raise ValueError("event_id must be a valid UUID") from error

    if validated["event_type"] not in ALLOWED_EVENT_TYPES:
        raise ValueError("event_type is not allowed")

    try:
        timestamp = datetime.fromisoformat(
            validated["timestamp"].replace("Z", "+00:00")
        )
    except ValueError as error:
        raise ValueError("timestamp must be ISO-8601 UTC") from error
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be ISO-8601 UTC")

    return validated
