import json

import pytest


VALID_EVENT = {
    "event_id": "5b834643-48c7-4b40-8a72-e6a3453e58b8",
    "user_id": "user_123",
    "content_id": "content_42",
    "event_type": "PLAY",
    "timestamp": "2026-09-05T12:00:00Z",
}


def event_payload(**overrides: object) -> bytes:
    event = {**VALID_EVENT, **overrides}
    return json.dumps(event).encode("utf-8")


def test_validate_event_accepts_valid_event() -> None:
    from consumer.validator import validate_event_message

    assert validate_event_message(event_payload()) == VALID_EVENT


@pytest.mark.parametrize(
    "missing_field",
    ["event_id", "user_id", "content_id", "event_type", "timestamp"],
)
def test_validate_event_rejects_missing_required_field(missing_field: str) -> None:
    from consumer.validator import validate_event_message

    event = dict(VALID_EVENT)
    del event[missing_field]

    with pytest.raises(ValueError, match="required field"):
        validate_event_message(json.dumps(event).encode("utf-8"))


def test_validate_event_rejects_invalid_uuid() -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match="event_id must be a valid UUID"):
        validate_event_message(event_payload(event_id="not-a-uuid"))


def test_validate_event_rejects_unknown_event_type() -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match="event_type is not allowed"):
        validate_event_message(event_payload(event_type="SEEK"))


@pytest.mark.parametrize(
    "timestamp",
    ["not-a-timestamp", "2026-09-05T12:00:00", "2026-09-05T12:00:00-03:00"],
)
def test_validate_event_rejects_invalid_or_non_utc_timestamp(timestamp: str) -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match="timestamp must be ISO-8601 UTC"):
        validate_event_message(event_payload(timestamp=timestamp))


def test_validate_event_rejects_non_string_field() -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match="user_id must be a non-empty string"):
        validate_event_message(event_payload(user_id=123))


@pytest.mark.parametrize("field", ["user_id", "content_id"])
def test_validate_event_accepts_identifier_at_database_limit(field: str) -> None:
    from consumer.validator import validate_event_message

    value = "x" * 100

    assert validate_event_message(event_payload(**{field: value}))[field] == value


@pytest.mark.parametrize("field", ["user_id", "content_id"])
def test_validate_event_rejects_identifier_above_database_limit(field: str) -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match=rf"{field} must be at most 100 characters"):
        validate_event_message(event_payload(**{field: "x" * 101}))


@pytest.mark.parametrize("field", ["user_id", "content_id"])
def test_validate_event_rejects_nul_in_identifier(field: str) -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match=rf"{field} must not contain NUL"):
        validate_event_message(event_payload(**{field: "safe\x00suffix"}))


@pytest.mark.parametrize("field", ["user_id", "content_id"])
def test_validate_event_rejects_unpaired_unicode_surrogate(field: str) -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match=rf"{field} must contain valid Unicode"):
        validate_event_message(event_payload(**{field: "safe\ud800suffix"}))


def test_validate_event_rejects_invalid_json() -> None:
    from consumer.validator import validate_event_message

    with pytest.raises(ValueError, match="valid UTF-8 JSON"):
        validate_event_message(b"not-json")


def test_validate_event_rejects_oversized_message() -> None:
    from consumer.validator import MAX_MESSAGE_BYTES, validate_event_message

    with pytest.raises(ValueError, match="message exceeds"):
        validate_event_message(b"x" * (MAX_MESSAGE_BYTES + 1))
