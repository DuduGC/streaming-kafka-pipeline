import json
from datetime import datetime, timezone
from uuid import UUID


ALLOWED_EVENT_TYPES = {"PLAY", "PAUSE", "BUFFERING", "ERROR", "COMPLETE"}


def test_generate_event_returns_valid_playback_event() -> None:
    from producer.event_generator import generate_event

    event = generate_event()

    assert set(event) == {
        "event_id",
        "user_id",
        "content_id",
        "event_type",
        "timestamp",
    }
    assert UUID(event["event_id"]).version == 4
    assert event["user_id"].startswith("user_")
    assert event["content_id"].startswith("content_")
    assert event["event_type"] in ALLOWED_EVENT_TYPES

    timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    assert timestamp.tzinfo == timezone.utc


def test_serialize_event_returns_utf8_json_bytes() -> None:
    from producer.event_generator import serialize_event

    event = {
        "event_id": "5b834643-48c7-4b40-8a72-e6a3453e58b8",
        "user_id": "user_123",
        "content_id": "content_42",
        "event_type": "PLAY",
        "timestamp": "2026-09-05T12:00:00Z",
    }

    serialized = serialize_event(event)

    assert isinstance(serialized, bytes)
    assert json.loads(serialized.decode("utf-8")) == event
