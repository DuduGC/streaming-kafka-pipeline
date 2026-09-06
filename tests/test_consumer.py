import logging
from typing import Any

import pytest


class RecordingConsumer:
    def __init__(self, actions: list[str] | None = None) -> None:
        self.commits: list[dict[str, Any]] = []
        self.actions = actions

    def commit(self, **kwargs: Any) -> None:
        if self.actions is not None:
            self.actions.append("commit")
        self.commits.append(kwargs)


class FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes:
        return b"content_42"

    def topic(self) -> str:
        return "playback-events"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 7


def test_load_settings_fails_when_consumer_group_is_missing() -> None:
    from consumer.consumer import load_settings

    with pytest.raises(ValueError, match="KAFKA_CONSUMER_GROUP"):
        load_settings(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
                "KAFKA_TOPIC": "playback-events",
            }
        )


def test_build_consumer_config_uses_group_and_manual_commits() -> None:
    from consumer.consumer import build_consumer_config

    config = build_consumer_config("kafka:9092", "playback-events-consumer")

    assert config == {
        "bootstrap.servers": "kafka:9092",
        "group.id": "playback-events-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }


def test_process_message_persists_event_then_commits_offset() -> None:
    from consumer.consumer import process_message

    message = FakeMessage(
        b'{"event_id":"5b834643-48c7-4b40-8a72-e6a3453e58b8",'
        b'"user_id":"user_123","content_id":"content_42",'
        b'"event_type":"PLAY","timestamp":"2026-09-05T12:00:00Z"}'
    )
    actions: list[str] = []
    kafka_consumer = RecordingConsumer(actions)

    def persist_event(_event: dict[str, Any]) -> bool:
        actions.append("persist")
        return True

    processed = process_message(kafka_consumer, message, persist_event)

    assert processed is True
    assert actions == ["persist", "commit"]
    assert kafka_consumer.commits == [
        {"message": message, "asynchronous": False}
    ]


def test_process_message_logs_duplicate_and_commits_offset(caplog: pytest.LogCaptureFixture) -> None:
    from consumer.consumer import process_message

    caplog.set_level(logging.INFO, logger="consumer")

    message = FakeMessage(
        b'{"event_id":"5b834643-48c7-4b40-8a72-e6a3453e58b8",'
        b'"user_id":"user_123","content_id":"content_42",'
        b'"event_type":"PLAY","timestamp":"2026-09-05T12:00:00Z"}'
    )
    kafka_consumer = RecordingConsumer()

    processed = process_message(kafka_consumer, message, lambda _event: False)

    assert processed is True
    assert "Evento duplicado ignorado" in caplog.text
    assert kafka_consumer.commits == [
        {"message": message, "asynchronous": False}
    ]


def test_process_message_discards_invalid_json_and_commits_offset() -> None:
    from consumer.consumer import process_message

    message = FakeMessage(b"not-json")
    kafka_consumer = RecordingConsumer()

    processed = process_message(kafka_consumer, message, lambda _event: None)

    assert processed is False
    assert kafka_consumer.commits == [
        {"message": message, "asynchronous": False}
    ]


def test_process_message_does_not_commit_when_persistence_fails() -> None:
    from consumer.consumer import process_message
    from consumer.database import PersistenceError

    message = FakeMessage(
        b'{"event_id":"5b834643-48c7-4b40-8a72-e6a3453e58b8",'
        b'"user_id":"user_123","content_id":"content_42",'
        b'"event_type":"PLAY","timestamp":"2026-09-05T12:00:00Z"}'
    )
    kafka_consumer = RecordingConsumer()

    def fail_to_persist(_event: dict[str, Any]) -> None:
        raise PersistenceError("database unavailable")

    with pytest.raises(PersistenceError, match="database unavailable"):
        process_message(kafka_consumer, message, fail_to_persist)

    assert kafka_consumer.commits == []
