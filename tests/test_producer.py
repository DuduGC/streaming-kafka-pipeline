import json
import sys
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest


class RecordingProducer:
    def __init__(self) -> None:
        self.message: dict[str, Any] | None = None
        self.poll_calls: list[float] = []

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable[..., None],
    ) -> None:
        self.message = {
            "topic": topic,
            "key": key,
            "value": value,
            "on_delivery": on_delivery,
        }

    def poll(self, timeout: float) -> None:
        self.poll_calls.append(timeout)


class FakeKafkaError:
    TOPIC_ALREADY_EXISTS = 36

    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code


class FakeKafkaException(Exception):
    pass


class FakeNewTopic:
    def __init__(
        self,
        topic: str,
        *,
        num_partitions: int,
        replication_factor: int,
    ) -> None:
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def result(self, timeout: float) -> None:
        assert timeout == 10
        if self.error is not None:
            raise self.error


class RecordingAdminClient:
    def __init__(
        self,
        *,
        create_error: Exception | None = None,
        existing_partition_count: int = 3,
    ) -> None:
        self.create_error = create_error
        self.existing_partition_count = existing_partition_count
        self.created_topics: list[FakeNewTopic] = []

    def create_topics(self, topics: list[FakeNewTopic]) -> dict[str, FakeFuture]:
        self.created_topics = topics
        return {topics[0].topic: FakeFuture(self.create_error)}

    def list_topics(self, topic: str, timeout: float) -> SimpleNamespace:
        assert timeout == 10
        partitions = {index: object() for index in range(self.existing_partition_count)}
        return SimpleNamespace(
            topics={topic: SimpleNamespace(partitions=partitions)}
        )


@pytest.fixture
def fake_confluent_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        SimpleNamespace(
            KafkaError=FakeKafkaError,
            KafkaException=FakeKafkaException,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka.admin",
        SimpleNamespace(NewTopic=FakeNewTopic),
    )


def test_load_settings_fails_when_kafka_bootstrap_servers_is_missing() -> None:
    from producer.producer import load_settings

    with pytest.raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
        load_settings({"KAFKA_TOPIC": "playback-events"})


def test_load_settings_reads_topic_partition_count() -> None:
    from producer.producer import load_settings

    settings = load_settings(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_TOPIC": "playback-events",
            "KAFKA_TOPIC_PARTITIONS": "3",
        }
    )

    assert settings == ("kafka:9092", "playback-events", 3, 1.0, "INFO")


@pytest.mark.parametrize("partition_count", ["zero", "0", "-1"])
def test_load_settings_rejects_invalid_topic_partition_count(
    partition_count: str,
) -> None:
    from producer.producer import load_settings

    with pytest.raises(ValueError, match="KAFKA_TOPIC_PARTITIONS"):
        load_settings(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
                "KAFKA_TOPIC": "playback-events",
                "KAFKA_TOPIC_PARTITIONS": partition_count,
            }
        )


def test_publish_event_uses_content_id_as_message_key() -> None:
    from producer.producer import publish_event

    event = {
        "event_id": "5b834643-48c7-4b40-8a72-e6a3453e58b8",
        "user_id": "user_123",
        "content_id": "content_42",
        "event_type": "PLAY",
        "timestamp": "2026-09-05T12:00:00Z",
    }
    kafka_producer = RecordingProducer()

    publish_event(kafka_producer, "playback-events", event)

    assert kafka_producer.message is not None
    assert kafka_producer.message["topic"] == "playback-events"
    assert kafka_producer.message["key"] == b"content_42"
    assert json.loads(kafka_producer.message["value"].decode("utf-8")) == event
    assert callable(kafka_producer.message["on_delivery"])
    assert kafka_producer.poll_calls == [0]


def test_ensure_topic_creates_three_partition_topic(
    fake_confluent_kafka: None,
) -> None:
    from producer.producer import ensure_topic

    admin_client = RecordingAdminClient()

    created = ensure_topic(admin_client, "playback-events", 3)

    assert created is True
    assert len(admin_client.created_topics) == 1
    topic = admin_client.created_topics[0]
    assert topic.topic == "playback-events"
    assert topic.num_partitions == 3
    assert topic.replication_factor == 1


def test_ensure_topic_accepts_existing_topic_with_expected_partitions(
    fake_confluent_kafka: None,
) -> None:
    from producer.producer import ensure_topic

    already_exists = FakeKafkaException(
        FakeKafkaError(FakeKafkaError.TOPIC_ALREADY_EXISTS)
    )
    admin_client = RecordingAdminClient(create_error=already_exists)

    assert ensure_topic(admin_client, "playback-events", 3) is False


def test_ensure_topic_rejects_existing_topic_with_wrong_partition_count(
    fake_confluent_kafka: None,
) -> None:
    from producer.producer import ensure_topic

    already_exists = FakeKafkaException(
        FakeKafkaError(FakeKafkaError.TOPIC_ALREADY_EXISTS)
    )
    admin_client = RecordingAdminClient(
        create_error=already_exists,
        existing_partition_count=2,
    )

    with pytest.raises(ValueError, match="expected 3 partitions, found 2"):
        ensure_topic(admin_client, "playback-events", 3)
