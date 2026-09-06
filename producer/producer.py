import logging
import os
import signal
import threading
from collections.abc import Mapping
from typing import Any

from producer.event_generator import generate_event, serialize_event


LOGGER = logging.getLogger("producer")
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def ensure_topic(admin_client: Any, topic: str, partition_count: int) -> bool:
    """Create the topic, or validate a compatible topic that already exists."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import NewTopic

    new_topic = NewTopic(
        topic,
        num_partitions=partition_count,
        replication_factor=1,
    )
    future = admin_client.create_topics([new_topic])[topic]
    try:
        future.result(timeout=10)
        return True
    except KafkaException as error:
        kafka_error = error.args[0] if error.args else None
        if (
            kafka_error is None
            or kafka_error.code() != KafkaError.TOPIC_ALREADY_EXISTS
        ):
            raise

    metadata = admin_client.list_topics(topic=topic, timeout=10)
    existing_partition_count = len(metadata.topics[topic].partitions)
    if existing_partition_count != partition_count:
        raise ValueError(
            f"topic {topic} expected {partition_count} partitions, "
            f"found {existing_partition_count}"
        )
    return False


def load_settings(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str, int, float, str]:
    """Load and validate the Producer settings."""
    values = os.environ if environ is None else environ

    bootstrap_servers = values.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    topic = values.get("KAFKA_TOPIC", "").strip()
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
    if not topic:
        raise ValueError("KAFKA_TOPIC must be set")

    try:
        partition_count = int(values.get("KAFKA_TOPIC_PARTITIONS", "3"))
    except ValueError as error:
        raise ValueError("KAFKA_TOPIC_PARTITIONS must be an integer") from error
    if partition_count <= 0:
        raise ValueError("KAFKA_TOPIC_PARTITIONS must be greater than zero")

    try:
        interval_seconds = float(values.get("PRODUCER_INTERVAL_SECONDS", "1"))
    except ValueError as error:
        raise ValueError("PRODUCER_INTERVAL_SECONDS must be a number") from error
    if interval_seconds <= 0:
        raise ValueError("PRODUCER_INTERVAL_SECONDS must be greater than zero")

    log_level = values.get("LOG_LEVEL", "INFO").upper()
    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}")

    return bootstrap_servers, topic, partition_count, interval_seconds, log_level


def delivery_report(error: Any, message: Any) -> None:
    """Log the asynchronous Kafka delivery result."""
    if error is not None:
        LOGGER.error("Erro ao publicar evento no Kafka: %s", error)
        return

    LOGGER.info(
        "Evento publicado topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def publish_event(kafka_producer: Any, topic: str, event: dict[str, str]) -> None:
    """Queue one event using content_id as its Kafka message key."""
    key = event["content_id"].encode("utf-8")
    value = serialize_event(event)

    while True:
        try:
            kafka_producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=delivery_report,
            )
            break
        except BufferError:
            LOGGER.warning("Fila local do Producer cheia; aguardando entregas pendentes")
            kafka_producer.poll(1)

    kafka_producer.poll(0)


def main() -> int:
    try:
        (
            bootstrap_servers,
            topic,
            partition_count,
            interval_seconds,
            log_level,
        ) = load_settings()
    except ValueError as error:
        logging.basicConfig(level=logging.ERROR)
        LOGGER.critical("Configuração inválida: %s", error)
        return 1

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient

    admin_client = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "playback-events-admin",
        }
    )
    kafka_producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "playback-events-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    stop_event = threading.Event()

    def request_shutdown(signum: int, _frame: Any) -> None:
        LOGGER.info("Shutdown solicitado signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    while not stop_event.is_set():
        try:
            created = ensure_topic(
                admin_client,
                topic,
                partition_count,
            )
            if created:
                LOGGER.info(
                    "Topic criado topic=%s partitions=%s",
                    topic,
                    partition_count,
                )
            else:
                LOGGER.info(
                    "Topic existente validado topic=%s partitions=%s",
                    topic,
                    partition_count,
                )
            LOGGER.info("Producer conectado ao Kafka")
            break
        except ValueError as error:
            LOGGER.critical("Configuração de topic incompatível: %s", error)
            return 1
        except KafkaException as error:
            LOGGER.error("Erro de conexão com Kafka: %s", error)
            stop_event.wait(5)

    try:
        while not stop_event.is_set():
            event = generate_event()
            publish_event(kafka_producer, topic, event)
            stop_event.wait(interval_seconds)
    finally:
        pending_messages = kafka_producer.flush(10)
        if pending_messages:
            LOGGER.warning("Shutdown com %s evento(s) ainda pendente(s)", pending_messages)
        else:
            LOGGER.info("Shutdown concluído")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
