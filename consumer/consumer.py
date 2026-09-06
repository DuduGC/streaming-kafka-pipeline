import logging
import os
import signal
import threading
from collections.abc import Callable, Mapping
from typing import Any

from consumer.database import PersistenceError, create_database_engine, persist_event
from consumer.validator import validate_event_message


LOGGER = logging.getLogger("consumer")
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def load_settings(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str, str, str]:
    """Load and validate the Consumer settings."""
    values = os.environ if environ is None else environ

    required_names = (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "KAFKA_CONSUMER_GROUP",
    )
    required_values: list[str] = []
    for name in required_names:
        value = values.get(name, "").strip()
        if not value:
            raise ValueError(f"{name} must be set")
        required_values.append(value)

    log_level = values.get("LOG_LEVEL", "INFO").upper()
    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}")

    return required_values[0], required_values[1], required_values[2], log_level


def build_consumer_config(
    bootstrap_servers: str,
    consumer_group: str,
) -> dict[str, str | bool]:
    """Build Kafka settings with explicit offset management."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }


def process_message(
    kafka_consumer: Any,
    message: Any,
    persist: Callable[[dict[str, Any]], bool],
) -> bool:
    """Process one Kafka message and synchronously commit its offset."""
    try:
        event = validate_event_message(message.value())
    except ValueError as error:
        LOGGER.warning(
            "Evento inválido descartado topic=%s partition=%s offset=%s reason=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            error,
        )
        kafka_consumer.commit(message=message, asynchronous=False)
        return False

    LOGGER.info(
        "Evento recebido event_id=%s topic=%s partition=%s offset=%s",
        event.get("event_id", "missing"),
        message.topic(),
        message.partition(),
        message.offset(),
    )
    inserted = persist(event)
    if inserted:
        LOGGER.info("Evento persistido event_id=%s", event["event_id"])
    else:
        LOGGER.info("Evento duplicado ignorado event_id=%s", event["event_id"])
    kafka_consumer.commit(message=message, asynchronous=False)
    LOGGER.info(
        "Offset confirmado topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )
    return True


def main() -> int:
    try:
        bootstrap_servers, topic, consumer_group, log_level = load_settings()
    except ValueError as error:
        logging.basicConfig(level=logging.ERROR)
        LOGGER.critical("Configuração inválida: %s", error)
        return 1

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        database_engine = create_database_engine()
    except ValueError as error:
        LOGGER.critical("Configuração inválida: %s", error)
        return 1

    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    kafka_consumer = Consumer(
        build_consumer_config(bootstrap_servers, consumer_group)
    )
    stop_event = threading.Event()

    def request_shutdown(signum: int, _frame: Any) -> None:
        LOGGER.info("Shutdown solicitado signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    try:
        while not stop_event.is_set():
            try:
                metadata = kafka_consumer.list_topics(timeout=10)
                LOGGER.info("Consumer conectado ao Kafka brokers=%s", len(metadata.brokers))
                break
            except KafkaException as error:
                LOGGER.error("Erro de conexão com Kafka: %s", error)
                stop_event.wait(5)

        if not stop_event.is_set():
            kafka_consumer.subscribe([topic])
            LOGGER.info("Consumer inscrito topic=%s group=%s", topic, consumer_group)

        while not stop_event.is_set():
            message = kafka_consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    LOGGER.debug(
                        "Fim da partition topic=%s partition=%s offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
                else:
                    LOGGER.error("Erro ao consumir mensagem: %s", message.error())
                continue

            try:
                process_message(
                    kafka_consumer,
                    message,
                    lambda event: persist_event(database_engine, event),
                )
            except PersistenceError as error:
                LOGGER.error(
                    "Erro ao persistir evento topic=%s partition=%s offset=%s error=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    error,
                )
                kafka_consumer.seek(
                    TopicPartition(
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
                )
                stop_event.wait(5)
            except KafkaException as error:
                LOGGER.error(
                    "Falha ao confirmar offset topic=%s partition=%s offset=%s error=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    error,
                )
    finally:
        kafka_consumer.close()
        database_engine.dispose()
        LOGGER.info("Shutdown concluído")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
