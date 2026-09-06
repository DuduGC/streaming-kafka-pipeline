import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_SERVICES = {"kafka", "postgres", "producer", "consumer"}

pytestmark = pytest.mark.integration


def run_compose(
    *arguments: str,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["docker", "compose", *arguments],
            cwd=PROJECT_ROOT,
            capture_output=True,
            check=False,
            encoding="utf-8",
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        pytest.fail("Docker CLI não encontrado no PATH")
    except subprocess.TimeoutExpired:
        pytest.fail(f"docker compose {' '.join(arguments)} excedeu {timeout}s")

    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip()
        pytest.fail(
            f"docker compose {' '.join(arguments)} falhou: {details}"
        )
    return result


@pytest.fixture
def running_compose() -> None:
    if os.environ.get("RUN_DOCKER_INTEGRATION") != "1":
        pytest.skip("defina RUN_DOCKER_INTEGRATION=1 para executar")

    running_services = set(
        run_compose("ps", "--status", "running", "--services")
        .stdout.strip()
        .splitlines()
    )
    missing_services = REQUIRED_SERVICES - running_services
    assert not missing_services, (
        "execute 'docker compose up -d --build' antes do teste; "
        f"serviços ausentes: {sorted(missing_services)}"
    )


def query_event(event_id: str) -> dict[str, Any] | None:
    sql = (
        "SELECT json_build_object("
        "'event_id', event_id::text, "
        "'user_id', user_id, "
        "'content_id', content_id, "
        "'event_type', event_type, "
        "'event_timestamp', event_timestamp::text, "
        "'processed_at_present', processed_at IS NOT NULL"
        ")::text FROM playback_events "
        f"WHERE event_id = '{event_id}';"
    )
    result = run_compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" '
        '-d "$POSTGRES_DB" -tA -c "$1"',
        "sh",
        sql,
    )
    output = result.stdout.strip()
    return json.loads(output) if output else None


def delete_event(event_id: str) -> None:
    sql = f"DELETE FROM playback_events WHERE event_id = '{event_id}';"
    run_compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" '
        '-d "$POSTGRES_DB" -q -c "$1"',
        "sh",
        sql,
    )


def query_application_role_attributes() -> dict[str, bool]:
    script = "\n".join(
        (
            "import json",
            "from sqlalchemy import text",
            "from consumer.database import create_database_engine",
            "engine = create_database_engine()",
            "with engine.connect() as connection:",
            "    row = connection.execute(text(\"\"\"",
            "        SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication",
            "        FROM pg_roles WHERE rolname = current_user",
            "    \"\"\")).mappings().one()",
            "print(json.dumps(dict(row)))",
            "engine.dispose()",
        )
    )
    result = run_compose(
        "exec",
        "-T",
        "consumer",
        "python",
        "-c",
        script,
    )
    return json.loads(result.stdout.strip())


def publish_with_application_producer(event: dict[str, str]) -> None:
    event_json = json.dumps(event, separators=(",", ":"))
    script = "\n".join(
        (
            "import json",
            "import os",
            "from confluent_kafka import Producer",
            "from producer.producer import publish_event",
            f"event = json.loads({event_json!r})",
            "producer = Producer({",
            "    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],",
            "    'client.id': 'pipeline-integration-test',",
            "    'acks': 'all',",
            "    'enable.idempotence': True,",
            "})",
            "publish_event(producer, os.environ['KAFKA_TOPIC'], event)",
            "pending = producer.flush(10)",
            "raise SystemExit(0 if pending == 0 else 1)",
        )
    )
    run_compose("exec", "-T", "producer", "python", "-c", script)


def test_producer_kafka_consumer_postgres_pipeline(
    running_compose: None,
) -> None:
    event = {
        "event_id": str(uuid4()),
        "user_id": "user_integration",
        "content_id": "content_integration",
        "event_type": "PLAY",
        "timestamp": "2026-09-06T15:00:00Z",
    }

    try:
        publish_with_application_producer(event)

        deadline = time.monotonic() + 20
        persisted_event = None
        while time.monotonic() < deadline:
            persisted_event = query_event(event["event_id"])
            if persisted_event is not None:
                break
            time.sleep(0.5)

        assert persisted_event is not None, "evento não persistido em até 20s"
        assert persisted_event["event_id"] == event["event_id"]
        assert persisted_event["user_id"] == "user_integration"
        assert persisted_event["content_id"] == "content_integration"
        assert persisted_event["event_type"] == "PLAY"
        assert datetime.fromisoformat(
            persisted_event["event_timestamp"].replace("Z", "+00:00")
        ) == datetime.fromisoformat("2026-09-06T15:00:00+00:00")
        assert persisted_event["processed_at_present"] is True
    finally:
        delete_event(event["event_id"])


def test_application_admin_creates_three_partition_topic(
    running_compose: None,
) -> None:
    topic = f"playback-events-test-{uuid4().hex}"
    topic_created = False
    script = "\n".join(
        (
            "import os",
            "from confluent_kafka.admin import AdminClient",
            "from producer.producer import ensure_topic",
            "admin = AdminClient({",
            "    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],",
            "    'client.id': 'topic-integration-test',",
            "})",
            f"created = ensure_topic(admin, {topic!r}, 3)",
            "raise SystemExit(0 if created else 1)",
        )
    )

    try:
        run_compose("exec", "-T", "producer", "python", "-c", script)
        topic_created = True
        description = run_compose(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--topic",
            topic,
        ).stdout

        assert "PartitionCount: 3" in description
        assert "ReplicationFactor: 1" in description
    finally:
        if topic_created:
            run_compose(
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--delete",
                "--topic",
                topic,
            )


def test_application_database_role_has_no_cluster_admin_privileges(
    running_compose: None,
) -> None:
    assert query_application_role_attributes() == {
        "rolsuper": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolreplication": False,
    }
