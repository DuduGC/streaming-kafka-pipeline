import os
from collections.abc import Mapping
from typing import Any

from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


INSERT_EVENT_SQL = text(
    """
    INSERT INTO playback_events (
        event_id,
        user_id,
        content_id,
        event_type,
        event_timestamp
    ) VALUES (
        :event_id,
        :user_id,
        :content_id,
        :event_type,
        :event_timestamp
    )
    ON CONFLICT (event_id) DO NOTHING
    """
)


class PersistenceError(RuntimeError):
    """Raised when an event cannot be persisted."""


def create_database_engine(environ: Mapping[str, str] | None = None) -> Engine:
    """Create a PostgreSQL engine from required environment variables."""
    values = os.environ if environ is None else environ
    required_names = (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
    settings: dict[str, str] = {}
    for name in required_names:
        value = values.get(name, "").strip()
        if not value:
            raise ValueError(f"{name} must be set")
        settings[name] = value

    try:
        port = int(settings["POSTGRES_PORT"])
    except ValueError as error:
        raise ValueError("POSTGRES_PORT must be an integer") from error
    if not 1 <= port <= 65_535:
        raise ValueError("POSTGRES_PORT must be between 1 and 65535")

    database_url = URL.create(
        drivername="postgresql+psycopg",
        username=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
        host=settings["POSTGRES_HOST"],
        port=port,
        database=settings["POSTGRES_DB"],
    )
    return create_engine(database_url, pool_pre_ping=True)


def persist_event(engine: Engine, event: dict[str, Any]) -> bool:
    """Persist one event, returning False when event_id already exists."""
    parameters = {
        "event_id": event["event_id"],
        "user_id": event["user_id"],
        "content_id": event["content_id"],
        "event_type": event["event_type"],
        "event_timestamp": event["timestamp"],
    }
    try:
        with engine.begin() as connection:
            result = connection.execute(INSERT_EVENT_SQL, parameters)
            return result.rowcount == 1
    except SQLAlchemyError as error:
        raise PersistenceError("failed to persist playback event") from error
