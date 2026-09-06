from sqlalchemy import create_engine, text


def create_test_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE playback_events (
                    event_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    content_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

    return engine


def playback_event() -> dict[str, str]:
    return {
        "event_id": "5b834643-48c7-4b40-8a72-e6a3453e58b8",
        "user_id": "user_123",
        "content_id": "content_42",
        "event_type": "PLAY",
        "timestamp": "2026-09-05T12:00:00Z",
    }


def test_persist_event_inserts_all_event_fields() -> None:
    from consumer.database import persist_event

    engine = create_test_engine()
    event = playback_event()

    assert persist_event(engine, event) is True

    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT event_id, user_id, content_id, event_type,
                       event_timestamp, processed_at
                FROM playback_events
                """
            )
        ).mappings().one()

    assert row["event_id"] == event["event_id"]
    assert row["user_id"] == "user_123"
    assert row["content_id"] == "content_42"
    assert row["event_type"] == "PLAY"
    assert row["event_timestamp"] == "2026-09-05T12:00:00Z"
    assert row["processed_at"] is not None


def test_persist_event_ignores_duplicate_event_id() -> None:
    from consumer.database import persist_event

    engine = create_test_engine()
    event = playback_event()

    first_inserted = persist_event(engine, event)
    duplicate_inserted = persist_event(engine, event)

    with engine.connect() as connection:
        total = connection.execute(
            text("SELECT COUNT(*) FROM playback_events")
        ).scalar_one()

    assert first_inserted is True
    assert duplicate_inserted is False
    assert total == 1
