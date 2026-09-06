CREATE TABLE IF NOT EXISTS playback_events (
    event_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    content_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
