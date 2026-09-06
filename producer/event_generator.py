import json
import random
from datetime import datetime, timezone
from uuid import uuid4


EVENT_TYPES = ("PLAY", "PAUSE", "BUFFERING", "ERROR", "COMPLETE")


def generate_event() -> dict[str, str]:
    """Generate one synthetic playback event."""
    return {
        "event_id": str(uuid4()),
        "user_id": f"user_{random.randint(1, 1_000)}",
        "content_id": f"content_{random.randint(1, 100)}",
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def serialize_event(event: dict[str, str]) -> bytes:
    """Serialize an event as compact UTF-8 JSON."""
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
