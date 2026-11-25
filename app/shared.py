import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

import clickhouse_connect
from pydantic import BaseModel, ConfigDict, Field, field_validator

TOPIC = "movie-events"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "movie_event.avsc"


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def utc_required(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include timezone, e.g. Z")
        return value.astimezone(timezone.utc)

    def as_avro(self) -> dict:
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": int(self.timestamp.timestamp() * 1000),
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


def schema_str() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "clickhouse"),
        port=int(os.getenv("CH_PORT", "8123")),
        username="default",
        password=os.getenv("CH_PASSWORD", "cinema_pass"),
    )


def kafka_servers() -> str:
    return os.getenv("KAFKA_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")


def registry_url() -> str:
    return os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
