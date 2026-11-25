import asyncio
import logging
import os
import random
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException

from app.shared import DeviceType, EventType, MovieEvent, TOPIC, kafka_servers, registry_url, schema_str

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


class KafkaPublisher:
    def __init__(self):
        registry = SchemaRegistryClient({"url": registry_url()})
        self.serializer = AvroSerializer(
            registry, schema_str(), conf={"auto.register.schemas": False}
        )
        self.client = Producer({
            "bootstrap.servers": kafka_servers(),
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 200,
            "message.timeout.ms": 20000,
        })
        self.lock = threading.Lock()

    def send(self, event: MovieEvent) -> str:
        payload = self.serializer(
            event.as_avro(), SerializationContext(TOPIC, MessageField.VALUE)
        )
        with self.lock:
            for attempt in range(4):
                errors = []
                try:
                    self.client.produce(
                        TOPIC, key=event.user_id.encode("utf-8"), value=payload,
                        on_delivery=lambda err, msg: errors.append(err) if err else None,
                    )
                    remaining = self.client.flush(25)
                    if remaining or errors:
                        raise RuntimeError(f"Kafka delivery incomplete: {errors}")
                    log.info("published event_id=%s event_type=%s timestamp=%s",
                             event.event_id, event.event_type.value, event.timestamp.isoformat())
                    return str(event.event_id)
                except (KafkaException, BufferError, RuntimeError) as exc:
                    if attempt == 3:
                        raise
                    delay = min(0.25 * 2**attempt, 3)
                    log.warning("Kafka retry=%d error=%s backoff=%.2fs", attempt + 1, exc, delay)
                    time.sleep(delay)
        raise RuntimeError("Kafka publish failed")


publisher = None


def stable_event(user, movie, session, event_type, dt, progress, device="MOBILE", seed=False):
    identity = f"demo:v1:{user}:{session}:{event_type}:{progress}"
    return MovieEvent(
        event_id=uuid5(NAMESPACE_URL, identity) if seed else uuid4(),
        user_id=user, movie_id=movie, event_type=event_type,
        timestamp=dt, device_type=device, session_id=session,
        progress_seconds=progress,
    )


def one_session(user, movie, day, session, seed):
    now = datetime.now(timezone.utc)
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=9)
    if start > now - timedelta(minutes=5):
        start = now - timedelta(minutes=5)
    device = ["MOBILE", "DESKTOP", "TV", "TABLET"][sum(map(ord, user)) % 4]
    events = [stable_event(user, movie, session, "VIEW_STARTED", start, 0, device, seed)]
    if int(uuid5(NAMESPACE_URL, session).int % 3) == 0:
        events.append(stable_event(user, movie, session, "VIEW_PAUSED", start + timedelta(minutes=1), 60, device, seed))
        events.append(stable_event(user, movie, session, "VIEW_RESUMED", start + timedelta(minutes=2), 60, device, seed))
    if int(uuid5(NAMESPACE_URL, session).int % 5) != 0:
        events.append(stable_event(user, movie, session, "VIEW_FINISHED", start + timedelta(minutes=3), 180, device, seed))
    return events


def seed_historical_events():
    today = datetime.now(timezone.utc).date()
    count = 0
    for offset in range(11, -1, -1):
        for index in range(6):
            user = f"demo-cohort-{offset}-{index}"
            movie = f"movie-{(offset + index) % 8 + 1}"
            for day_offset in range(min(8, offset + 1)):
                if day_offset > 0 and (index + day_offset) % (day_offset // 2 + 2) == 0:
                    continue
                day = today - timedelta(days=offset - day_offset)
                session = f"seed-{offset}-{index}-{day_offset}"
                for event in one_session(user, movie, day, session, seed=True):
                    publisher.send(event)
                    count += 1
    log.info("seeded history events=%d days=12", count)


def generate_live_session():
    user = f"live-user-{random.randint(1, 30)}"
    movie = f"movie-{random.randint(1, 8)}"
    session = f"live-{uuid4()}"
    for event in one_session(user, movie, datetime.now(timezone.utc).date(), session, seed=False):
        publisher.send(event)
    if random.random() < 0.25:
        publisher.send(stable_event(user, movie, session, "LIKED", datetime.now(timezone.utc), None))
    if random.random() < 0.15:
        publisher.send(stable_event(user, movie, session, "SEARCHED", datetime.now(timezone.utc), None))


async def generator_task():
    if os.getenv("GENERATE_EVENTS", "true").lower() != "true":
        return
    await asyncio.sleep(2)
    while True:
        try:
            await asyncio.to_thread(seed_historical_events)
            break
        except Exception:
            log.exception("seed failed; retrying")
            await asyncio.sleep(10)
    while True:
        try:
            await asyncio.to_thread(generate_live_session)
        except Exception:
            log.exception("live generator failed")
        await asyncio.sleep(float(os.getenv("GENERATE_INTERVAL_SECONDS", "3")))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global publisher
    publisher = KafkaPublisher()
    task = asyncio.create_task(generator_task())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="Movie Producer", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "producer"}


@app.post("/events", status_code=201)
def publish(event: MovieEvent):
    try:
        event_id = publisher.send(event)
        return {"event_id": event_id, "status": "published"}
    except Exception as exc:
        log.exception("failed event_id=%s", event.event_id)
        raise HTTPException(503, "Kafka publication failed") from exc
