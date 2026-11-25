import csv
import io
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import boto3
import clickhouse_connect
import psycopg
import pytest
import requests
from botocore.config import Config
from confluent_kafka.admin import AdminClient

from app.shared import MovieEvent, ch_client, kafka_servers, registry_url

PRODUCER = "http://producer:8000"
AGGREGATOR = "http://aggregator:8000"


def wait_for_event(event_id, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with ch_client() as ch:
            rows = ch.query("""
                SELECT event_id, user_id, movie_id, event_type, device_type,
                       session_id, progress_seconds
                FROM cinema.raw_events FINAL WHERE event_id = {id:UUID}
            """, parameters={"id": event_id}).result_rows
        if rows:
            return rows[0]
        time.sleep(2)
    pytest.fail(f"Kafka -> ClickHouse timeout for {event_id}")


def test_avro_schema_and_replicated_topic():
    response = requests.get(f"{registry_url()}/subjects/movie-events-value/versions/latest", timeout=10)
    response.raise_for_status()
    assert response.json()["version"] >= 1
    assert response.json().get("schemaType", "AVRO") == "AVRO"
    meta = AdminClient({"bootstrap.servers": kafka_servers()}).list_topics(timeout=10)
    topic = meta.topics["movie-events"]
    assert len(topic.partitions) >= 3
    assert all(len(part.replicas) >= 2 for part in topic.partitions.values())


def test_pipeline_aggregation_and_s3_export():
    unique = uuid4().hex
    session = f"test-session-{unique}"
    user = f"test-user-{unique}"
    now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=5)
    events = [
        {
            "event_id": str(uuid4()),
            "user_id": user,
            "movie_id": "test-movie",
            "event_type": kind,
            "timestamp": when.isoformat(),
            "device_type": "TABLET",
            "session_id": session,
            "progress_seconds": progress,
        }
        for kind, when, progress in [
            ("VIEW_STARTED", now, 0),
            ("VIEW_PAUSED", now + timedelta(seconds=60), 60),
            ("VIEW_RESUMED", now + timedelta(seconds=65), 60),
            ("VIEW_FINISHED", now + timedelta(seconds=180), 180),
        ]
    ]
    for event in events:
        response = requests.post(f"{PRODUCER}/events", json=event, timeout=35)
        assert response.status_code == 201, response.text
        assert response.json()["event_id"] == event["event_id"]
    for event in events:
        result = wait_for_event(event["event_id"])
        assert str(result[0]) == event["event_id"]
        assert result[1] == user
        assert result[2] == "test-movie"
        assert result[3] == event["event_type"]
        assert result[4] == "TABLET"
        assert result[5] == session
        assert result[6] == event["progress_seconds"]

    day = now.date().isoformat()
    for _ in range(2):
        result = requests.post(f"{AGGREGATOR}/aggregate/{day}", timeout=90)
        assert result.status_code == 200, result.text
    with psycopg.connect(os.environ["PG_DSN"]) as pg:
        rows = pg.execute("SELECT metric, dimension, value FROM aggregates WHERE day=%s", (now.date(),)).fetchall()
        assert {m for m, d, v in rows} >= {
            "dau", "avg_watch_seconds", "view_conversion_percent", "retention_percent", "top_movie_views"
        }
        assert pg.execute("""
            SELECT count(*) FROM aggregates
            WHERE day=%s AND metric='dau' AND dimension=''
        """, (now.date(),)).fetchone()[0] == 1

    result = requests.post(f"{AGGREGATOR}/export/{day}", timeout=40)
    assert result.status_code == 200, result.text
    assert result.json()["s3"] == f"s3://movie-analytics/daily/{day}/aggregates.csv"
    s3 = boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
    )
    obj = s3.get_object(Bucket="movie-analytics", Key=f"daily/{day}/aggregates.csv")
    records = list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8"))))
    assert any(row["metric"] == "dau" for row in records)
    assert any(row["metric"] == "retention_percent" for row in records)
    assert all(row["date"] == day and row["computed_at_utc"] for row in records)


def test_validation_rejects_wrong_enum():
    body = {
        "user_id": "test", "movie_id": "movie-1", "event_type": "NOT_AN_EVENT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": "MOBILE", "session_id": "validation",
    }
    assert requests.post(f"{PRODUCER}/events", json=body, timeout=10).status_code == 422


def test_validation_requires_timezone():
    body = {
        "user_id": "test", "movie_id": "movie-1", "event_type": "VIEW_STARTED",
        "timestamp": "2026-01-01T10:00:00", "device_type": "MOBILE", "session_id": "validation",
    }
    assert requests.post(f"{PRODUCER}/events", json=body, timeout=10).status_code == 422
