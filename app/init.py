import json
import logging
import os
import time
from pathlib import Path

import psycopg
import requests
from confluent_kafka.admin import AdminClient, NewTopic

from app.shared import TOPIC, ch_client, kafka_servers, registry_url, schema_str

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("init")


def retry(label, action, attempts=90, delay=2):
    for attempt in range(attempts):
        try:
            return action()
        except Exception as exc:
            log.warning("Waiting for %s (%d/%d): %s", label, attempt + 1, attempts, exc)
            time.sleep(delay)
    raise RuntimeError(f"{label} did not become available")


def init_kafka():
    admin = AdminClient({"bootstrap.servers": kafka_servers(), "socket.timeout.ms": 10000})

    def ensure():
        metadata = admin.list_topics(timeout=10)
        if len(metadata.brokers) < 3:
            raise RuntimeError(f"only {len(metadata.brokers)} brokers are ready")
        if TOPIC not in metadata.topics:
            futures = admin.create_topics([NewTopic(
                TOPIC, num_partitions=3, replication_factor=3,
                config={"min.insync.replicas": "2", "cleanup.policy": "delete"},
            )])
            try:
                futures[TOPIC].result(timeout=20)
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise
        topic = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
        if len(topic.partitions) < 3:
            raise RuntimeError("topic does not have >=3 partitions")
        for partition in topic.partitions.values():
            if len(partition.replicas) < 3:
                raise RuntimeError("topic is not replicated to 3 brokers")
        log.info("Kafka topic %s: partitions=%s replication=3", TOPIC, len(topic.partitions))

    retry("Kafka topic", ensure)


def init_registry():
    url = registry_url()
    schema = json.dumps(json.loads(schema_str()), ensure_ascii=False)

    def register():
        response = requests.post(
            f"{url}/subjects/{TOPIC}-value/versions",
            json={"schemaType": "AVRO", "schema": schema}, timeout=10,
        )
        response.raise_for_status()
        version = requests.get(f"{url}/subjects/{TOPIC}-value/versions/latest", timeout=10)
        version.raise_for_status()
        log.info("Registered Avro schema: subject=%s-value version=%s id=%s",
                 TOPIC, version.json()["version"], response.json()["id"])

    retry("Schema Registry", register)


def init_postgres():
    def migrate():
        with psycopg.connect(os.environ["PG_DSN"]) as conn:
            conn.execute(Path("sql/postgres.sql").read_text(encoding="utf-8"))
        log.info("PostgreSQL migrations done")

    retry("PostgreSQL", migrate)


def init_clickhouse():
    queries = [q.strip() for q in Path("sql/clickhouse.sql").read_text(encoding="utf-8").split(";") if q.strip()]

    def migrate():
        with ch_client() as client:
            for sql in queries:
                client.command(sql)
        log.info("ClickHouse migrations done (%s statements)", len(queries))

    retry("ClickHouse", migrate, attempts=45, delay=3)


if __name__ == "__main__":
    init_kafka()
    init_registry()
    init_postgres()
    init_clickhouse()
    log.info("All initializations completed")
