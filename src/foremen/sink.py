"""Consume the bridge's ordered partition and persist CDC before acknowledging it."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import signal
import threading
from typing import Any


mutation_columns = [
    "commit_version", "key", "op_code", "value", "range_end",
    "kafka_offset", "batch_index",
]
version_end_columns = ["commit_version", "kafka_offset"]
health_path = Path("/tmp/foremen-sink-health")
logger = logging.getLogger(__name__)


def logical_key(key: bytes, prefix: bytes) -> str:
    """Remove precisely the configured watch prefix from a readable key."""
    if not key.startswith(prefix):
        raise ValueError(f"CDC key {key!r} is outside watch prefix {prefix!r}")
    return key[len(prefix):].decode("utf-8")


def flatten_record(
    record: Any, kafka_offset: int, prefix: bytes = b"foremen/"
) -> tuple[list[list[Any]], list[list[int]]]:
    """Flatten the bridge's typed record while preserving offset/index identity."""
    from src.serialization import Mutation, MutationBatch, VersionEnd

    body = record.body
    if isinstance(body, VersionEnd):
        return [], [[body.fdb_version, kafka_offset]]
    if isinstance(body, Mutation):
        mutations = (body,)
    elif isinstance(body, MutationBatch):
        mutations = body.mutations
    else:
        raise TypeError(f"Unknown bridge record body: {type(body).__name__}")

    rows = []
    for index, mutation in enumerate(mutations):
        key = logical_key(mutation.param1, prefix)
        value = mutation.param2
        range_end = ""
        if mutation.type == 1:
            # The end may be the prefix's exclusive upper bound, outside it.
            if mutation.param2.startswith(prefix):
                range_end = mutation.param2[len(prefix):].decode("utf-8")
            else:
                range_end = mutation.param2.decode("utf-8")
            value = b""
        rows.append([
            mutation.version_index.fdb_version, key, mutation.type,
            value, range_end, kafka_offset, index,
        ])
    return rows, []


def persist_messages(
    client: Any,
    consumer: Any,
    messages: list[Any],
    *,
    prefix: bytes = b"foremen/",
    stream_name: str = "foremen",
) -> None:
    """Persist an ordered batch, then acknowledge its final Kafka offset.

    Validate the entire batch before writing. All mutations must be durable
    before any of its boundaries can advance the detector's watermark. Any
    failure stops consumption so the unacknowledged batch can replay intact.
    """
    from src.serialization import deserialize_record

    if not messages:
        return
    mutations = []
    boundaries = []
    for message in messages:
        if message.error():
            raise RuntimeError(f"Kafka consumer error: {message.error()}")
        if message.partition() != 0:
            raise ValueError("Foremen requires exactly one Kafka partition (0)")
        payload = message.value()
        if payload is None:
            raise ValueError("Unexpected tombstone in the CDC topic")
        record = deserialize_record(payload)
        if record.stream_name != stream_name:
            raise ValueError(f"Unexpected CDC stream {record.stream_name!r}")
        record_mutations, record_boundaries = flatten_record(record, message.offset(), prefix)
        mutations.extend(record_mutations)
        boundaries.extend(record_boundaries)
    if mutations:
        client.insert("mutations", mutations, column_names=mutation_columns)
    if boundaries:
        client.insert("version_ends", boundaries, column_names=version_end_columns)
    committed = consumer.commit(message=messages[-1], asynchronous=False)
    # confluent-kafka may return partition-specific errors from a sync commit.
    for partition in committed or []:
        if partition.error is not None:
            raise RuntimeError(f"Kafka offset commit failed: {partition.error}")


def persist_message(
    client: Any,
    consumer: Any,
    message: Any,
    *,
    prefix: bytes = b"foremen/",
    stream_name: str = "foremen",
) -> None:
    """Persist one record using the same ordering guarantees as a batch."""
    persist_messages(client, consumer, [message], prefix=prefix, stream_name=stream_name)


def run() -> None:
    """Run one sink consumer; let Compose restart it after any delivery failure."""
    from confluent_kafka import Consumer
    from foremen.recorder import clickhouse_client

    health_path.unlink(missing_ok=True)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    topic = os.environ.get("KAFKA_TOPIC", "fdb.cdc.foremen")
    prefix = os.environ.get("FOREMEN_PREFIX", "foremen/").encode()
    stream_name = os.environ.get("CDC_STREAM_NAME", "foremen")
    client = clickhouse_client()
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.environ.get("KAFKA_GROUP_ID", "foremen-clickhouse-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "isolation.level": "read_committed",
        "max.poll.interval.ms": 300000,
    })
    try:
        for table in ("mutations", "version_ends", "agent_decisions"):
            client.query(f"SELECT count() FROM {table}")
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic {topic!r} is unavailable")
        if sorted(topic_metadata.partitions) != [0]:
            raise ValueError("Foremen requires a topic with exactly one partition")
        consumer.subscribe([topic])
        health_path.touch()
        logger.info("Consuming %s into ClickHouse", topic)
        while not stop.is_set():
            messages = consumer.consume(num_messages=256, timeout=0.1)
            if messages:
                persist_messages(
                    client, consumer, messages, prefix=prefix, stream_name=stream_name
                )
            health_path.touch()
    finally:
        health_path.unlink(missing_ok=True)
        consumer.close()
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
