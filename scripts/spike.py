"""Prove one real FDB commit reaches Kafka with its original commit version."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid

import fdb
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING, TopicPartition
from src.serialization import Mutation, MutationBatch, VersionEnd, deserialize_record


def main() -> None:
    """Write a unique key, decode upstream records, and assert version alignment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", type=float, default=10.0, help="CDC delivery deadline in seconds")
    args = parser.parse_args()
    if args.wait <= 0:
        parser.error("--wait must be positive")

    fdb.api_version(800)
    database = fdb.open(os.environ.get("FDB_CLUSTER_FILE", "/etc/foundationdb/fdb.cluster"))
    database.options.set_transaction_timeout(5000)
    stream_name = os.environ.get("CDC_STREAM_NAME", "foremen").encode()
    registration_deadline = time.monotonic() + 30
    while True:
        streams = database.list_cdc_streams().wait()
        if any(stream.name == stream_name for stream in streams):
            break
        if time.monotonic() >= registration_deadline:
            raise TimeoutError(f"Bridge has not registered CDC stream {stream_name!r}")
        time.sleep(0.25)

    topic = os.environ.get("KAFKA_TOPIC", "fdb.cdc.foremen")
    run_id = uuid.uuid4().hex
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": f"foremen-spike-{run_id}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic {topic!r} is not ready: {topic_metadata}")
        if len(topic_metadata.partitions) != 1:
            raise RuntimeError("Foremen requires exactly one Kafka partition")
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

        prefix = os.environ.get("FOREMEN_PREFIX", "foremen/").encode()
        key = prefix + f"spike/{run_id}".encode()
        value = json.dumps({"qty": 1, "rev": 1, "writer": "spike"}).encode()
        transaction = database.create_transaction()
        transaction[key] = value
        transaction.commit().wait()
        commit_version = transaction.get_committed_version()
        committed_at = time.monotonic()
        deadline = committed_at + args.wait
        matched: dict[str, object] | None = None
        watermark = -1
        while time.monotonic() < deadline:
            message = consumer.poll(min(0.5, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            record = deserialize_record(message.value())
            if record.stream_name != stream_name.decode():
                continue
            body = record.body
            if isinstance(body, VersionEnd):
                watermark = max(watermark, body.fdb_version)
                mutations = ()
            elif isinstance(body, MutationBatch):
                mutations = body.mutations
            elif isinstance(body, Mutation):
                mutations = (body,)
            else:
                raise TypeError(f"Unexpected record body: {type(body)}")
            for batch_index, mutation in enumerate(mutations):
                if mutation.param1 != key:
                    continue
                if mutation.type != 0 or mutation.param2 != value:
                    raise AssertionError("CDC mutation does not match the committed key/value")
                if mutation.version_index.fdb_version != commit_version:
                    raise AssertionError(
                        f"Version mismatch: client={commit_version}, CDC={mutation.version_index.fdb_version}"
                    )
                matched = {
                    "key": key.decode(),
                    "value": value.decode(),
                    "client_commit_version": commit_version,
                    "cdc_commit_version": mutation.version_index.fdb_version,
                    "sequence_no": mutation.version_index.sequence_no,
                    "kafka_offset": message.offset(),
                    "batch_index": batch_index,
                    "commit_to_kafka_seconds": round(time.monotonic() - committed_at, 4),
                }
            if matched is not None and watermark >= commit_version:
                matched["watermark"] = watermark
                print("M0 PASS: committed key/value and native CDC version match", flush=True)
                print(json.dumps(matched, indent=2), flush=True)
                return
        raise TimeoutError(
            f"No complete matching CDC delivery within {args.wait}s: "
            f"commit_version={commit_version}, matched={matched is not None}, watermark={watermark}"
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
