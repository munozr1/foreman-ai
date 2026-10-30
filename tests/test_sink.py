"""Exercise the real bridge codec and crash/replay acknowledgement boundaries."""

from types import SimpleNamespace

import pytest

from src.serialization import (
    Mutation,
    MutationBatch,
    Record,
    VersionIndex,
    serialize_batch,
    serialize_version_end,
)
from foremen.sink import flatten_record, persist_message, persist_messages


class Message:
    def __init__(self, payload: bytes, offset: int = 42, partition: int = 0) -> None:
        self.payload = payload
        self.position = offset
        self.partition_id = partition

    def error(self):
        return None

    def value(self) -> bytes:
        return self.payload

    def offset(self) -> int:
        return self.position

    def partition(self) -> int:
        return self.partition_id


class Client:
    def __init__(self, events: list, fail: bool = False, fail_table: str | None = None) -> None:
        self.events = events
        self.fail = fail
        self.fail_table = fail_table
        self.rows = []

    def insert(self, table: str, rows: list, column_names: list[str]) -> None:
        self.events.append(("insert", table))
        if self.fail or table == self.fail_table:
            raise OSError("ClickHouse unavailable")
        self.rows.append((table, rows, column_names))


class Consumer:
    def __init__(self, events: list, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def commit(self, message: Message, asynchronous: bool) -> list:
        assert not asynchronous
        self.events.append(("commit", message.offset()))
        if self.fail:
            raise OSError("Kafka unavailable")
        return []


def mutation_payload(stream_name: str = "foremen") -> bytes:
    return serialize_batch(
        [SimpleNamespace(type=0, param1=b"foremen/inv/sku-1", param2=b'{"writer":"b"}')],
        fdb_version=123,
        first_sequence_no=7,
        stream_name=stream_name,
        bridge_timestamp_ns=0,
    )


def test_actual_codec_and_offset_are_preserved() -> None:
    events = []
    client = Client(events)
    persist_message(client, Consumer(events), Message(mutation_payload()))
    assert events == [("insert", "mutations"), ("commit", 42)]
    assert client.rows[0][1] == [[123, "inv/sku-1", 0, b'{"writer":"b"}', "", 42, 0]]


def test_insert_failure_leaves_kafka_offset_uncommitted() -> None:
    events = []
    with pytest.raises(OSError, match="ClickHouse"):
        persist_message(Client(events, fail=True), Consumer(events), Message(mutation_payload()))
    assert events == [("insert", "mutations")]


def test_commit_failure_replay_retains_exact_dedup_identity() -> None:
    events = []
    client = Client(events)
    message = Message(mutation_payload())
    with pytest.raises(OSError, match="Kafka"):
        persist_message(client, Consumer(events, fail=True), message)
    persist_message(client, Consumer(events), message)
    assert client.rows[0] == client.rows[1]


def test_boundary_is_inserted_before_acknowledgement() -> None:
    payload = serialize_version_end(
        fdb_version=123, total_mutations=1, stream_name="foremen", bridge_timestamp_ns=0
    )
    events = []
    client = Client(events)
    persist_message(client, Consumer(events), Message(payload, offset=43))
    assert events == [("insert", "version_ends"), ("commit", 43)]
    assert client.rows[0][1] == [[123, 43]]


def test_failed_boundary_is_not_acknowledged() -> None:
    payload = serialize_version_end(
        fdb_version=123, total_mutations=1, stream_name="foremen", bridge_timestamp_ns=0
    )
    events = []
    with pytest.raises(OSError):
        persist_message(Client(events, fail=True), Consumer(events), Message(payload))
    assert events == [("insert", "version_ends")]


def test_clear_range_is_preserved_separately_from_value() -> None:
    record = Record("foremen", None, MutationBatch((
        Mutation(1, b"foremen/inv/a", b"foremen/inv/z", VersionIndex(123, 0)),
        Mutation(0, b"foremen/inv/b", b"value", VersionIndex(123, 1)),
    )))
    rows, ends = flatten_record(record, 7)
    assert rows == [
        [123, "inv/a", 1, b"", "inv/z", 7, 0],
        [123, "inv/b", 0, b"value", "", 7, 1],
    ]
    assert ends == []


@pytest.mark.parametrize("payload,partition", [
    (b"not protobuf", 0),
    (mutation_payload("other-stream"), 0),
    (mutation_payload(), 1),
])
def test_bad_records_cannot_advance_offsets(payload: bytes, partition: int) -> None:
    events = []
    with pytest.raises(Exception):
        persist_message(Client(events), Consumer(events), Message(payload, partition=partition))
    assert events == []


def test_outside_watch_range_rejected() -> None:
    record = Record("foremen", None, Mutation(0, b"other/inv/a", b"value", VersionIndex(1, 0)))
    with pytest.raises(ValueError, match="outside watch prefix"):
        flatten_record(record, 0)


def ordered_batch() -> list[Message]:
    first = serialize_batch(
        [SimpleNamespace(type=0, param1=b"foremen/inv/a", param2=b"first"),
         SimpleNamespace(type=0, param1=b"foremen/inv/b", param2=b"second")],
        fdb_version=123, first_sequence_no=7, stream_name="foremen", bridge_timestamp_ns=0,
    )
    second = serialize_batch(
        [SimpleNamespace(type=0, param1=b"foremen/inv/a", param2=b"third")],
        fdb_version=124, first_sequence_no=0, stream_name="foremen", bridge_timestamp_ns=0,
    )
    return [
        Message(first, offset=42),
        Message(serialize_version_end(fdb_version=123, total_mutations=2,
                                      stream_name="foremen", bridge_timestamp_ns=0), offset=43),
        Message(second, offset=44),
        Message(serialize_version_end(fdb_version=124, total_mutations=1,
                                      stream_name="foremen", bridge_timestamp_ns=0), offset=45),
    ]


def test_batch_inserts_all_mutations_before_boundaries_and_acks_only_last_offset() -> None:
    events = []
    client = Client(events)
    persist_messages(client, Consumer(events), ordered_batch())
    assert events == [("insert", "mutations"), ("insert", "version_ends"), ("commit", 45)]
    assert client.rows[0][1] == [
        [123, "inv/a", 0, b"first", "", 42, 0],
        [123, "inv/b", 0, b"second", "", 42, 1],
        [124, "inv/a", 0, b"third", "", 44, 0],
    ]
    assert client.rows[1][1] == [[123, 43], [124, 45]]


def test_failed_batch_mutations_do_not_advance_boundaries_or_offsets() -> None:
    events = []
    with pytest.raises(OSError, match="ClickHouse"):
        persist_messages(Client(events, fail_table="mutations"), Consumer(events), ordered_batch())
    assert events == [("insert", "mutations")]


def test_failed_batch_boundary_replays_mutations_with_identical_dedup_keys() -> None:
    events = []
    client = Client(events, fail_table="version_ends")
    messages = ordered_batch()
    with pytest.raises(OSError, match="ClickHouse"):
        persist_messages(client, Consumer(events), messages)
    assert events == [("insert", "mutations"), ("insert", "version_ends")]
    client.fail_table = None
    persist_messages(client, Consumer(events), messages)
    assert client.rows[0] == client.rows[1]
    assert events[-1] == ("commit", 45)


@pytest.mark.parametrize("payload,partition", [
    (b"not protobuf", 0),
    (mutation_payload("other-stream"), 0),
    (mutation_payload(), 1),
])
def test_invalid_batch_tail_prevents_every_write_and_ack(payload: bytes, partition: int) -> None:
    events = []
    messages = ordered_batch() + [Message(payload, offset=46, partition=partition)]
    with pytest.raises(Exception):
        persist_messages(Client(events), Consumer(events), messages)
    assert events == []


def test_failed_batch_offset_commit_replays_both_tables_exactly() -> None:
    events = []
    client = Client(events)
    messages = ordered_batch()
    with pytest.raises(OSError, match="Kafka"):
        persist_messages(client, Consumer(events, fail=True), messages)
    persist_messages(client, Consumer(events), messages)
    assert client.rows[:2] == client.rows[2:]
    assert events == [
        ("insert", "mutations"), ("insert", "version_ends"), ("commit", 45),
        ("insert", "mutations"), ("insert", "version_ends"), ("commit", 45),
    ]


def test_empty_batch_performs_no_writes_or_ack() -> None:
    events = []
    persist_messages(Client(events), Consumer(events), [])
    assert events == []
