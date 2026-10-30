"""Exercise Foremen's launcher against the pinned bridge's real configuration API."""

from __future__ import annotations

import importlib.util
import itertools
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from src.bridge import Bridge
from src.serialization import serialize_version_group

from foremen import bridge as launcher


@pytest.fixture
def paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    ready = tmp_path / "ready"
    cursor = tmp_path / "cursor.json"
    ready.write_text("old process\n")
    cursor.write_text(
        json.dumps({"stream_name": "foremen", "stream_id": 7, "last_consumed_version": 100})
    )
    monkeypatch.setenv("BRIDGE_READY_FILE", str(ready))
    monkeypatch.setenv("CDC_CURSOR_FILE", str(cursor))
    monkeypatch.setenv("CDC_STREAM_NAME", "foremen")
    monkeypatch.setenv("FOREMEN_PREFIX", "foremen/")
    monkeypatch.setattr(launcher.signal, "signal", lambda *args: None)
    return ready, cursor


def test_start_failure_never_reports_ready_or_removes_cursor(
    monkeypatch: pytest.MonkeyPatch, paths: tuple[Path, Path]
) -> None:
    ready, cursor = paths
    saved_cursor = cursor.read_bytes()
    bridge = MagicMock()

    def fail_start() -> None:
        assert not ready.exists()
        raise RuntimeError("native CDC unavailable")

    bridge.start.side_effect = fail_start
    monkeypatch.setattr(launcher, "Bridge", lambda config: bridge)
    with pytest.raises(RuntimeError, match="native CDC unavailable"):
        launcher.main()
    bridge.stop.assert_called_once()
    bridge.run.assert_not_called()
    assert not ready.exists()
    assert cursor.read_bytes() == saved_cursor


def test_worker_failure_propagates_and_clears_readiness(
    monkeypatch: pytest.MonkeyPatch, paths: tuple[Path, Path]
) -> None:
    ready, cursor = paths
    saved_cursor = cursor.read_bytes()
    bridge = MagicMock()
    bridge.run.side_effect = RuntimeError("stream lost")
    monkeypatch.setattr(launcher, "Bridge", lambda config: bridge)
    with pytest.raises(RuntimeError, match="stream lost"):
        launcher.main()
    bridge.start.assert_called_once()
    bridge.stop.assert_called_once()
    assert not ready.exists()
    assert cursor.read_bytes() == saved_cursor


def test_restart_configuration_resumes_cursor_with_real_upstream_listener(
    paths: tuple[Path, Path]
) -> None:
    _, cursor_file = paths
    client = MagicMock()
    client.get_read_version.return_value = 100
    consumer = client.resume_cdc_consumer.return_value
    producer = MagicMock()
    producer.transactional = False
    config = launcher.configuration()
    bridge = Bridge(config, client=client, producer=producer)
    try:
        bridge.start()
        client.register_cdc_stream.assert_called_once_with(b"foremen", b"foremen/", b"foremen0")
        client.create_cdc_consumer.assert_not_called()
        resumed_cursor = client.resume_cdc_consumer.call_args.args[0]
        assert resumed_cursor.stream_id == 7
        assert resumed_cursor.last_consumed_version == 100
        consumer.acknowledge.assert_called_once()
        assert config.cdc.cursor_file == str(cursor_file)
        assert config.kafka.transactional_id is None
    finally:
        bridge.stop()


def test_empty_cursor_configuration_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDC_CURSOR_FILE", "")
    with pytest.raises(ValueError, match="durable checkpoints"):
        launcher.configuration()


@pytest.mark.parametrize(
    ("wire_version", "include_boundary", "expected_error"),
    [(100, True, None), (101, True, AssertionError), (100, False, TimeoutError)],
)
def test_spike_requires_exact_version_and_complete_group(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    wire_version: int,
    include_boundary: bool,
    expected_error: type[Exception] | None,
) -> None:
    script_path = Path(__file__).parents[1] / "scripts" / "spike.py"
    spec = importlib.util.spec_from_file_location("foremen_spike", script_path)
    assert spec is not None and spec.loader is not None
    spike = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(spike)
    monkeypatch.setattr(spike.fdb, "api_version", lambda version: None)
    database = MagicMock()
    database.list_cdc_streams.return_value.wait.return_value = [SimpleNamespace(name=b"foremen")]
    transaction = database.create_transaction.return_value
    transaction.get_committed_version.return_value = 100
    monkeypatch.setattr(spike.fdb, "open", lambda cluster: database)
    monkeypatch.setattr(sys, "argv", ["spike", "--wait", "1"])
    monkeypatch.setenv("CDC_STREAM_NAME", "foremen")
    monkeypatch.setenv("KAFKA_TOPIC", "fdb.cdc.foremen")
    clock = itertools.count(0, 0.01)
    monkeypatch.setattr(spike.time, "monotonic", lambda: next(clock))
    consumer = MagicMock()
    consumer.list_topics.return_value.topics = {
        "fdb.cdc.foremen": SimpleNamespace(error=None, partitions={0: object()})
    }
    monkeypatch.setattr(spike, "Consumer", lambda config: consumer)
    messages: list[MagicMock] | None = None

    def poll(timeout: float) -> MagicMock | None:
        nonlocal messages
        if messages is None:
            key, value = transaction.__setitem__.call_args.args
            records = serialize_version_group(
                [SimpleNamespace(type=0, param1=key, param2=value)],
                fdb_version=wire_version,
                max_record_bytes=1_000_000,
                stream_name="foremen",
                bridge_timestamp_ns=0,
            )
            if not include_boundary:
                records = records[:-1]
            messages = []
            for offset, record in enumerate(records):
                message = MagicMock()
                message.error.return_value = None
                message.value.return_value = record
                message.offset.return_value = offset
                messages.append(message)
        return messages.pop(0) if messages else None

    consumer.poll.side_effect = poll
    if expected_error is None:
        spike.main()
        assert "M0 PASS" in capsys.readouterr().out
    else:
        with pytest.raises(expected_error):
            spike.main()
        assert "M0 PASS" not in capsys.readouterr().out
    consumer.close.assert_called_once()
