"""Run the pinned, unmodified Streamline bridge with environment configuration."""

from __future__ import annotations

import logging
import os
import signal
import threading
from pathlib import Path

from src.bridge import Bridge, BridgeConfig, from_dict
from src.cdc.listener import strinc


def configuration() -> BridgeConfig:
    """Configure the upstream API with a raw prefix and durable cursor path."""
    prefix = os.environ.get("FOREMEN_PREFIX", "foremen/").encode()
    if not prefix:
        raise ValueError("FOREMEN_PREFIX must not be empty")
    cursor_file = os.environ.get("CDC_CURSOR_FILE", "/state/cursor.json")
    if not cursor_file:
        raise ValueError("CDC_CURSOR_FILE must not be empty; durable checkpoints are required")
    return from_dict(
        {
            "fdb": {
                "api_version": 800,
                "cluster_file": os.environ.get(
                    "FDB_CLUSTER_FILE", "/etc/foundationdb/fdb.cluster"
                ),
            },
            "cdc": {
                "stream_name": os.environ.get("CDC_STREAM_NAME", "foremen"),
                "begin_key": prefix,
                "end_key": strinc(prefix),
                "cursor_file": cursor_file,
                "max_record_bytes": 1_000_000,
            },
            "kafka": {
                "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                "topic": os.environ.get("KAFKA_TOPIC", "fdb.cdc.foremen"),
                "transactional_id": None,
            },
            "pipeline": {"shutdown_timeout_seconds": 5.0},
        }
    )


def main() -> None:
    """Start one durable CDC consumer and stop it cleanly on container shutdown."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ready_path = Path(os.environ.get("BRIDGE_READY_FILE", "/tmp/foremen-bridge-ready"))
    ready_path.unlink(missing_ok=True)
    config = configuration()
    bridge = Bridge(config)
    shutdown = threading.Event()
    failures: list[BaseException] = []

    def request_shutdown(signum: int, frame: object) -> None:
        shutdown.set()

    def run_bridge() -> None:
        try:
            bridge.run()
        except BaseException as exc:
            failures.append(exc)
        finally:
            shutdown.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    worker: threading.Thread | None = None
    try:
        bridge.start()
        worker = threading.Thread(target=run_bridge, name="streamline", daemon=True)
        worker.start()
        ready_path.write_text(f"{os.getpid()}\n")
        logging.info("Bridge registered stream %s", config.cdc.stream_name)
        shutdown.wait()
    finally:
        ready_path.unlink(missing_ok=True)
        bridge.stop()
        if worker is not None:
            worker.join(timeout=15)
            if worker.is_alive():
                raise RuntimeError("Bridge worker did not stop within its shutdown deadline")
    if failures:
        raise failures[0]


if __name__ == "__main__":
    main()
