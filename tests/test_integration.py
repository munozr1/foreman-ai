"""Live acceptance checks. Unit doubles never count as these checks passing."""

import asyncio
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import time
from uuid import uuid4

import pytest

from demo.heartbeat import Heartbeat
from demo.scripted_race import run_race
from foremen.context import Decision, ReadObservation
from foremen.detector import findings_for_run, wait_for_run
from foremen.fdb_access import open_database, physical_key
from foremen.recorder import clickhouse_client, record_decision


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("FOREMEN_INTEGRATION") != "1",
                       reason="Run make test against the Compose stack"),
]


@pytest.fixture
def client():
    connection = clickhouse_client()
    try:
        yield connection
    finally:
        connection.close()


def wait_for_mutation(client, key: str, version: int, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = client.query(
            "SELECT count() FROM mutations WHERE key = {key:String} "
            "AND commit_version = {version:UInt64}",
            parameters={"key": key, "version": version},
        ).result_rows
        if rows[0][0]:
            return
        time.sleep(0.05)
    pytest.fail(f"Missing mutation {key} at exact client version {version} after {timeout}s")


def test_version_alignment(client):
    key = f"alignment/{uuid4().hex}"
    transaction = open_database().create_transaction()
    transaction.options.set_timeout(5000)
    transaction[physical_key(key)] = b'{"writer":"alignment"}'
    transaction.commit().wait()
    wait_for_mutation(client, key, transaction.get_committed_version())


def test_serial_control(client):
    with Heartbeat():
        result = asyncio.run(run_race(serial=True))
        assert len(result.orders) == 1
        assert wait_for_run(client, result.run_id) == []


@pytest.mark.parametrize("guarded,orders,outcome", [
    (False, 2, "committed"), (True, 1, "blocked_stale"),
])
def test_race(client, guarded, orders, outcome):
    with Heartbeat():
        result = asyncio.run(run_race(guarded=guarded))
        findings = wait_for_run(client, result.run_id)
    assert len(result.orders) == orders
    assert len(findings) == 1
    finding = findings[0]
    assert (finding.agent_id, finding.read_key, finding.missed_writer) == (
        "agent-a", "inv/sku-1", "agent-b")
    assert finding.outcome == outcome
    assert finding.read_version < finding.missed_version < finding.decision_version


def test_watermark_gates_decision(client):
    # An isolated database prevents synthetic versions corrupting the live watermark.
    database = f"watermark_test_{uuid4().hex}"
    client.command(f"CREATE DATABASE {database}")
    import clickhouse_connect

    isolated = clickhouse_connect.get_client(
        host=os.environ.get("CH_HOST", "clickhouse"),
        port=int(os.environ.get("CH_PORT", "8123")),
        username=os.environ.get("CH_USER", "foremen"),
        password=os.environ.get("CH_PASSWORD", "local-demo"), database=database,
    )
    try:
        schema = Path("docker/clickhouse/init.sql").read_text().replace("foremen.", f"{database}.")
        for statement in schema.split(";"):
            if statement.strip() and "CREATE DATABASE" not in statement:
                isolated.command(statement)
        decision = Decision("agent-a", "synthetic", "committed", 300,
                            (ReadObservation("inv/sku-1", 100, 1, {"qty": 1}),))
        record_decision(decision, isolated)
        isolated.insert("mutations", [[200, "inv/sku-1", 0,
                         '{"writer":"agent-b"}', "", 0, 0]],
                        column_names=["commit_version", "key", "op_code", "value",
                                      "range_end", "kafka_offset", "batch_index"])
        isolated.insert("version_ends", [[250, 1]],
                        column_names=["commit_version", "kafka_offset"])
        assert findings_for_run(isolated, "synthetic") == []
        isolated.insert("version_ends", [[300, 2]],
                        column_names=["commit_version", "kafka_offset"])
        assert len(findings_for_run(isolated, "synthetic")) == 1
    finally:
        isolated.close()
        client.command(f"DROP DATABASE {database} SYNC")


def test_latency_report_twenty_races(client):
    latencies = []
    with Heartbeat():
        for _ in range(20):
            committed_at = {}

            def record(decision):
                # Called immediately after the successful FDB commit, before HTTP insert.
                committed_at[decision.agent_id] = time.monotonic()
                record_decision(decision, client)

            result = asyncio.run(run_race(recorder=record))
            decision = next(item for item in result.decisions if item.agent_id == "agent-a")
            wait_for_mutation(client, "inv/sku-1", decision.decision_version)
            latencies.append(time.monotonic() - committed_at["agent-a"])
            assert len(wait_for_run(client, result.run_id)) == 1
            if len(latencies) % 5 == 0:
                print(f"Latency: {len(latencies)}/20 races complete; "
                      f"latest commit-to-row {latencies[-1]:.3f}s", flush=True)
    p50 = statistics.median(latencies)
    p95 = sorted(latencies)[math.ceil(len(latencies) * 0.95) - 1]
    report = {"measured_at": datetime.now(timezone.utc).isoformat(),
              "runs": 20, "commit_to_row_p50_seconds": p50,
              "commit_to_row_p95_seconds": p95, "samples_seconds": latencies}
    print("LATENCY " + json.dumps(report))
    assert p95 < 10, f"Commit-to-row p95 {p95:.3f}s exceeded 10s"
