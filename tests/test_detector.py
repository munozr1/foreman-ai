"""Watermark failures must never turn into a clean verdict."""

from types import SimpleNamespace

import pytest

from foremen.detector import (
    UnknownRunError,
    WatermarkStatus,
    WatermarkTimeout,
    format_finding,
    wait_for_run,
)


class ScriptedClient:
    def __init__(self, statuses: list[tuple[int, int, int]]) -> None:
        self.statuses = iter(statuses)
        self.findings_queries = 0
        self.run_ids = []

    def query(self, sql: str, parameters: dict) -> SimpleNamespace:
        self.run_ids.append(parameters["run_id"])
        if "SELECT count()" in sql:
            return SimpleNamespace(result_rows=[next(self.statuses)])
        self.findings_queries += 1
        return SimpleNamespace(result_rows=[(
            "decision-1", "agent-a", "reserve_stock", "committed", "inv/sku-1",
            1000000, 2000000, "agent-b", 3000000, 2.0,
        )])


def test_timeout_cannot_report_a_clean_run() -> None:
    client = ScriptedClient([(1, 300, 299)])
    with pytest.raises(WatermarkTimeout, match="no clean verdict"):
        wait_for_run(client, "run-1", timeout=0)
    assert client.findings_queries == 0


def test_unknown_run_cannot_report_a_clean_run() -> None:
    client = ScriptedClient([(0, 0, 1000)])
    with pytest.raises(UnknownRunError):
        wait_for_run(client, "typo", timeout=0)
    assert client.findings_queries == 0


def test_wait_queries_evidence_only_after_watermark_catches_up() -> None:
    client = ScriptedClient([(2, 300, 100), (2, 300, 299), (2, 300, 300)])
    findings = wait_for_run(client, "run-'quoted", timeout=1, poll_interval=0.001)
    assert len(findings) == 1
    assert client.findings_queries == 1
    assert client.run_ids == ["run-'quoted"] * 4
    text = format_finding(findings[0])
    assert "agent-a" in text and "agent-b" in text
    assert "inv/sku-1" in text and "2.00 s" in text


def test_empty_decisions_are_never_ready() -> None:
    assert not WatermarkStatus(0, 0, 100).ready
    assert WatermarkStatus(1, 100, 100).ready


@pytest.mark.parametrize("run_id", ["", " "])
def test_missing_run_id_rejected_before_query(run_id: str) -> None:
    with pytest.raises(ValueError, match="run_id"):
        wait_for_run(None, run_id)
