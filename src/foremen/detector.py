"""Join tracked decisions to committed mutations only after CDC catches up."""

from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any


findings_sql = """
SELECT DISTINCT
    r.decision_id,
    r.agent_id,
    r.tool_name,
    r.outcome,
    r.read_key,
    r.read_version,
    m.commit_version AS missed_version,
    JSONExtractString(m.value, 'writer') AS missed_writer,
    r.decision_version,
    round((r.decision_version - r.read_version) / 1000000, 2)
        AS approx_view_age_seconds
FROM
(
    SELECT decision_id, agent_id, tool_name, outcome, decision_version,
           read_key, read_version
    FROM agent_decisions
    ARRAY JOIN read_keys AS read_key, read_versions AS read_version
    WHERE run_id = {run_id:String}
) AS r
INNER JOIN mutations AS m ON m.key = r.read_key
WHERE r.decision_version <= (SELECT max(commit_version) FROM version_ends)
  AND m.op_code = 0
  AND m.commit_version > r.read_version
  AND m.commit_version < r.decision_version
  AND JSONExtractString(m.value, 'writer') != ''
  AND JSONExtractString(m.value, 'writer') != r.agent_id
ORDER BY r.decision_version, missed_version, r.read_key
"""

status_sql = """
SELECT count(), max(decision_version),
       (SELECT max(commit_version) FROM version_ends)
FROM agent_decisions
WHERE run_id = {run_id:String}
"""


@dataclass(frozen=True)
class Finding:
    """Evidence of a decision made from a superseded read."""

    decision_id: str
    agent_id: str
    tool_name: str
    outcome: str
    read_key: str
    read_version: int
    missed_version: int
    missed_writer: str
    decision_version: int
    approx_view_age_seconds: float


@dataclass(frozen=True)
class WatermarkStatus:
    """Current decision and stream progress for a completed run."""

    decision_count: int
    decision_version: int
    watermark: int

    @property
    def ready(self) -> bool:
        return self.decision_count > 0 and self.watermark >= self.decision_version


class WatermarkTimeout(TimeoutError):
    """CDC did not cover all decisions before the requested deadline."""


class UnknownRunError(ValueError):
    """No decisions were found for the requested run."""


def _validate_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")


def watermark_status(client: Any, run_id: str) -> WatermarkStatus:
    """Read the run's latest decision version and the persisted CDC watermark."""
    _validate_run_id(run_id)
    result = client.query(status_sql, parameters={"run_id": run_id})
    count, decision_version, watermark = result.result_rows[0]
    return WatermarkStatus(int(count), int(decision_version), int(watermark))


def findings_for_run(client: Any, run_id: str) -> list[Finding]:
    """Return available evidence; decisions above the watermark are excluded."""
    _validate_run_id(run_id)
    result = client.query(findings_sql, parameters={"run_id": run_id})
    return [Finding(*row) for row in result.result_rows]


def wait_for_run(
    client: Any, run_id: str, timeout: float = 30.0, poll_interval: float = 0.1
) -> list[Finding]:
    """Wait for a finished run's decisions, failing visibly on incomplete CDC.

    Call this after the run's decision writers finish. It cannot determine
    whether an agent will record another decision in the future.
    """
    _validate_run_id(run_id)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be finite and non-negative")
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("poll_interval must be finite and positive")
    deadline = time.monotonic() + timeout
    while True:
        status = watermark_status(client, run_id)
        if status.ready:
            return findings_for_run(client, run_id)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if status.decision_count == 0:
                raise UnknownRunError(f"No decisions found for run {run_id!r}.")
            raise WatermarkTimeout(
                f"CDC incomplete for run {run_id!r}: watermark "
                f"{status.watermark} is below decision version "
                f"{status.decision_version}; no clean verdict is available."
            )
        time.sleep(min(poll_interval, remaining))


def format_finding(finding: Finding) -> str:
    """Format CLI evidence without implying the version gap is a clock."""
    return (
        f"STALE DECISION  {finding.agent_id}  {finding.tool_name}  "
        f"({finding.outcome})\n"
        f"  read     {finding.read_key}  at version {finding.read_version}\n"
        f"  missed   a write by {finding.missed_writer} "
        f"at version {finding.missed_version}\n"
        f"  decided  at version {finding.decision_version}\n"
        f"  the view was about {finding.approx_view_age_seconds:.2f} s old "
        "when the decision was made"
    )
