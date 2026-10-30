"""Decision persistence is independent of optional SDK tracing."""

import os
from typing import Any

from foremen.context import Decision


def clickhouse_client() -> Any:
    """Create an HTTP client using the local stack's environment settings."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CH_HOST", "clickhouse"),
        port=int(os.environ.get("CH_PORT", "8123")),
        username=os.environ.get("CH_USER", "foremen"),
        password=os.environ.get("CH_PASSWORD", "local-demo"),
        database=os.environ.get("CH_DATABASE", "foremen"),
        connect_timeout=5,
        send_receive_timeout=15,
    )


def record_decision(decision: Decision, client: Any = None) -> None:
    """Persist a completed attempt even when tracing is disabled."""
    owns_client = client is None
    client = client if client is not None else clickhouse_client()
    try:
        client.insert(
            "agent_decisions",
            [[decision.decision_id, decision.run_id, decision.trace_id,
              decision.agent_id, decision.tool_name, decision.outcome,
              decision.decision_version, decision.decided_at,
              decision.read_keys, decision.read_versions]],
            column_names=["decision_id", "run_id", "trace_id", "agent_id", "tool_name",
                          "outcome", "decision_version", "decided_at", "read_keys",
                          "read_versions"],
        )
    finally:
        if owns_client:
            client.close()
