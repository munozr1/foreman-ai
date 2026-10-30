"""Command-line checks for a completed Foremen run."""

from __future__ import annotations

import argparse
import sys

from foremen.detector import (
    UnknownRunError,
    WatermarkTimeout,
    format_finding,
    wait_for_run,
)


def main(argv: list[str] | None = None) -> int:
    """Print stale decisions, or return a failure if CDC is incomplete."""
    parser = argparse.ArgumentParser(prog="foremen")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="Check a completed agent run")
    check.add_argument("--run-id", required=True)
    check.add_argument("--wait", type=float, default=30.0, metavar="SECONDS")
    args = parser.parse_args(argv)
    if args.wait < 0:
        parser.error("--wait must be non-negative")

    from foremen.recorder import clickhouse_client

    client = clickhouse_client()
    try:
        findings = wait_for_run(client, args.run_id, timeout=args.wait)
        for finding in findings:
            print(format_finding(finding))
        print(f"{len(findings)} finding(s) for run {args.run_id}; CDC caught up.")
        return 0
    except (UnknownRunError, WatermarkTimeout) as exc:
        print(f"CHECK INCOMPLETE: {exc}", file=sys.stderr)
        return 2
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
