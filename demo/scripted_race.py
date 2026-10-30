"""Deterministic ordering: A reads, B reserves, then A attempts to reserve."""

import argparse
import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from demo.agents import advance_after, check_stock, reserve_stock, wait_for_first_read
from demo.heartbeat import Heartbeat
from foremen.context import Decision, ForemenContext
from foremen.fdb_access import StaleStateError, confirmed_orders, open_database, seed_inventory
from foremen.recorder import clickhouse_client, record_decision


@dataclass
class RaceResult:
    run_id: str
    guarded: bool
    serial: bool
    decisions: list[Decision]
    orders: list[dict[str, Any]]
    contexts: list[ForemenContext]


async def run_race(
    guarded: bool = False, serial: bool = False, run_id: str | None = None,
    db: Any = None, recorder: Callable[[Decision], None] | None = None,
) -> RaceResult:
    """Run shared tools without an LLM; all integration assertions use this path."""
    run_id = run_id if run_id is not None else uuid4().hex
    db = db if db is not None else open_database()
    persist = recorder if recorder is not None else record_decision
    await asyncio.to_thread(seed_inventory, db)
    decisions: list[Decision] = []
    agent_a = ForemenContext("agent-a", run_id)
    agent_b = ForemenContext("agent-b", run_id)
    read_ready = asyncio.Event()
    release_read = asyncio.Event()

    def record(decision: Decision) -> None:
        persist(decision)
        decisions.append(decision)

    async def act(ctx: ForemenContext, pause: bool = False) -> None:
        stock = await check_stock(ctx, db=db,
                                  read_ready=read_ready if pause else None,
                                  release_read=release_read if pause else None)
        if stock["qty"] >= 1:
            try:
                await reserve_stock(ctx, guarded=guarded, db=db, recorder=record)
            except StaleStateError:
                pass

    if serial:
        await act(agent_a)
        await act(agent_b)
    else:
        task_a = asyncio.create_task(act(agent_a, pause=True))
        try:
            await wait_for_first_read(task_a, read_ready, timeout=10)
            await act(agent_b)
            # A blocked attempt uses a read version; ensure it is strictly after B.
            committed = [decision.decision_version for decision in decisions
                         if decision.agent_id == "agent-b" and decision.outcome == "committed"]
            if committed:
                await advance_after(max(committed), db)
            release_read.set()
            await task_a
        finally:
            release_read.set()
            if not task_a.done():
                task_a.cancel()
            await asyncio.gather(task_a, return_exceptions=True)
    orders = await asyncio.to_thread(confirmed_orders, run_id, db)
    return RaceResult(run_id, guarded, serial, decisions, orders, [agent_a, agent_b])


async def run_demo(guarded: bool = False, serial: bool = False) -> RaceResult:
    """Keep heartbeats running until the detector has caught up."""
    from foremen.detector import format_finding, wait_for_run

    client = clickhouse_client()
    try:
        with Heartbeat():
            result = await run_race(guarded=guarded, serial=serial)
            print(f"Run {result.run_id}: {'guarded' if guarded else 'unguarded'}"
                  f" {'serial' if serial else 'race'}")
            print(f"Orders confirmed: {len(result.orders)} for 1 unit of stock")
            for decision in result.decisions:
                print(f"  {decision.agent_id}: {decision.outcome} at {decision.decision_version}")
            findings = await asyncio.to_thread(wait_for_run, client, result.run_id, 30.0)
            for finding in findings:
                print(format_finding(finding))
            if not findings:
                print("No stale decisions")
            return result
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guarded", action="store_true")
    parser.add_argument("--serial", action="store_true")
    args = parser.parse_args()
    asyncio.run(run_demo(args.guarded, args.serial))


if __name__ == "__main__":
    main()
