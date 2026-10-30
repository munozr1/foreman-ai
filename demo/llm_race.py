"""Optional real Agents SDK demo. Output is intentionally not asserted on."""

import argparse
import asyncio
import json
import os
from uuid import uuid4

from agents import Agent, ModelSettings, RunConfig, RunContextWrapper, Runner, function_tool

from demo.agents import check_stock as shared_check_stock
from demo.agents import reserve_stock as shared_reserve_stock
from demo.agents import advance_after, wait_for_first_read
from demo.heartbeat import Heartbeat
from foremen.context import ForemenContext
from foremen.detector import format_finding, wait_for_run
from foremen.fdb_access import StaleStateError, confirmed_orders, seed_inventory
from foremen.recorder import clickhouse_client


async def run_llm_race(guarded: bool = False) -> str:
    """Coordinate two real agents through the same read gate as scripted mode."""
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required only for the LLM demo")
    run_id = uuid4().hex
    read_ready = asyncio.Event()
    release_read = asyncio.Event()
    b_commits: list[int] = []
    await asyncio.to_thread(seed_inventory)

    @function_tool
    async def check_stock(ctx: RunContextWrapper[ForemenContext], sku: str) -> str:
        """Check available quantity and revision for an inventory SKU."""
        is_a = ctx.context.agent_id == "agent-a"
        value = await shared_check_stock(
            ctx.context, sku, read_ready=read_ready if is_a else None,
            release_read=release_read if is_a else None,
        )
        return json.dumps(value)

    @function_tool
    async def reserve_stock(ctx: RunContextWrapper[ForemenContext], sku: str, qty: int) -> str:
        """Reserve stock using the most recent check; report any stale-state rejection."""
        try:
            decision = await shared_reserve_stock(ctx.context, sku, qty, guarded=guarded)
        except StaleStateError as error:
            return f"blocked_stale: {error}"
        if ctx.context.agent_id == "agent-b":
            b_commits.append(decision.decision_version)
        return f"confirmed at version {decision.decision_version}"

    instructions = (
        "You reserve inventory. First call check_stock for sku-1 exactly once. "
        "If the returned quantity is at least 1, call reserve_stock for sku-1 and qty 1 "
        "exactly once. Then report the result. If blocked_stale is returned, report "
        "the rejection and stop; do not retry in this demonstration."
    )
    config = RunConfig(tracing_disabled=os.environ.get("FOREMEN_TRACING", "0") != "1",
                       workflow_name="Foremen inventory race", group_id=run_id)

    async def act(agent_id: str) -> None:
        agent = Agent[ForemenContext](
            name=agent_id, instructions=instructions,
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14"),
            model_settings=ModelSettings(temperature=0, parallel_tool_calls=False),
            tools=[check_stock, reserve_stock],
        )
        result = await Runner.run(agent, "Please reserve one unit of sku-1.",
                                  context=ForemenContext(agent_id, run_id),
                                  max_turns=6, run_config=config)
        print(f"{agent_id}: {result.final_output}")

    client = clickhouse_client()
    try:
        with Heartbeat():
            task_a = asyncio.create_task(act("agent-a"))
            try:
                await wait_for_first_read(task_a, read_ready, timeout=90)
                await asyncio.wait_for(act("agent-b"), timeout=90)
                if b_commits:
                    await advance_after(max(b_commits))
                release_read.set()
                await asyncio.wait_for(task_a, timeout=90)
            finally:
                release_read.set()
                if not task_a.done():
                    task_a.cancel()
                await asyncio.gather(task_a, return_exceptions=True)
            orders = await asyncio.to_thread(confirmed_orders, run_id)
            print(f"Run {run_id}: {len(orders)} orders confirmed for 1 unit of stock")
            findings = await asyncio.to_thread(wait_for_run, client, run_id, 30.0)
            for finding in findings:
                print(format_finding(finding))
            if not findings:
                print("No stale decisions: inspect the agents' tool calls for this run")
    finally:
        client.close()
    return run_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guarded", action="store_true")
    args = parser.parse_args()
    asyncio.run(run_llm_race(args.guarded))


if __name__ == "__main__":
    main()
