"""The same stock tools serve scripted agents and SDK agents."""

import asyncio
import time
from collections.abc import Callable
from typing import Any

from foremen.context import Decision, ForemenContext
from foremen.fdb_access import guarded_write, open_database, tracked_read, unguarded_write


async def advance_after(version: int, db: Any = None, timeout: float = 10.0) -> int:
    """Make the demo's strict version inequality explicit before releasing A."""
    db = db if db is not None else open_database()

    def read_version() -> int:
        transaction = db.create_transaction()
        transaction.options.set_timeout(5000)
        try:
            return transaction.get_read_version().wait()
        finally:
            transaction.cancel()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = await asyncio.to_thread(read_version)
        if current > version:
            return current
        await asyncio.sleep(0.01)
    raise TimeoutError(f"FDB read version did not advance beyond {version}")


async def wait_for_first_read(
    agent_task: asyncio.Task[None], read_ready: asyncio.Event, timeout: float,
) -> None:
    """Surface an early agent/tool failure instead of waiting on a dead read gate."""
    readiness = asyncio.create_task(read_ready.wait())
    try:
        completed, _ = await asyncio.wait(
            [agent_task, readiness], timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not completed:
            raise TimeoutError("Agent A did not check stock before the demo deadline")
        if agent_task in completed:
            await agent_task
            raise RuntimeError("Agent A finished without waiting at the stock-read gate")
    finally:
        readiness.cancel()
        await asyncio.gather(readiness, return_exceptions=True)


async def check_stock(
    ctx: ForemenContext, sku: str = "sku-1", *, db: Any = None,
    read_ready: asyncio.Event | None = None,
    release_read: asyncio.Event | None = None,
) -> dict[str, Any]:
    """Pause after A's observation, before it receives the tool's result."""
    stock = await asyncio.to_thread(tracked_read, ctx, f"inv/{sku}", db)
    if read_ready is not None:
        read_ready.set()
    if release_read is not None:
        await asyncio.wait_for(release_read.wait(), timeout=120)
    return stock


async def reserve_stock(
    ctx: ForemenContext, sku: str = "sku-1", qty: int = 1, *,
    guarded: bool = False, db: Any = None,
    recorder: Callable[[Decision], None] | None = None,
) -> Decision:
    """Make exactly one reservation attempt from the observed inventory."""
    write = guarded_write if guarded else unguarded_write
    return await asyncio.to_thread(write, ctx, sku, qty, db, recorder)
