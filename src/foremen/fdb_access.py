"""Short FDB transactions with observations carried across agent thinking time."""

import json
import os
from collections.abc import Callable
from functools import lru_cache
from typing import Any
from uuid import uuid4

from foremen.context import Decision, ForemenContext, ReadObservation
from foremen.recorder import record_decision
from foremen.tracing import current_trace_id, read_span


class StaleStateError(RuntimeError):
    """A reservation was rejected because its observed revision changed."""

    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        super().__init__("Stock changed since you checked; check again before reserving")


class CommitOutcomeUnknownError(RuntimeError):
    """The server may have committed; callers must not blindly retry the order."""

    def __init__(self, order_key: str) -> None:
        self.order_key = order_key
        super().__init__(
            f"Reservation commit outcome is unknown for {order_key}. "
            "Inspect that order before retrying; no decision was recorded."
        )


@lru_cache(maxsize=1)
def fdb_module() -> Any:
    import fdb

    fdb.api_version(800)
    return fdb


@lru_cache(maxsize=1)
def open_database() -> Any:
    """Open the shared database using the same API as the pinned bridge."""
    database = fdb_module().open(os.environ.get("FDB_CLUSTER_FILE"))
    database.options.set_transaction_timeout(5000)
    database.options.set_transaction_retry_limit(10)
    return database


def physical_key(key: str) -> bytes:
    """The bridge and sink remove exactly this configured watch prefix."""
    return os.environ.get("FOREMEN_PREFIX", "foremen/").encode() + key.encode()


def _transaction(db: Any) -> Any:
    transaction = db.create_transaction()
    transaction.options.set_timeout(5000)
    transaction.options.set_retry_limit(10)
    return transaction


def _decode(value: Any) -> dict[str, Any]:
    # FDB's lazy value wait() returns itself; .value resolves bytes or None.
    raw = value.value
    if raw is None:
        raise KeyError("Inventory key does not exist; run the seed step first")
    return json.loads(raw)


def tracked_read(ctx: ForemenContext, key: str, db: Any = None) -> dict[str, Any]:
    """Record the transaction version and revision alongside a point read."""
    db = db if db is not None else open_database()
    transaction = _transaction(db)
    data: dict[str, Any] = {"key": key}
    try:
        with read_span(data):
            while True:
                try:
                    read_version = transaction.get_read_version().wait()
                    value = _decode(transaction[physical_key(key)])
                    observation = ReadObservation(key, read_version, int(value["rev"]), value.copy())
                    ctx.reads.append(observation)
                    ctx.trace_id = current_trace_id()
                    data.update(read_version=read_version, rev=observation.rev)
                    return value
                except fdb_module().FDBError as error:
                    transaction.on_error(error.code).wait()
    finally:
        transaction.cancel()


def _reserve(
    ctx: ForemenContext, sku: str, qty: int, guarded: bool, db: Any,
    recorder: Callable[[Decision], None],
) -> Decision:
    if type(qty) is not int or qty <= 0:
        raise ValueError("Reservation quantity must be a positive integer")
    key = f"inv/{sku}"
    observed = ctx.latest_read(key)
    if observed.value["qty"] < qty:
        raise ValueError("Insufficient stock in the observed inventory")
    transaction = _transaction(db)
    order_id = f"{ctx.run_id}/{ctx.agent_id}/{uuid4().hex}"
    while True:
        try:
            attempt_version = transaction.get_read_version().wait()
            raw = transaction[physical_key(key)].value
            current = json.loads(raw) if raw is not None else None
            if guarded and (current is None or current["rev"] != observed.rev):
                transaction.cancel()
                decision = Decision(ctx.agent_id, ctx.run_id, "blocked_stale",
                                    attempt_version, tuple(ctx.reads), current_trace_id())
                recorder(decision)
                raise StaleStateError(decision)
            if current is None:
                raise KeyError(key)
            # Only the revision bookkeeping uses current data. The unguarded quantity
            # deliberately comes from the agent's earlier, potentially stale observation.
            available = current["qty"] if guarded else observed.value["qty"]
            if available < qty:
                raise ValueError("Insufficient stock")
            inventory = {"qty": available - qty, "rev": current["rev"] + 1,
                         "writer": ctx.agent_id}
            order = {"sku": sku, "qty": qty, "agent": ctx.agent_id,
                     "status": "confirmed", "run_id": ctx.run_id}
            transaction[physical_key(key)] = json.dumps(inventory).encode()
            transaction[physical_key(f"order/{order_id}")] = json.dumps(order).encode()
            transaction.commit().wait()
            commit_version = transaction.get_committed_version()
            break
        except fdb_module().FDBError as error:
            # Retrying an ambiguous commit could treat our own successful write as
            # a competing revision, or decrement stock twice on the unguarded path.
            if error.code == 1021:
                raise CommitOutcomeUnknownError(f"order/{order_id}") from error
            transaction.on_error(error.code).wait()
    decision = Decision(ctx.agent_id, ctx.run_id, "committed", commit_version,
                        tuple(ctx.reads), current_trace_id())
    recorder(decision)
    return decision


def unguarded_write(
    ctx: ForemenContext, sku: str = "sku-1", qty: int = 1, db: Any = None,
    recorder: Callable[[Decision], None] | None = None,
) -> Decision:
    """Reserve using the prior quantity without validating the observed revision."""
    return _reserve(ctx, sku, qty, False, db if db is not None else open_database(),
                    recorder if recorder is not None else record_decision)


def guarded_write(
    ctx: ForemenContext, sku: str = "sku-1", qty: int = 1, db: Any = None,
    recorder: Callable[[Decision], None] | None = None,
) -> Decision:
    """Recheck the revision on every retry of the short write transaction."""
    return _reserve(ctx, sku, qty, True, db if db is not None else open_database(),
                    recorder if recorder is not None else record_decision)


def seed_inventory(db: Any = None, sku: str = "sku-1") -> int:
    """Reset quantity for a new demo while retaining monotonic revisions."""
    db = db if db is not None else open_database()
    transaction = _transaction(db)
    key = physical_key(f"inv/{sku}")
    while True:
        try:
            raw = transaction[key].value
            revision = json.loads(raw)["rev"] + 1 if raw is not None else 1
            transaction[key] = json.dumps({"qty": 1, "rev": revision, "writer": "seed"}).encode()
            transaction.commit().wait()
            return transaction.get_committed_version()
        except fdb_module().FDBError as error:
            transaction.on_error(error.code).wait()


def confirmed_orders(run_id: str, db: Any = None) -> list[dict[str, Any]]:
    """Count actual orders, because the lost update leaves stock at zero."""
    db = db if db is not None else open_database()
    prefix = physical_key(f"order/{run_id}/")
    return [json.loads(item.value) for item in db.get_range_startswith(prefix)
            if json.loads(item.value).get("status") == "confirmed"]
