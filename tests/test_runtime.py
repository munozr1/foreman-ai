"""Local algorithm checks; these do not replace the real-stack acceptance suite."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from demo.scripted_race import run_race
from demo.agents import advance_after, wait_for_first_read
from foremen.context import ForemenContext
from foremen import fdb_access
from foremen.fdb_access import (
    CommitOutcomeUnknownError, StaleStateError, guarded_write, seed_inventory,
    tracked_read, unguarded_write,
)
from foremen.recorder import record_decision


class FakeFDBError(Exception):
    code = 1020


class Future:
    def __init__(self, value=None):
        self.value = value

    def wait(self):
        return self.value


class ValueFuture(Future):
    """Match native FDB lazy values, whose wait() returns the future itself."""

    def wait(self):
        return self


class Options:
    def set_timeout(self, value):
        pass

    def set_retry_limit(self, value):
        pass


class Database:
    def __init__(self):
        self.values = {}
        self.version = 1_000_000
        self.before_commit = None
        self.retries = 0

    def create_transaction(self):
        return Transaction(self)

    def get_range_startswith(self, prefix):
        return [SimpleNamespace(key=key, value=value)
                for key, value in self.values.items() if key.startswith(prefix)]


class Transaction:
    def __init__(self, db):
        self.db = db
        self.options = Options()
        self.pending = {}
        self.committed = None

    def get_read_version(self):
        return Future(self.db.version + 1)

    def __getitem__(self, key):
        return ValueFuture(self.db.values.get(key))

    def __setitem__(self, key, value):
        self.pending[key] = value

    def commit(self):
        if self.db.before_commit is not None:
            hook = self.db.before_commit
            self.db.before_commit = None
            hook(self.db)
        self.db.values.update(self.pending)
        self.db.version += 1_000_000
        self.committed = self.db.version
        return Future()

    def get_committed_version(self):
        assert self.committed is not None, "Version must be read after committing"
        return self.committed

    def cancel(self):
        self.pending.clear()

    def on_error(self, code):
        self.pending.clear()
        self.db.retries += 1
        return Future()


@pytest.fixture
def database(monkeypatch):
    monkeypatch.setattr(fdb_access, "fdb_module", lambda: SimpleNamespace(FDBError=FakeFDBError))
    monkeypatch.setattr(fdb_access, "current_trace_id", lambda: "")
    monkeypatch.setattr(fdb_access, "read_span", lambda data: __import__("contextlib").nullcontext())
    return Database()


def test_unguarded_race_confirms_two_orders_and_hides_oversell(database):
    result = asyncio.run(run_race(db=database, recorder=lambda decision: None))
    assert len(result.orders) == 2
    assert [decision.agent_id for decision in result.decisions] == ["agent-b", "agent-a"]
    inventory = json.loads(database.values[b"foremen/inv/sku-1"])
    assert inventory == {"qty": 0, "rev": 3, "writer": "agent-a"}
    agent_b, agent_a = result.decisions
    assert agent_a.read_versions[0] < agent_b.decision_version < agent_a.decision_version


def test_guarded_race_logs_block_and_confirms_one_order(database):
    result = asyncio.run(run_race(guarded=True, db=database, recorder=lambda decision: None))
    assert len(result.orders) == 1
    assert [(decision.agent_id, decision.outcome) for decision in result.decisions] == [
        ("agent-b", "committed"), ("agent-a", "blocked_stale")]
    assert result.decisions[1].decision_version > result.decisions[0].decision_version
    assert json.loads(database.values[b"foremen/inv/sku-1"])["writer"] == "agent-b"


def test_serial_control_does_not_attempt_second_reservation(database):
    result = asyncio.run(run_race(serial=True, db=database, recorder=lambda decision: None))
    assert len(result.orders) == 1
    assert len(result.decisions) == 1
    assert result.contexts[1].reads[0].value["qty"] == 0


def test_version_barrier_rejects_equal_version_at_deadline(database):
    with pytest.raises(TimeoutError, match="did not advance"):
        asyncio.run(advance_after(database.version + 1, database, timeout=0.02))


def test_read_gate_surfaces_agent_failure_without_waiting_for_timeout():
    async def scenario():
        async def failed_agent():
            raise ValueError("tool failed")

        task = asyncio.create_task(failed_agent())
        await wait_for_first_read(task, asyncio.Event(), timeout=10)

    with pytest.raises(ValueError, match="tool failed"):
        asyncio.run(scenario())


def test_guard_rechecks_revision_after_transaction_conflict(database):
    seed_inventory(database)
    context = ForemenContext("agent-a", "retry-test")
    tracked_read(context, "inv/sku-1", database)

    def concurrent_commit(db):
        db.values[b"foremen/inv/sku-1"] = json.dumps(
            {"qty": 0, "rev": 2, "writer": "agent-b"}).encode()
        db.version += 1_000_000
        raise FakeFDBError()

    database.before_commit = concurrent_commit
    recorded = []
    with pytest.raises(StaleStateError):
        guarded_write(context, db=database, recorder=recorded.append)
    assert database.retries == 1
    assert len(recorded) == 1
    assert recorded[0].outcome == "blocked_stale"
    assert not list(database.get_range_startswith(b"foremen/order/"))


def test_guard_does_not_reuse_old_fdb_read_version(database):
    seed_inventory(database)
    context = ForemenContext("agent-a", "old-read")
    tracked_read(context, "inv/sku-1", database)
    database.version += 60_000_000
    decision = guarded_write(context, db=database, recorder=lambda decision: None)
    assert decision.outcome == "committed"
    assert decision.decision_version - decision.read_versions[0] > 5_000_000


@pytest.mark.parametrize("write", [guarded_write, unguarded_write])
def test_unknown_commit_does_not_retry_or_claim_a_block(database, monkeypatch, write):
    seed_inventory(database)
    context = ForemenContext("agent-a", "ambiguous")
    tracked_read(context, "inv/sku-1", database)
    original_commit = Transaction.commit

    def ambiguous_commit(transaction):
        original_commit(transaction)
        error = FakeFDBError()
        error.code = 1021
        raise error

    monkeypatch.setattr(Transaction, "commit", ambiguous_commit)
    recorded = []
    with pytest.raises(CommitOutcomeUnknownError) as failure:
        write(context, db=database, recorder=recorded.append)
    assert failure.value.order_key.startswith("order/ambiguous/agent-a/")
    assert len(database.get_range_startswith(b"foremen/order/")) == 1
    assert database.retries == 0
    assert recorded == []


def test_missing_inventory_is_stale_for_guard(database):
    seed_inventory(database)
    context = ForemenContext("agent-a", "deleted")
    tracked_read(context, "inv/sku-1", database)
    del database.values[b"foremen/inv/sku-1"]
    with pytest.raises(StaleStateError):
        guarded_write(context, db=database, recorder=lambda decision: None)


def test_tracked_read_of_missing_native_lazy_value(database):
    context = ForemenContext("agent-a", "missing-read")
    with pytest.raises(KeyError, match="Inventory key does not exist"):
        tracked_read(context, "inv/sku-1", database)
    assert context.reads == []


def test_decision_persists_without_trace(database):
    result = asyncio.run(run_race(guarded=True, db=database, recorder=lambda decision: None))
    inserts = []
    client = SimpleNamespace(insert=lambda table, data, column_names: inserts.append((table, data, column_names)))
    record_decision(result.decisions[1], client)
    table, rows, columns = inserts[0]
    persisted = dict(zip(columns, rows[0]))
    assert table == "agent_decisions"
    assert persisted["outcome"] == "blocked_stale"
    assert persisted["trace_id"] == ""
    assert persisted["read_keys"] == ["inv/sku-1"]


@pytest.mark.parametrize("quantity", [0, -1, True, 1.5])
def test_invalid_reservation_quantity_is_rejected(database, quantity):
    with pytest.raises(ValueError, match="positive integer"):
        guarded_write(ForemenContext("agent-a", "invalid"), qty=quantity, db=database)
