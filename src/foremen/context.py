"""Per-agent observations kept outside the model's prompt."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4


@dataclass(frozen=True)
class ReadObservation:
    key: str
    read_version: int
    rev: int
    value: dict[str, Any]


@dataclass
class ForemenContext:
    agent_id: str
    run_id: str
    reads: list[ReadObservation] = field(default_factory=list)
    trace_id: str = ""

    def latest_read(self, key: str) -> ReadObservation:
        """Return the latest observation used to make this decision."""
        for observation in reversed(self.reads):
            if observation.key == key:
                return observation
        raise ValueError(f"Read {key} with check_stock before reserving it")


@dataclass(frozen=True)
class Decision:
    agent_id: str
    run_id: str
    outcome: str
    decision_version: int
    reads: tuple[ReadObservation, ...]
    trace_id: str = ""
    tool_name: str = "reserve_stock"
    decision_id: UUID = field(default_factory=uuid4)
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def read_keys(self) -> list[str]:
        return [read.key for read in self.reads]

    @property
    def read_versions(self) -> list[int]:
        return [read.read_version for read in self.reads]
