"""Use SDK spans only inside an active, enabled SDK trace."""

from contextlib import nullcontext
from typing import Any


def current_trace_id() -> str:
    """Return an empty ID when tracing or the optional SDK is unavailable."""
    try:
        from agents import get_current_trace
    except ImportError:
        return ""
    trace = get_current_trace()
    if trace is None or trace.trace_id == "no-op":
        return ""
    return trace.trace_id


def read_span(data: dict[str, Any]) -> Any:
    """Share the span's metadata dictionary so version fields can be added."""
    if not current_trace_id():
        return nullcontext()
    from agents import custom_span

    return custom_span("foremen.read", data=data)
