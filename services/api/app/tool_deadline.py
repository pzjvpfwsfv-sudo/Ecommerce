from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import math
from time import perf_counter


class ToolDeadlineExceededError(TimeoutError):
    """Raised before starting transport work after the request deadline."""


@dataclass(frozen=True)
class _Deadline:
    expires_at: float
    timer: Callable[[], float]


_CURRENT_DEADLINE: ContextVar[_Deadline | None] = ContextVar(
    "tool_execution_deadline",
    default=None,
)


@contextmanager
def use_tool_deadline(
    timeout_seconds: float,
    timer: Callable[[], float] = perf_counter,
) -> Iterator[None]:
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ToolDeadlineExceededError("tool deadline exceeded")
    deadline = _Deadline(expires_at=timer() + timeout_seconds, timer=timer)
    token = _CURRENT_DEADLINE.set(deadline)
    try:
        yield
    finally:
        _CURRENT_DEADLINE.reset(token)


def remaining_timeout(per_request_cap_seconds: float) -> float:
    deadline = _CURRENT_DEADLINE.get()
    if deadline is None:
        return per_request_cap_seconds
    remaining = deadline.expires_at - deadline.timer()
    if remaining <= 0:
        raise ToolDeadlineExceededError("tool deadline exceeded")
    return min(per_request_cap_seconds, remaining)
