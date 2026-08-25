from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextvars import copy_context
from threading import BoundedSemaphore, Lock
from typing import TypeVar


Result = TypeVar("Result")


class ToolRunnerUnavailableError(RuntimeError):
    """Raised when a bounded tool runner cannot safely accept a request."""


class BoundedToolRunner:
    def __init__(self, max_workers: int) -> None:
        if type(max_workers) is not int or not 1 <= max_workers <= 3:
            raise ValueError("tool runner workers must be between 1 and 3")
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tool-worker")
        self._capacity = BoundedSemaphore(max_workers)
        self._lock = Lock()
        self._closed = False

    def run(self, operation: Callable[[], Result], timeout_seconds: float) -> Result:
        if not self._capacity.acquire(blocking=False):
            raise ToolRunnerUnavailableError("tool runner unavailable")
        with self._lock:
            if self._closed:
                self._capacity.release()
                raise ToolRunnerUnavailableError("tool runner unavailable")
            try:
                parent_context = copy_context()
                future = self._executor.submit(lambda: parent_context.run(operation))
            except Exception:
                self._capacity.release()
                raise ToolRunnerUnavailableError("tool runner unavailable") from None

        future.add_done_callback(self._release_capacity)
        try:
            return future.result(timeout=timeout_seconds)
        except (CancelledError, FutureTimeoutError):
            future.cancel()
            raise ToolRunnerUnavailableError("tool runner unavailable") from None

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._executor.shutdown(wait=False, cancel_futures=True)

    def _release_capacity(self, future: Future[object]) -> None:
        self._capacity.release()
