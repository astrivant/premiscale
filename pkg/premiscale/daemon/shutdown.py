"""
Record termination requests without acquiring locks inside a signal handler.
"""

from __future__ import annotations

from contextlib import contextmanager
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import FrameType
    from typing import Iterator


class Shutdown:
    """
    Keep a main-thread stop flag that signals can update during any supervisor operation.
    """

    def __init__(self) -> None:
        """
        Initialize Shutdown with the supplied settings.
        """
        self.requested = False

    def is_set(self) -> bool:
        """
        Report whether termination was requested.

        Returns:
            bool: Whether termination was requested.
        """
        return self.requested

@contextmanager
def signals(stopped: Shutdown) -> Iterator[None]:
    """
    Record SIGINT and SIGTERM and restore previous handlers after all cleanup.

    Args:
        stopped (Shutdown): Flag indicating that controller shutdown has been requested.

    Yields:
        None: Control while the temporary signal handlers remain installed.
    """
    previous = {}

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        """
        Record a shutdown request without acquiring locks in the signal handler.

        Args:
            _signum (int): Signal number supplied by the operating system.
            _frame (FrameType | None): Interrupted Python stack frame, when available.

        Returns:
            None: No value is returned.
        """
        stopped.requested = True

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
