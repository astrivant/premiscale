"""
Report active outbound operations with expiring, process-owned Redis observations.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
from threading import Event, RLock, Thread
from typing import TYPE_CHECKING
from uuid import uuid4

from redis.exceptions import RedisError

from premiscale.support.lua import load_lua

if TYPE_CHECKING:
    from typing import Iterator
    from premiscale.messaging.queue import RedisQueue


log = logging.getLogger(__name__)


class Activity:
    """
    Aggregate concurrent operations without counting idle database connections as demand.
    """

    def __init__(self, queue: RedisQueue) -> None:
        """
        Prepare one observation identity and a bounded heartbeat thread.

        Args:
            queue (RedisQueue): Process-local client identifying the workload stream.
        """
        self.queue = queue
        self.identity = str(uuid4())
        self.count = 0
        self.lock = RLock()
        self.stopped = Event()
        self.thread = Thread(target=self._heartbeat, name='outbound-activity', daemon=True)

    def _write(self) -> None:
        """
        Refresh the process count using the broker's clock and a thirty-second lease.

        Returns:
            None: No value is returned.
        """
        with self.lock:
            self.queue.client.eval(load_lua('workers/activity.lua'), 2,
                                   f'{self.queue.key}:activity', f'{self.queue.key}:counts',
                                   self.identity, self.count, 30)

    def _heartbeat(self) -> None:
        """
        Refresh live observations and retry transient broker failures.

        Returns:
            None: No value is returned when shutdown is requested.
        """
        while not self.stopped.wait(5):
            try:
                self._write()
            except RedisError:
                log.warning('Outbound activity refresh failed; observation will expire')

    @contextmanager
    def connection(self) -> Iterator[None]:
        """
        Count a host connection or a database operation until its cleanup finishes.

        Yields:
            None: Control while one outbound operation is active.
        """
        with self.lock:
            self.count += 1
        try:
            self._write()
            yield
        finally:
            with self.lock:
                self.count -= 1
            try:
                self._write()
            except RedisError:
                log.warning('Outbound activity cleanup failed; observation will expire')

    def __enter__(self) -> 'Activity':
        """
        Verify the broker and start reporting before processing any work.

        Returns:
            Activity: The running process-owned activity reporter.
        """
        self._write()
        self.thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        """
        Stop the heartbeat and remove this process's observation.

        Args:
            *_args (object): Context exit information.

        Returns:
            None: No value is returned.
        """
        self.stopped.set()
        self.thread.join(timeout=self.queue.config.socketTimeout + 1)
        with self.lock:
            self.count = 0
        try:
            self._write()
        except RedisError:
            log.warning('Outbound activity cleanup failed; observation will expire')
