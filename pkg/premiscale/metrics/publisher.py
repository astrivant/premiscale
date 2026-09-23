"""
Consume one subscriber's backlog independently of collection and other databases.
"""

from __future__ import annotations

import logging
from contextlib import nullcontext
from queue import Empty
from time import sleep
from typing import TYPE_CHECKING

from premiscale.messaging import InvalidMessage

if TYPE_CHECKING:
    from premiscale.messaging import RedisQueue
    from premiscale.messaging.kafka import KafkaQueue
    from premiscale.schemas.metrics import MetricBatch
    from .publishers._base import Publisher
    from premiscale.reconciliation.activity import Activity


log = logging.getLogger(__name__)


class MetricsPublisher:
    """
    Acknowledge only committed database writes and retain failures for redelivery.
    """

    def __init__(self, queue: RedisQueue[MetricBatch] | KafkaQueue, publisher: Publisher, activity: 'Activity | None' = None) -> None:
        """
        Bind a subscriber stream to its process-local database adapter.

        Args:
            queue (RedisQueue[MetricBatch] | KafkaQueue): Subscriber's independently acknowledged backlog.
            publisher (Publisher): Adapter responsible for conversion and idempotent writes.
            activity (Activity | None): Optional reporter for connections busy writing batches.
        """
        self.queue = queue
        self.publisher = publisher
        self.activity = activity

    def publish_once(self, *, block: bool = True) -> None:
        """
        Persist one delivery while renewing its lease, then acknowledge success.

        Args:
            block (bool): Whether to wait for an available batch.

        Returns:
            None: No value is returned after publication and acknowledgement.
        """
        with self.queue.delivery(block=block) as delivery:
            with self.activity.connection() if self.activity is not None else nullcontext():
                self.publisher.publish(delivery.payload, delivery.message_id)

    def __call__(self) -> None:
        """
        Retry failed subscribers without blocking collection or other destinations.

        Returns:
            None: No value is returned before process shutdown.
        """
        delay = 1
        try:
            while True:
                try:
                    self.publish_once()
                    delay = 1
                except Empty:
                    continue
                except InvalidMessage:
                    log.warning('Malformed metrics batch moved to the subscriber dead-letter stream')
                except Exception as error:
                    log.warning('Metrics publication failed (%s); delivery remains queued', type(error).__name__)
                    self.queue.ready = False
                    try:
                        self.publisher.close()
                    except Exception as close_error:
                        log.warning('Metrics connection cleanup failed (%s)', type(close_error).__name__)
                    sleep(delay)
                    delay = min(delay * 2, 30)
        finally:
            try:
                self.publisher.close()
            finally:
                self.queue.close()
