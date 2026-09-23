"""
Fan out reduced metrics to independent durable subscriber queues.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import json
from uuid import uuid4

from premiscale.messaging.kafka import KafkaFanout, KafkaQueue
from premiscale.support.lua import load_lua
from premiscale.messaging.queue import RedisQueue
from .codec import decode_batch, encode_batch

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Broker, Config
    from premiscale.schemas.metrics import MetricBatch


def build_fanout(config: Config) -> 'MetricsFanout | KafkaFanout':
    """
    Select metrics transport independently of collection and database conversion.

    Args:
        config (Config): Metrics transport and configured destinations.

    Returns:
        MetricsFanout | KafkaFanout: Process-local metrics producer.
    """
    if config.controller.kafka.enabled:
        return KafkaFanout(config.controller.kafka)
    return MetricsFanout(config.controller.broker, ['_state', *config.controller.databases.destinations])


def subscriber_queue(config: Config, subscriber: str) -> 'RedisQueue[MetricBatch] | KafkaQueue':
    """
    Select one database's independent backlog without changing its adapter.

    Args:
        config (Config): Metrics transport and connection settings.
        subscriber (str): Stable subscriber name.

    Returns:
        RedisQueue[MetricBatch] | KafkaQueue: Acknowledged metrics consumer.
    """
    if config.controller.kafka.enabled:
        return KafkaQueue(config.controller.kafka, subscriber)
    return metrics_queue(config.controller.broker, subscriber)


def metrics_queue(config: Broker, subscriber: str) -> RedisQueue[MetricBatch]:
    """
    Construct one subscriber's queue without connecting to the broker.

    Args:
        config (Broker): Dragonfly or Redis connection settings.
        subscriber (str): Stable publisher name identifying its private backlog.

    Returns:
        RedisQueue[MetricBatch]: Queue with independent acknowledgement and delivery leases.
    """
    return RedisQueue(config, f'metrics:{subscriber}', encode=encode_batch, decode=decode_batch)


class MetricsFanout:
    """
    Publish one JSON envelope to every configured database subscriber.

    Each subscriber consumes its own stream, so its acknowledgements and failures
    do not affect other databases. Redis persistence controls backlog durability.
    """

    def __init__(self, config: Broker, subscribers: list[str]) -> None:
        """
        Prepare subscriber keys and a process-local broker connection pool.

        Args:
            config (Broker): Broker connection and namespace settings.
            subscribers (list[str]): Unique stable names of metric publishers.

        Raises:
            ValueError: If the subscriber list is empty or contains duplicates.
        """
        if not subscribers or len(set(subscribers)) != len(subscribers):
            raise ValueError('Metrics fanout requires unique subscribers')
        self.queues = [metrics_queue(config, name) for name in subscribers]
        self.script = load_lua('metrics/fanout.lua')

    def publish(self, batch: MetricBatch) -> str:
        """
        Append the same batch and stable message ID to every subscriber stream.

        Args:
            batch (MetricBatch): Reduced measurements from one collection pass.

        Returns:
            str: Publisher UUID shared by all copies for idempotent database writes.
        """
        identity = str(uuid4())
        body = json.dumps({'version': 1, 'id': identity, 'data': encode_batch(batch)},
                          separators=(',', ':'), allow_nan=False)
        keys = [queue.key for queue in self.queues]
        self.queues[0].client.eval(self.script, len(keys), *keys, body)
        return identity

    def close(self) -> None:
        """
        Release broker pools without deleting queued measurements.

        Returns:
            None: No value is returned.
        """
        for queue in self.queues:
            queue.close()
