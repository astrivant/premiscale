"""
Verify independent Kafka subscriber recovery and acknowledgement after database commits.
"""

from __future__ import annotations

from contextlib import ExitStack
import os
from queue import Empty
from time import monotonic
from typing import TYPE_CHECKING
from unittest.mock import Mock
from uuid import uuid4

from confluent_kafka.admin import AdminClient, NewTopic
import pytest

from premiscale.config.v1alpha1 import Kafka
from premiscale.messaging import InvalidMessage
from premiscale.messaging.kafka import KafkaFanout, KafkaLag, KafkaQueue
from premiscale.metrics.publisher import MetricsPublisher
from tests.unit.test_metrics_pipeline import batch

if TYPE_CHECKING:
    from typing import Iterator
    from premiscale.schemas.metrics import MetricBatch


@pytest.fixture
def kafka() -> Iterator[Kafka]:
    """
    Create isolated retained topics on a disposable Kafka broker.

    Yields:
        Kafka: Transport settings with unique topic and consumer group prefixes.
    """
    address = os.getenv('PREMISCALE_TEST_KAFKA_BOOTSTRAP_SERVERS')
    if not address:
        pytest.skip('Set PREMISCALE_TEST_KAFKA_BOOTSTRAP_SERVERS for Kafka integration tests')
    identity = str(uuid4())
    config = Kafka(True, address, f'premiscale-test-{identity}', f'premiscale-test-{identity}')
    admin = AdminClient({'bootstrap.servers': address})
    names = [config.topic, f'{config.topic}.dead']
    for future in admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1) for name in names]).values():
        future.result(timeout=30)
    try:
        yield config
    finally:
        for future in admin.delete_topics(names).values():
            future.result(timeout=30)


def publish_next(publisher: MetricsPublisher) -> None:
    """
    Wait for group assignment while allowing processing failures to reach the test.

    Args:
        publisher (MetricsPublisher): Database subscriber under test.

    Returns:
        None: No value is returned.

    Raises:
        AssertionError: If no record arrives before the test deadline.
    """
    deadline = monotonic() + 20
    while monotonic() < deadline:
        try:
            publisher.publish_once()
            return
        except Empty:
            pass
    raise AssertionError('Kafka subscriber did not receive its pending record')


def test_kafka_subscribers_recover_independently_without_skipping_failed_writes(kafka: Kafka, batch: MetricBatch) -> None:
    """
    Keep a failed database's offset while another destination commits its own copy.

    Args:
        kafka (Kafka): Isolated Kafka topics and consumer groups.
        batch (MetricBatch): Reduced measurements to publish.

    Returns:
        None: No value is returned.
    """
    with ExitStack() as resources:
        producer = KafkaFanout(kafka)
        resources.callback(producer.close)
        identity = producer.publish(batch)
        unstarted = KafkaLag(kafka, 'not-started-yet')
        resources.callback(unstarted.close)
        assert unstarted.count() == 1
        first = resources.enter_context(KafkaQueue(kafka, 'first'))
        second = resources.enter_context(KafkaQueue(kafka, 'second'))
        failed = Mock()
        failed.publish.side_effect = RuntimeError('Database unavailable')
        independent = Mock()
        with pytest.raises(RuntimeError, match='Database unavailable'):
            publish_next(MetricsPublisher(first, failed))
        publish_next(MetricsPublisher(second, independent))
        independent.publish.assert_called_once_with(batch, identity)
        pending = KafkaLag(kafka, 'first')
        complete = KafkaLag(kafka, 'second')
        resources.callback(pending.close)
        resources.callback(complete.close)
        assert pending.count() == 1
        assert complete.count() == 0
        failed.publish.side_effect = None
        publish_next(MetricsPublisher(first, failed))
        assert failed.publish.call_count == 2
        assert failed.publish.call_args_list[0] == failed.publish.call_args_list[1]
        assert pending.count() == 0


def test_invalid_kafka_record_is_dead_lettered_before_its_offset_advances(kafka: Kafka) -> None:
    """
    Preserve malformed payloads without repeatedly blocking a subscriber partition.

    Args:
        kafka (Kafka): Isolated Kafka topics and consumer groups.

    Returns:
        None: No value is returned.
    """
    producer = KafkaFanout(kafka)
    try:
        producer.send(kafka.topic, str(uuid4()), '{invalid-json')
        with KafkaQueue(kafka, 'poison') as queue:
            adapter = Mock()
            with pytest.raises(InvalidMessage):
                publish_next(MetricsPublisher(queue, adapter))
            adapter.publish.assert_not_called()
        lag = KafkaLag(kafka, 'poison')
        try:
            assert lag.count() == 0
        finally:
            lag.close()
    finally:
        producer.close()
