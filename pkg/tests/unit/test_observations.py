"""
Verify independently delivered state observations, stale-sample rejection, and wire validation.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
import os
from typing import TYPE_CHECKING
from unittest.mock import Mock
from uuid import uuid4

from attrs import evolve
import pytest

from premiscale.messaging.kafka import KafkaFanout, KafkaQueue
from premiscale.metrics.fanout import MetricsFanout, metrics_queue
from premiscale.metrics.publisher import MetricsPublisher
from .test_messaging import broker
from .test_kafka_transport import kafka, publish_next

from premiscale.metrics.codec import decode_batch, encode_batch
from premiscale.metrics.publishers.state import StatePublisher
from premiscale.schemas.metrics import Metric, MetricBatch
from premiscale.schemas.observations import DomainObservation
from premiscale.schemas.qemu import ManagedDomain

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any
    from premiscale.config.v1alpha1 import Broker, Kafka


@pytest.fixture
def observed() -> DomainObservation:
    """
    Describe one managed VM with state and capacity counters.

    Returns:
        DomainObservation: Timestamped VM observation with a unique cluster and UUID.
    """
    return DomainObservation(ManagedDomain(str(uuid4()), 'worker', f'cluster-{uuid4()}', 'workers', 'host'),
                             datetime.now(timezone.utc), state=1, reason=1, vcpus=2,
                             memory_bytes=4096, storage_bytes=8192)


def test_observation_codec_preserves_types_and_optional_counters(observed: DomainObservation) -> None:
    """
    Transfer observations with metrics while retaining old metrics-only payload support.

    Args:
        observed (DomainObservation): Managed VM observation fixture.

    Returns:
        None: No value is returned.
    """
    batch = MetricBatch((), (evolve(observed, memory_bytes=None),))
    assert decode_batch(json.loads(json.dumps(encode_batch(batch)))) == batch
    assert decode_batch({'metrics': []}) == MetricBatch(())


@pytest.mark.parametrize('key, value', [('state', True), ('vcpus', -1), ('memory_bytes', 1.5),
                                       ('time', '2026-01-01T00:00:00'), ('domain', {})])
def test_observation_codec_rejects_ambiguous_or_invalid_state(observed: DomainObservation, key: str, value: Any) -> None:
    """
    Prevent malformed state from reaching the database subscriber.

    Args:
        observed (DomainObservation): Managed VM observation fixture.
        key (str): Wire field to replace.
        value (Any): Invalid wire value.

    Returns:
        None: No value is returned.
    """
    payload = encode_batch(MetricBatch((), (observed,)))
    datum = payload['metrics'][0]
    if key == 'domain':
        datum['tags'] = value
    elif key == 'time':
        datum['time'] = value
    else:
        datum['fields'][key] = value
    with pytest.raises(ValueError):
        decode_batch(payload)


@pytest.mark.parametrize('backend', ['sqlite', 'postgresql'])
def test_state_updates_revisions_only_for_changes_and_rejects_stale_delivery(
        observed: DomainObservation, tmp_path: Path, backend: str) -> None:
    """
    Persist fresh observations independently without turning duplicates into state changes.

    Args:
        observed (DomainObservation): Managed VM observation fixture.
        tmp_path (Path): Isolated SQLite directory.
        backend (str): Real SQL adapter to exercise.

    Returns:
        None: No value is returned.
    """
    dsn = os.getenv('PREMISCALE_TEST_POSTGRES_DSN', '') if backend == 'postgresql' else ''
    if backend == 'postgresql' and not dsn:
        pytest.skip('PostgreSQL integration service is not configured')
    publisher = StatePublisher(str(tmp_path / 'state.db'), observed.domain.cluster, dsn)
    assert publisher.connection is None
    try:
        publisher.publish(MetricBatch((), (observed,)), str(uuid4()))
        assert publisher.connection is not None
        first = publisher.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone()
        publisher.publish(MetricBatch((), (observed,)), str(uuid4()))
        assert publisher.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone() == first
        unchanged = evolve(observed, time=observed.time + timedelta(seconds=5), memory_bytes=None)
        publisher.publish(MetricBatch((), (unchanged,)), str(uuid4()))
        fresh = publisher.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone()
        assert fresh[11] > first[11] and fresh[12:] == first[12:]
        assert fresh[9] == observed.memory_bytes
        changed = evolve(observed, time=observed.time + timedelta(seconds=10), state=5)
        publisher.publish(MetricBatch((), (changed,)), str(uuid4()))
        newer = publisher.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone()
        assert newer[6] == 5 and newer[13] == 2 and newer[12] > first[12]
        publisher.close()
        publisher.publish(MetricBatch((), (observed,)), str(uuid4()))
        assert publisher.connection is not None
        assert publisher.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone() == newer
        with pytest.raises(ValueError, match='different cluster'):
            publisher.publish(MetricBatch((), (evolve(observed, domain=evolve(observed.domain, cluster='foreign')),)), str(uuid4()))
    finally:
        publisher.close()


@pytest.mark.parametrize('transport', ['redis', 'kafka'])
def test_state_failure_does_not_block_metrics_and_its_delivery_recovers(
        observed: DomainObservation, tmp_path: Path, broker: Broker, kafka: Kafka, transport: str) -> None:
    """
    Deliver identical compiled data to independent state and visualization subscribers.

    Args:
        observed (DomainObservation): Managed VM observation fixture.
        tmp_path (Path): Isolated observation store directory.
        broker (Broker): Disposable Redis-compatible broker namespace.
        kafka (Kafka): Disposable Kafka topic and consumer groups.
        transport (str): Durable fanout implementation to exercise.

    Returns:
        None: No value is returned.
    """
    batch = MetricBatch((Metric('cpu', observed.time, {'id': observed.domain.id}, {'cpu_time_ns': 100}),), (observed,))
    with ExitStack() as resources:
        fanout = KafkaFanout(kafka) if transport == 'kafka' else MetricsFanout(broker, ['_state', 'visualization'])
        state_queue = KafkaQueue(kafka, '_state') if transport == 'kafka' else metrics_queue(broker, '_state')
        metric_queue = KafkaQueue(kafka, 'visualization') if transport == 'kafka' else metrics_queue(broker, 'visualization')
        for item in (fanout, state_queue, metric_queue):
            resources.callback(item.close)
        identity = fanout.publish(batch)
        failed = Mock()
        failed.publish.side_effect = OSError('Database unavailable')
        with pytest.raises(OSError):
            publish_next(MetricsPublisher(state_queue, failed))
        visualization = Mock()
        publish_next(MetricsPublisher(metric_queue, visualization))
        visualization.publish.assert_called_once_with(batch, identity)
        recovered = StatePublisher(str(tmp_path / 'recovered.db'), observed.domain.cluster)
        resources.callback(recovered.close)
        publish_next(MetricsPublisher(state_queue, recovered))
        assert recovered.connection is not None
        row = recovered.connection.execute('observations/select.sql', (observed.domain.cluster, observed.domain.id)).fetchone()
        assert row[6] == observed.state and row[13] == 1
