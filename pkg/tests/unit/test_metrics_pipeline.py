"""
Verify reduction, durable fanout, and independent database publication.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
import os
from queue import Empty
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, Mock
from uuid import uuid4

from attrs import evolve
import psycopg
import pytest
from redis.exceptions import ResponseError

from premiscale.config.v1alpha1 import Config, Host
from premiscale.config.databases import Databases, SQLiteState, LocalMetrics, PostgreSQLMetrics
from premiscale.messaging import InvalidMessage
from premiscale.metrics.codec import decode_batch, encode_batch
from premiscale.metrics.collector import MetricsCollector
from premiscale.metrics.fanout import MetricsFanout, metrics_queue
from premiscale.metrics.publisher import MetricsPublisher
from premiscale.metrics.publishers.local import LocalPublisher
from premiscale.metrics.publishers.postgresql import PostgreSQLPublisher
from premiscale.schemas.metrics import Metric, MetricBatch
from premiscale.support.sql import load_sql
from tests.unit.test_messaging import broker

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any
    from premiscale.config.v1alpha1 import Broker


@pytest.fixture
def batch() -> MetricBatch:
    """
    Use exact integer counters and timezone-aware timestamps for publication tests.

    Returns:
        MetricBatch: Two measurements sharing the same collection timestamp.
    """
    now = datetime.now(timezone.utc)
    return MetricBatch((Metric('cpu', now, {'host': 'host', 'name': 'worker'}, {'cpu_time': 2 ** 55 + 1}),
                        Metric('memory', now, {'host': 'host', 'name': 'worker'}, {'utilization': 12.5})))


def test_metric_codec_preserves_types_and_timestamp(batch: MetricBatch) -> None:
    """
    Round-trip a metrics batch through JSON without losing large counters or timezones.

    Args:
        batch (MetricBatch): Reduced metrics fixture.

    Returns:
        None: No value is returned.
    """
    assert decode_batch(json.loads(json.dumps(encode_batch(batch)))) == batch


@pytest.mark.parametrize('field, value', [('fields', {'cpu': True}), ('fields', {'cpu': float('nan')}),
                                        ('fields', {'cpu': '100'}), ('time', '2026-01-01T00:00:00'),
                                        ('tags', {'_premiscale_sample': 'forged'})])
def test_metric_codec_rejects_invalid_payloads(batch: MetricBatch, field: str, value: Any) -> None:
    """
    Reject values that could corrupt numeric samples or publisher deduplication.

    Args:
        batch (MetricBatch): Reduced metrics fixture.
        field (str): Metric field to corrupt.
        value (Any): Invalid JSON-compatible value.

    Returns:
        None: No value is returned.
    """
    payload = encode_batch(batch)
    payload['metrics'][0][field] = value
    with pytest.raises(ValueError):
        decode_batch(payload)


def test_collector_releases_host_before_reduction_and_publication(batch: MetricBatch, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Collect raw snapshots and publish outside the hypervisor connection lifetime.

    Args:
        batch (MetricBatch): Reduced metrics fixture.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring collector dependencies.

    Returns:
        None: No value is returned.
    """
    connection = MagicMock()
    raw = (object(),)
    connection.__enter__.return_value.request_domain_stats.return_value = raw
    monkeypatch.setattr('premiscale.metrics.collector.build_hypervisor_connection', Mock(return_value=connection))
    reduce = Mock(return_value=batch)
    monkeypatch.setattr('premiscale.metrics.collector.compile_domains', reduce)
    fanout = Mock()
    host = cast(Host, SimpleNamespace(name='host'))
    config = cast(Config, SimpleNamespace(controller=SimpleNamespace(
        autoscale=SimpleNamespace(groups={'workers': SimpleNamespace(hosts=['host'])}),
        kubernetes=SimpleNamespace(clusterName='cluster'))))

    def publish(value: MetricBatch) -> None:
        """
        Check that publication receives reduced data after the host has closed.

        Args:
            value (MetricBatch): Batch submitted by the collector.

        Returns:
            None: No value is returned.
        """
        connection.__exit__.assert_called_once()
        assert value is batch

    fanout.publish.side_effect = publish
    MetricsCollector(config, fanout).collect_host(host)
    reduce.assert_called_once_with(raw)
    fanout.publish.assert_called_once_with(batch)


def test_local_publisher_deduplicates_after_reopening(batch: MetricBatch, tmp_path: Path) -> None:
    """
    Retain measurements and sample identities through a publisher process restart.

    Args:
        batch (MetricBatch): Reduced metrics fixture.
        tmp_path (Path): Isolated CSV storage directory.

    Returns:
        None: No value is returned.
    """
    settings = LocalMetrics(dbfile=str(tmp_path / 'metrics.csv'))
    identity = str(uuid4())
    first = LocalPublisher(settings)
    try:
        first.publish(batch, identity)
        first.publish(batch, identity)
        assert len(first.store.get_all()) == 2
    finally:
        first.close()
    second = LocalPublisher(settings)
    try:
        second.publish(batch, identity)
        assert len(second.store.get_all()) == 2
    finally:
        second.close()


@pytest.mark.parametrize('name', ['primary', '_state', 'bad:name', '../publisher', 'x' * 64])
def test_invalid_subscriber_names_are_rejected(name: str) -> None:
    """
    Reject names that collide with the primary subscriber or cannot form queue keys.

    Args:
        name (str): Invalid configured publisher name.

    Returns:
        None: No value is returned.
    """
    with pytest.raises(ValueError, match='Publisher names'):
        Databases(5, 30, 1, SQLiteState(), LocalMetrics(),
                  publishers={name: LocalMetrics()})


def test_publishers_cannot_share_a_csv_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Resolve environment variables before checking for competing TinyFlux writers.

    Args:
        tmp_path (Path): Isolated CSV directory.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring the CSV path environment variable.

    Returns:
        None: No value is returned.
    """
    path = tmp_path / 'metrics.csv'
    monkeypatch.setenv('PREMISCALE_TEST_CSV', str(path))
    with pytest.raises(ValueError, match='own CSV file'):
        Databases(5, 30, 1, SQLiteState(), LocalMetrics(dbfile='$PREMISCALE_TEST_CSV'),
                  publishers={'duplicate': LocalMetrics(dbfile=str(path))})


def test_metrics_fanout_isolates_failed_subscriber(broker: Broker, batch: MetricBatch, tmp_path: Path) -> None:
    """
    Deliver every batch to both destinations while retaining only the failed copy.

    Args:
        broker (Broker): Disposable Dragonfly configuration.
        batch (MetricBatch): Reduced metrics fixture.
        tmp_path (Path): Independent subscriber CSV files.

    Returns:
        None: No value is returned.
    """
    with ExitStack() as resources:
        fanout = MetricsFanout(broker, ['healthy', 'unavailable'])
        resources.callback(fanout.close)
        identity = fanout.publish(batch)
        healthy = resources.enter_context(metrics_queue(broker, 'healthy'))
        unavailable = resources.enter_context(metrics_queue(broker, 'unavailable'))
        store = LocalPublisher(LocalMetrics(dbfile=str(tmp_path / 'healthy.csv')))
        resources.callback(store.close)
        MetricsPublisher(healthy, store).publish_once(block=False)
        assert len(store.store.get_all()) == 2
        failure = Mock()
        failure.publish.side_effect = RuntimeError('Database unavailable')
        with pytest.raises(RuntimeError, match='Database unavailable'):
            MetricsPublisher(unavailable, failure).publish_once(block=False)
        assert healthy.client.xlen(healthy.key) == 0
        assert healthy.client.xlen(unavailable.key) == 1
        pending = unavailable.client.xpending_range(unavailable.key, unavailable.group, '-', '+', 1)[0]
        unavailable.client.xclaim(unavailable.key, unavailable.group, unavailable.consumer, 0,
                                   [pending['message_id']], idle=4000)
        replacement = resources.enter_context(metrics_queue(broker, 'unavailable'))
        with replacement.delivery(block=False) as recovered:
            assert recovered.message_id == identity
            assert recovered.payload == batch
        with pytest.raises(Empty), healthy.delivery(block=False):
            pytest.fail('The successful publisher received a duplicate delivery')


def test_fanout_validates_all_destinations_before_append(broker: Broker, batch: MetricBatch) -> None:
    """
    Leave healthy destinations untouched when another stream key has the wrong type.

    Args:
        broker (Broker): Disposable Dragonfly configuration.
        batch (MetricBatch): Reduced metrics fixture.

    Returns:
        None: No value is returned.
    """
    fanout = MetricsFanout(broker, ['good', 'bad'])
    try:
        client = fanout.queues[0].client
        client.set(fanout.queues[1].key, 'not a stream')
        with pytest.raises(ResponseError, match='not a stream'):
            fanout.publish(batch)
        assert client.xlen(fanout.queues[0].key) == 0
    finally:
        fanout.close()


def test_malformed_metrics_are_dead_lettered_per_subscriber(broker: Broker, batch: MetricBatch) -> None:
    """
    Reject malformed batches before they can reach a database adapter.

    Args:
        broker (Broker): Disposable Dragonfly configuration.
        batch (MetricBatch): Reduced metrics fixture.

    Returns:
        None: No value is returned.
    """
    with metrics_queue(broker, 'invalid') as queue:
        payload = encode_batch(batch)
        payload['metrics'][0]['fields'] = {'cpu_time': True}
        queue.client.xadd(queue.key, {'message': json.dumps({'version': 1, 'id': str(uuid4()), 'data': payload})})
        publisher = Mock()
        with pytest.raises(InvalidMessage):
            MetricsPublisher(queue, publisher).publish_once(block=False)
        publisher.publish.assert_not_called()
        assert queue.client.xlen(queue.dead_letters) == 1


def test_postgresql_commits_deduplicates_and_rolls_back(batch: MetricBatch) -> None:
    """
    Preserve numeric samples, avoid duplicate retries, and roll back partial batches.

    Args:
        batch (MetricBatch): Reduced metrics fixture.

    Returns:
        None: No value is returned.
    """
    dsn = os.getenv('PREMISCALE_TEST_POSTGRES_DSN')
    if not dsn:
        pytest.skip('Set PREMISCALE_TEST_POSTGRES_DSN to run PostgreSQL publication tests')
    publisher = PostgreSQLPublisher(PostgreSQLMetrics(dsn=dsn))
    identity = str(uuid4())
    failed_identity = str(uuid4())
    try:
        publisher.publish(batch, identity)
        publisher.close()
        publisher.publish(batch, identity)
        with psycopg.connect(dsn) as observer:
            rows = observer.execute(load_sql('metrics/get_batch.sql'), (identity,)).fetchall()
            assert len(rows) == 2
            assert rows[0][3]['cpu_time'] == 2 ** 55 + 1
            assert rows[0][1] == batch.metrics[0].time
        invalid = MetricBatch((batch.metrics[0], evolve(batch.metrics[1], fields={'invalid': float('nan')})))
        with pytest.raises(psycopg.Error):
            publisher.publish(invalid, failed_identity)
        with psycopg.connect(dsn) as observer:
            assert observer.execute(load_sql('metrics/get_batch.sql'), (failed_identity,)).fetchall() == []
        expired = MetricBatch(tuple(evolve(metric, time=metric.time - timedelta(days=1)) for metric in batch.metrics))
        expired_identity = str(uuid4())
        publisher.publish(expired, expired_identity)
        with psycopg.connect(dsn) as observer:
            assert observer.execute(load_sql('metrics/get_batch.sql'), (expired_identity,)).fetchall() == []
    finally:
        publisher.close()
