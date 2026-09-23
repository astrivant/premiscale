"""
Verify independent worker roles, shared scheduling, and KEDA demand observations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import os
from pathlib import Path
import socket
from threading import Barrier
from time import monotonic, sleep
from unittest.mock import Mock
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import psycopg
import pytest
from jsonschema import Draft7Validator
from redis.exceptions import ConnectionError as RedisConnectionError
from ruamel.yaml import YAML

from premiscale.api import create_app
from premiscale.config.v1alpha1 import Config, Host
from premiscale.config.databases import PostgreSQLMetrics
from premiscale.daemon.processes import build
from premiscale.metrics.fanout import metrics_queue
from premiscale.metrics.publisher import MetricsPublisher
from premiscale.metrics.publishers.postgresql import PostgreSQLPublisher
from premiscale.support.lua import load_lua
from premiscale.support.sql import load_sql
from premiscale.reconciliation.activity import Activity
from premiscale.reconciliation.collectors.queued import collect_once
from premiscale.reconciliation.demand import Demand
from premiscale.reconciliation.collectors.schedule import collection_queue, schedule_once
from premiscale.daemon.settings import Execution
from tests.data.processes.run_worker import run as run_worker
from tests.unit.test_broker_chart import broker_environment, render
from tests.unit.test_messaging import broker
from tests.unit.test_kafka_transport import kafka
from premiscale.messaging.kafka import KafkaFanout, KafkaLag
from tests.unit.test_metrics_pipeline import batch

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Broker, Kafka
    from premiscale.schemas.metrics import MetricBatch


ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def worker_config() -> Config:
    """
    Load the documented Kubernetes worker example without opening connections.

    Returns:
        Config: Controller with one remote PostgreSQL subscriber and two test hosts.
    """
    values = YAML(typ='safe').load(ROOT / '.config/keda/controller.yaml')
    # This fixture performs no TLS IO; the configuration only checks file existence.
    values['controller']['platform']['certificates']['path'] = __file__
    config = Config.from_dict(values)
    config.controller.autoscale.hosts = [Host(name, f'{name}.example', 'ssh', 22, 'qemu')
                                       for name in ('first', 'second')]
    return config


def test_roles_keep_single_writers_out_of_workers(worker_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Keep infrastructure and local-file writers in one controller while scaling remote IO.

    Args:
        worker_config (Config): Kubernetes controller and subscriber settings.
        monkeypatch (pytest.MonkeyPatch): Attribute override fixture.

    Returns:
        None: No value is returned.
    """
    controller = build(worker_config, 'version', '', Execution(mode='hha', publishers=('postgresql',)))
    names = {process.name for process in controller}
    assert names == {'api', 'leadership'}
    assert 'metrics-publisher-postgresql' not in names
    assert 'reconciliation' not in names
    collectors = build(worker_config, 'version', '', Execution(mode='hha', role='collector'))
    assert [process.name for process in collectors] == ['api', 'reconciliation']
    publishers = build(worker_config, 'version', '', Execution(mode='hha', role='publisher', publisher='postgresql', connections=2))
    assert [process.name for process in publishers] == ['api', 'reconciliation']
    with pytest.raises(ValueError, match='PostgreSQL'):
        build(worker_config, 'version', '', Execution(mode='hha', role='publisher', publisher='primary'))
    worker_config.controller.mode = 'standalone'
    with pytest.raises(ValueError, match='kubernetes mode'):
        build(worker_config, 'version', '', Execution(mode='hha', role='collector'))


@pytest.mark.parametrize('name_override', ['', 'premiscale-worker'])
def test_scaling_chart_separates_targets_and_bounds_database_connections(tmp_path: Path, name_override: str) -> None:
    """
    Render independently scaled workloads without sharing the controller journal or selectors.

    Args:
        tmp_path (Path): Temporary Helm values directory.
        name_override (str): Parent chart name, including a potential worker label collision.

    Returns:
        None: No value is returned.
    """
    values = YAML(typ='safe').load(ROOT / '.config/keda/values.yaml')
    values['keda']['enabled'] = name_override == ''
    values['strimzi-kafka-operator']['enabled'] = False
    values['nameOverride'] = name_override
    values['configMap']['config'] = (ROOT / '.config/keda/controller.yaml').read_text()
    resources = render(tmp_path, values)
    controller = next(item for item in resources if item['kind'] == 'Deployment' and item['metadata']['name'] == 'premiscale')
    workers = [item for item in resources if item['kind'] == 'Deployment'
               and item['spec']['template']['metadata']['labels'].get('app.kubernetes.io/component')
               in {'collectors', 'publisher-postgresql'}]
    scaled = [item for item in resources if item['kind'] == 'ScaledObject']
    assert len(workers) == len(scaled) == 2
    if values['keda']['enabled']:
        crd = next(item for item in resources if item['kind'] == 'CustomResourceDefinition'
                   and item['spec']['names']['kind'] == 'ScaledObject')
        schema = next(version['schema']['openAPIV3Schema'] for version in crd['spec']['versions']
                      if version['name'] == 'v1alpha1')
        for scaler in scaled:
            Draft7Validator(schema).validate(scaler)
    assert controller['spec']['replicas'] == 2
    environment = controller['spec']['template']['spec']['containers'][0]['env']
    assert {'name': 'PREMISCALE_REMOTE_PUBLISHERS', 'value': 'postgresql'} in environment
    selector = controller['spec']['selector']['matchLabels']
    for worker in workers:
        assert 'replicas' not in worker['spec']
        assert worker['spec']['strategy']['rollingUpdate'] == {'maxSurge': 1, 'maxUnavailable': 0}
        pod = worker['spec']['template']
        assert any(pod['metadata']['labels'].get(key) != value for key, value in selector.items())
        assert not pod['spec']['automountServiceAccountToken']
        assert all('persistentVolumeClaim' not in volume for volume in pod['spec']['volumes'])
        container = pod['spec']['containers'][0]
        broker = next(item for item in container['env'] if item['name'] == 'PREMISCALE_REDIS_URL')
        assert broker == broker_environment(resources)
        scaler = next(item for item in scaled if item['spec']['scaleTargetRef']['name'] == worker['metadata']['name'])
        assert {trigger['metadata']['valueLocation'] for trigger in scaler['spec']['triggers']} == {
            'activeConnections', 'outstandingRequests',
        }
        assert all(trigger['metricType'] == 'AverageValue' for trigger in scaler['spec']['triggers'])
        if 'publisher' in worker['metadata']['name']:
            assert scaler['spec']['maxReplicaCount'] == 4
            assert {'name': 'PREMISCALE_PUBLISHER_CONNECTIONS', 'value': '2'} in container['env']


def test_scheduler_limits_work_and_consumers_share_hosts(worker_config: Config, broker: Broker) -> None:
    """
    Keep one unfinished request per host across repeated scheduling and parallel consumers.

    Args:
        worker_config (Config): Controller with two configured hosts.
        broker (Broker): Isolated Redis-compatible test namespace.

    Returns:
        None: No value is returned.
    """
    worker_config.controller.broker = broker
    with collection_queue(broker) as first, collection_queue(broker) as second:
        assert schedule_once(worker_config, first) == 2
        assert schedule_once(worker_config, second) == 0
        with first.delivery(block=False) as left, second.delivery(block=False) as right:
            assert {left.payload, right.payload} == {'first', 'second'}
            assert schedule_once(worker_config, first) == 0
        # Completion does not bypass the configured polling cadence.
        assert schedule_once(worker_config, first) == 0
        assert first.client.xlen(first.key) == 0
        first.client.delete(f'{first.key}:schedule')
        assert schedule_once(worker_config, first) == 2


def test_failed_collection_remains_pending_and_removed_hosts_are_retired(worker_config: Config, broker: Broker) -> None:
    """
    Preserve failed fanout for recovery without contacting a host removed from configuration.

    Args:
        worker_config (Config): Local authoritative host configuration.
        broker (Broker): Isolated Redis-compatible test namespace.

    Returns:
        None: No value is returned.
    """
    worker_config.controller.broker = broker
    with collection_queue(broker) as queue:
        queue.put('first')
        collector = Mock(collect_host=Mock(side_effect=RuntimeError('fanout failed')))
        assert collect_once(worker_config, collector) is False
        assert queue.client.xlen(queue.key) == 1
        queue.put('removed')
        collector.collect_host.reset_mock()
        assert collect_once(worker_config, collector) is True
        collector.collect_host.assert_not_called()
        assert queue.client.xlen(queue.key) == 1


def test_demand_counts_backlog_and_expires_crashed_connections(worker_config: Config, broker: Broker) -> None:
    """
    Aggregate live connections across replicas and retain backlog independently of activity.

    Args:
        worker_config (Config): Delegated workload configuration.
        broker (Broker): Isolated Redis-compatible test namespace.

    Returns:
        None: No value is returned.
    """
    worker_config.controller.broker = broker
    worker_config.controller.kafka.enabled = False
    demand = Demand(worker_config, Execution(mode='hha', publishers=('postgresql',)))
    try:
        with collection_queue(broker) as queue, Activity(queue) as first, Activity(queue) as second:
            queue.put('first')
            queue.put('second')
            with ExitStack() as active:
                active.enter_context(first.connection())
                active.enter_context(second.connection())
                assert demand.snapshot('collectors') == {'activeConnections': 2, 'outstandingRequests': 2}
            assert demand.snapshot('collectors') == {'activeConnections': 0, 'outstandingRequests': 2}
            queue.client.eval(load_lua('workers/activity.lua'), 2, f'{queue.key}:activity', f'{queue.key}:counts',
                              'crashed', 5, -1)
            assert demand.snapshot('collectors')['activeConnections'] == 0
            assert not queue.client.hexists(f'{queue.key}:counts', 'crashed')
            assert demand.snapshot('publishers/postgresql') == {'activeConnections': 0, 'outstandingRequests': 0}
    finally:
        demand.close()


def test_scaling_api_returns_unavailable_instead_of_zero() -> None:
    """
    Prevent failed observations from requesting scale-down or exposing credentials.

    Returns:
        None: No value is returned.
    """
    demand = Mock()
    demand.snapshot.return_value = {'activeConnections': 2, 'outstandingRequests': 8}
    client = create_app(demand=demand).test_client()
    assert client.get('/scaling/collectors').json == demand.snapshot.return_value
    demand.snapshot.side_effect = RedisConnectionError('secret connection string')
    response = client.get('/scaling/collectors')
    assert response.status_code == 503
    assert b'secret' not in response.data
    demand.snapshot.side_effect = KeyError('unknown')
    assert client.get('/scaling/publishers/unknown').status_code == 404


def test_parallel_postgresql_publishers_share_and_deduplicate_work(broker: Broker, batch: MetricBatch) -> None:
    """
    Use separate database connections while deduplicating redelivery across workers.

    Args:
        broker (Broker): Isolated Redis-compatible test namespace.
        batch (MetricBatch): Reduced measurements with stable timestamps.

    Returns:
        None: No value is returned.
    """
    dsn = os.getenv('PREMISCALE_TEST_POSTGRES_DSN')
    if not dsn:
        pytest.skip('Set PREMISCALE_TEST_POSTGRES_DSN to test remote workers')
    settings = PostgreSQLMetrics(dsn=dsn)
    initializer = PostgreSQLPublisher(settings)
    try:
        initializer.publish(batch, str(uuid4()))
    finally:
        initializer.close()
    identity = str(uuid4())
    with metrics_queue(broker, 'postgresql') as queue:
        queue.put(batch, message_id=identity)
        queue.put(batch, message_id=identity)
    barrier = Barrier(2)

    def consume() -> int:
        """
        Persist one claimed batch through an independently owned database connection.

        Returns:
            int: PostgreSQL server process ID for the worker's connection.
        """
        publisher = PostgreSQLPublisher(settings)
        try:
            with metrics_queue(broker, 'postgresql') as queue:
                barrier.wait(timeout=10)
                MetricsPublisher(queue, publisher).publish_once(block=False)
                assert publisher.connection is not None
                return publisher.connection.info.backend_pid
        finally:
            publisher.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(consume) for _ in range(2)]
        assert len({future.result(timeout=20) for future in futures}) == 2
    with metrics_queue(broker, 'postgresql') as queue:
        assert queue.client.xlen(queue.key) == 0
    with psycopg.connect(dsn) as observer:
        assert len(observer.execute(load_sql('metrics/get_batch.sql'), (identity,)).fetchall()) == len(batch.metrics)


def test_publisher_pod_starts_serves_health_and_shuts_down(worker_config: Config, broker: Broker,
                                                         batch: MetricBatch, tmp_path: Path, kafka: Kafka) -> None:
    """
    Run the real publisher process tree and verify delivery, health, and bounded shutdown.

    Args:
        worker_config (Config): Parsed worker configuration.
        broker (Broker): Isolated Redis-compatible test namespace.
        batch (MetricBatch): Reduced measurement batch.
        tmp_path (Path): Private runtime directory for process observations.
        kafka (Kafka): Isolated Kafka metrics topic and consumer groups.

    Returns:
        None: No value is returned.
    """
    dsn = os.getenv('PREMISCALE_TEST_POSTGRES_DSN')
    if not dsn:
        pytest.skip('Set PREMISCALE_TEST_POSTGRES_DSN to test worker processes')
    worker_config.controller.broker = broker
    destination = worker_config.controller.databases.publishers['postgresql']
    assert isinstance(destination, PostgreSQLMetrics)
    destination.dsn = dsn
    worker_config.controller.kafka = kafka
    health = worker_config.controller.healthcheck
    health.stateDirectory = str(tmp_path)
    health.host = '127.0.0.1'
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        health.apiPort = listener.getsockname()[1]
    producer = KafkaFanout(kafka)
    try:
        identity = producer.publish(batch)
    finally:
        producer.close()
    process = mp.get_context('spawn').Process(target=run_worker, args=(
        worker_config, Execution(mode='hha', role='publisher', publisher='postgresql', connections=2),
    ))
    process.start()
    try:
        deadline = monotonic() + 20
        while True:
            assert process.is_alive(), process.exitcode
            assert monotonic() < deadline, 'Worker never became ready'
            try:
                with urlopen(f'http://127.0.0.1:{health.apiPort}/ready', timeout=1) as response:
                    if response.status == 200:
                        break
            except URLError:
                sleep(0.1)
        lag = KafkaLag(kafka, 'postgresql')
        try:
            while lag.count():
                assert monotonic() < deadline, 'Worker did not publish its queued batch'
                sleep(0.1)
        finally:
            lag.close()
        with psycopg.connect(dsn) as observer:
            assert len(observer.execute(load_sql('metrics/get_batch.sql'), (identity,)).fetchall()) == len(batch.metrics)
        process.terminate()
        process.join(timeout=20)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        process.close()
