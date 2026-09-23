"""
Exercise CPU-aware partitioning, PID feedback, and bounded concurrent collection.
"""

from __future__ import annotations

from contextlib import contextmanager
from threading import Event, Lock
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest

from premiscale.config.v1alpha1 import CollectionControl, Config, Host
from premiscale.metrics.collector import MetricsCollector
from premiscale.reconciliation import capacity
from premiscale.reconciliation.collectors import runtime
from premiscale.reconciliation.collectors import partitioned as worker
from premiscale.reconciliation.control import ThroughputController
from premiscale.schemas.collection import CollectionReport

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Iterator


@pytest.mark.parametrize('layout, quota, expected', [
    ('v2', '150000 100000', 2), ('v2', '10000 100000', 1),
    ('v2', 'max 100000', 8), ('v2', 'invalid', 8),
    ('v1', '300000', 3), ('v1', '-1', 8),
])
def test_cpu_quota_caps_affinity(layout: str, quota: str, expected: int,
                                tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Respect container quotas while tolerating unlimited or malformed quota files.

    Args:
        layout (str): Cgroup version to simulate.
        quota (str): Quota file contents.
        expected (int): Expected available logical CPU count.
        tmp_path (Path): Temporary cgroup filesystem root.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring CPU-count patches.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setattr(capacity.os, 'process_cpu_count', lambda: 8)
    membership = tmp_path / 'membership'
    if layout == 'v2':
        directory = tmp_path / 'pod' / 'container'
        directory.mkdir(parents=True)
        directory.joinpath('cpu.max').write_text(quota)
        membership.write_text('0::/pod/container\n')
    else:
        directory = tmp_path / 'cpu,cpuacct' / 'pod'
        directory.mkdir(parents=True)
        directory.joinpath('cpu.cfs_quota_us').write_text(quota)
        directory.joinpath('cpu.cfs_period_us').write_text('100000')
        membership.write_text('2:cpu,cpuacct:/pod\n')
    assert capacity.available_cores(tmp_path, membership) == expected


def test_cpu_ancestors_affinity_and_missing_cgroups(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Apply the tightest visible ancestor limit and retain a safe CPU fallback.

    Args:
        tmp_path (Path): Temporary cgroup filesystem root.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring CPU-count patches.

    Returns:
        None: No value is returned.
    """
    membership = tmp_path / 'membership'
    directory = tmp_path / 'pod' / 'container'
    directory.mkdir(parents=True)
    directory.joinpath('cpu.max').write_text('max 100000')
    directory.parent.joinpath('cpu.max').write_text('200000 100000')
    membership.write_text('0::/pod/container\n')
    monkeypatch.setattr(capacity.os, 'process_cpu_count', lambda: 16)
    assert capacity.available_cores(tmp_path, membership) == 2
    monkeypatch.setattr(capacity.os, 'process_cpu_count', lambda: 1)
    assert capacity.available_cores(tmp_path, membership) == 1
    monkeypatch.setattr(capacity.os, 'process_cpu_count', lambda: None)
    assert capacity.available_cores(tmp_path / 'missing', tmp_path / 'absent') == 1


@pytest.mark.parametrize('cores, hosts', [(3, 11), (4, 2), (2, 0)])
def test_partitions_visit_each_host_once(cores: int, hosts: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Keep process count equal to available cores, including empty host partitions.

    Args:
        cores (int): Available logical CPU count.
        hosts (int): Number of configured hosts.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring capacity detection.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setattr(runtime, 'available_cores', lambda: cores)
    config: Any = SimpleNamespace(controller=SimpleNamespace(mode='kubernetes',
                                                            autoscale=SimpleNamespace(hosts=list(range(hosts)))))
    specs = runtime.processes(config)
    assert len(specs) == cores
    assert sorted(host for spec in specs for host in spec.args[1]) == list(range(hosts))
    assert all(spec.target is worker.run and spec.required for spec in specs)
    config.controller.mode = 'standalone-external-metrics'
    assert runtime.processes(config) == []


def test_pid_increases_decreases_and_respects_limits() -> None:
    """
    Scale with throughput error while bounding each change and total concurrency.

    Returns:
        None: No value is returned.
    """
    control = ThroughputController(CollectionControl(smoothing=1), hosts=60, maximum=8, interval=60)
    assert control.target == 1
    for _ in range(20):
        previous = control.threads
        assert previous <= control.update(CollectionReport(60, 60, 240)) <= min(8, previous + 2)
    assert control.threads == 8
    for _ in range(20):
        previous = control.threads
        assert max(1, previous - 2) <= control.update(CollectionReport(60, 60, 20)) <= previous
    assert control.threads == 1


def test_pid_failures_deadband_and_small_partitions() -> None:
    """
    Freeze on failures or acceptable error and never allocate more threads than hosts.

    Returns:
        None: No value is returned.
    """
    settings = CollectionControl(initialThreads=4, targetThroughput=2, smoothing=1)
    control = ThroughputController(settings, hosts=10, maximum=8, interval=60)
    assert control.update(CollectionReport(10, 10, 5.1)) == 4
    assert control.update(CollectionReport(10, 0, 600)) == 4
    assert control.update(CollectionReport(10, 9, 600)) == 4
    assert control.integral == 0
    assert control.update(CollectionReport(0, 0, 0)) == 4
    small = ThroughputController(settings, hosts=2, maximum=8, interval=60)
    assert small.threads == 2
    assert small.update(CollectionReport(2, 2, 100)) == 2


def test_pid_tracks_changing_host_latency() -> None:
    """
    Converge near the concurrency required for a simple parallel collection workload.

    Returns:
        None: No value is returned.
    """
    control = ThroughputController(CollectionControl(), hosts=60, maximum=10, interval=60)
    for _ in range(60):
        control.update(CollectionReport(60, 60, 240 / control.threads))
    assert 3 <= control.threads <= 5
    for _ in range(60):
        control.update(CollectionReport(60, 60, 30 / control.threads))
    assert control.threads == 1


def test_pid_integral_derivative_and_smoothed_measurements() -> None:
    """
    Exercise each feedback term and damp abrupt changes in observed throughput.

    Returns:
        None: No value is returned.
    """
    integral = ThroughputController(CollectionControl(proportionalGain=0, derivativeGain=0, smoothing=1),
                                    hosts=10, maximum=10, interval=5)
    counts = [integral.update(CollectionReport(10, 10, 10)) for _ in range(4)]
    assert counts[-1] > counts[0]
    derivative = ThroughputController(CollectionControl(initialThreads=5, targetThroughput=1,
                                                        proportionalGain=0, integralGain=0,
                                                        derivativeGain=100, smoothing=1),
                                      hosts=10, maximum=10, interval=5)
    assert derivative.update(CollectionReport(10, 10, 20)) == 5
    assert derivative.update(CollectionReport(10, 10, 12.5)) == 3
    smoothed = ThroughputController(CollectionControl(smoothing=0.25), hosts=10, maximum=10, interval=5)
    smoothed.update(CollectionReport(10, 10, 10))
    smoothed.update(CollectionReport(10, 10, 2))
    assert smoothed.observed == pytest.approx(2)


def test_collection_refills_without_waiting_for_slow_peers(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Keep free threads busy, isolate failures, and respect the selected concurrency.

    Args:
        monkeypatch (pytest.MonkeyPatch): Fixture restoring the host collection function.

    Returns:
        None: No value is returned.
    """
    released = Event()
    lock = Lock()
    active = 0
    peak = 0
    visited = []
    config: Any = SimpleNamespace(controller=SimpleNamespace(databases=SimpleNamespace(hostConnectionQueueSize=2)))
    collector = MetricsCollector(config, Mock())

    def collect(host: Host) -> bool:
        """
        Hold the first host until a later host reaches the available thread.

        Args:
            host (Host): Integer host identity supplied by the test.

        Returns:
            bool: Whether this host was collected successfully.

        Raises:
            RuntimeError: For the host designated to simulate a collection failure.
        """
        nonlocal active, peak
        identity = cast('Any', host)
        with lock:
            active += 1
            peak = max(peak, active)
            visited.append(identity)
        try:
            if identity == 0:
                assert released.wait(5), 'A slow peer prevented refilling the pool'
            if identity == 2:
                released.set()
            if identity == 3:
                raise RuntimeError('Host failed')
            return identity != 4
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(collector, 'collect_host', collect)
    report = collector.collect_once(cast(list[Host], list(range(8))), workers=2)
    assert report.attempted == 8 and report.succeeded == 6
    assert sorted(visited) == list(range(8))
    assert peak == 2
    assert report.throughput > 0
    assert released.is_set()


@pytest.mark.parametrize('domains, success', [(None, False), ((), True)])
def test_empty_hosts_are_distinguished_from_collection_failure(domains: Any, success: bool,
                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Count an empty domain list as success but exhausted connection retries as failure.

    Args:
        domains (Any): Raw collection result returned by the test connection.
        success (bool): Expected successful-host result.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring hypervisor construction.

    Returns:
        None: No value is returned.
    """
    @contextmanager
    def connect(*_args: Any, **_kwargs: Any) -> Iterator[Any]:
        """
        Yield a controlled raw hypervisor collector.

        Args:
            *_args (Any): Ignored connection arguments.
            **_kwargs (Any): Ignored connection options.

        Yields:
            Any: Test connection with the configured domain result.
        """
        yield None if domains is None else Mock(request_domain_stats=Mock(return_value=domains))

    monkeypatch.setattr('premiscale.metrics.collector.build_hypervisor_connection', connect)
    fanout = Mock()
    host = Mock()
    host.name = 'host'
    config = Mock()
    config.controller.autoscale.groups = {'workers': SimpleNamespace(hosts=['host'])}
    assert MetricsCollector(config, fanout).collect_host(host) is success
    fanout.publish.assert_not_called()


def test_worker_applies_feedback_to_next_pass_and_closes_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Feed a slow pass into the worker and observe increased next-pass concurrency.

    Args:
        monkeypatch (pytest.MonkeyPatch): Fixture restoring worker resources and sleeps.

    Returns:
        None: No value is returned.
    """
    config: Any = SimpleNamespace(controller=SimpleNamespace(
        databases=SimpleNamespace(collectionInterval=5, maxHostConnectionThreads=4),
        reconciliation=SimpleNamespace(collection=CollectionControl()), broker=Mock(),
    ))
    collector = Mock()
    collector.collect_once.side_effect = [CollectionReport(4, 4, 20), SystemExit(0)]
    fanout = Mock()
    monkeypatch.setattr(worker, 'MetricsCollector', Mock(return_value=collector))
    monkeypatch.setattr(worker, 'build_fanout', Mock(return_value=fanout))
    monkeypatch.setattr(worker, 'setproctitle', lambda _title: None)
    monkeypatch.setattr(worker, 'sleep', lambda _seconds: None)
    hosts = cast(list[Host], [Mock(name=f'host-{index}') for index in range(4)])
    with pytest.raises(SystemExit):
        worker.run(config, hosts, 0)
    assert [call.args[1] for call in collector.collect_once.call_args_list] == [1, 3]
    fanout.close.assert_called_once_with()


def test_empty_worker_does_not_open_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Idle an unassigned core without touching the state store or broker.

    Args:
        monkeypatch (pytest.MonkeyPatch): Fixture restoring worker clients and sleep.

    Returns:
        None: No value is returned.
    """
    state = Mock(side_effect=AssertionError('Empty workers must not open state stores'))
    fanout = Mock(side_effect=AssertionError('Empty workers must not open brokers'))
    config = Mock()
    config.controller.databases.state.adapter = state
    monkeypatch.setattr(worker, 'build_fanout', fanout)
    monkeypatch.setattr(worker, 'setproctitle', lambda _title: None)
    monkeypatch.setattr(worker, 'sleep', Mock(side_effect=SystemExit(0)))
    with pytest.raises(SystemExit):
        worker.run(config, [], 1)
    state.assert_not_called()
    fanout.assert_not_called()
