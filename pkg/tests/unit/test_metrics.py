"""
Check local time-series metric conversion without connecting to a host.
"""

from ipaddress import IPv4Address
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone
from typing import cast
from unittest.mock import Mock

from cattrs import unstructure
import libvirt
import pytest

from premiscale.hypervisor.qemu import Qemu
from premiscale.metrics.reduction import compile_domain
from premiscale.schemas.qemu import DomainState, HostInfo, HostResourceStats, HostStats, ManagedDomain, RawDomainStats


@pytest.fixture
def qemu_host() -> tuple[Qemu, Mock]:
    """
    Provide a host collector backed by a mock libvirt connection.

    Returns:
        tuple[Qemu, Mock]: QEMU collector and its connection, with no network access.
    """
    connection = Mock(spec=libvirt.virConnect)
    connection.getHostname.return_value = 'test-host'
    connection.getType.return_value = 'QEMU'
    connection.getURI.return_value = 'qemu:///system'
    connection.getVersion.return_value = 8000000
    connection.getLibVersion.return_value = 11008000
    connection.getCapabilities.return_value = '<capabilities><host><cpu><arch>x86_64</arch></cpu></host></capabilities>'
    connection.getInfo.return_value = ['x86_64', 8192, 4, 2000, 1, 1, 2, 2]
    connection.getMaxVcpus.return_value = 255
    connection.getFreeMemory.return_value = 1048576
    connection.getMemoryStats.return_value = {'total': 8388608, 'free': 1024}
    connection.getCPUStats.return_value = {'kernel': 100, 'user': 200, 'idle': 300}
    connection.listAllDomains.return_value = []
    host = Qemu(name='test-host', address=IPv4Address('192.0.2.1'), port=22, protocol='ssh')
    host._connection = cast(libvirt.virConnect, connection)
    return host, connection


@pytest.mark.parametrize('states', [[], [libvirt.VIR_DOMAIN_RUNNING, libvirt.VIR_DOMAIN_SHUTOFF]])
def test_host_stats_preserve_serialized_snapshot(qemu_host: tuple[Qemu, Mock], states: list[int]) -> None:
    """
    Preserve host data and active or inactive domain states through attrs serialization.

    Args:
        qemu_host (tuple[Qemu, Mock]): Host collector and mock libvirt connection.
        states (list[int]): Libvirt domain states to include in the snapshot.

    Returns:
        None: No value is returned.
    """
    host, connection = qemu_host
    domains = []
    for index, state in enumerate(states):
        domain = Mock(spec=libvirt.virDomain)
        domain.name.return_value = f'vm-{index}'
        domain.info.return_value = [state, 4096, 2048, 2, 1000]
        domains.append(domain)
    connection.listAllDomains.return_value = domains

    snapshot = host.collect_host_stats()

    assert isinstance(snapshot, HostStats)
    assert isinstance(snapshot.host, HostInfo)
    assert isinstance(snapshot.host.stats, HostResourceStats)
    assert all(isinstance(domain, DomainState) for domain in snapshot.vms)
    assert unstructure(snapshot) == {
        'host': {
            'name': 'test-host',
            'type': 'QEMU',
            'uri': 'qemu:///system',
            'version': 8000000,
            'libvirt_version': 11008000,
            'capabilities': {'capabilities': {'host': {'cpu': {'arch': 'x86_64'}}}},
            'node_info': ['x86_64', 8192, 4, 2000, 1, 1, 2, 2],
            'max_vcpus': 255,
            'free_memory': 1048576,
            'node_memory': {'total': 8388608, 'free': 1024},
            'node_cpu_stats': {'kernel': 100, 'user': 200, 'idle': 300},
            'stats': {
                'cpu': {'kernel': 100, 'user': 200, 'idle': 300},
                'memory': {'total': 8388608, 'free': 1024},
            },
        },
        'vms': [{'name': f'vm-{index}', 'state': [state, 4096, 2048, 2, 1000]} for index, state in enumerate(states)],
    }
    connection.listAllDomains.assert_called_once_with(flags=libvirt.VIR_DOMAIN_NOSTATE)


def test_host_stats_return_none_when_reconnection_fails(qemu_host: tuple[Qemu, Mock], monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Return no snapshot when the libvirt retry wrapper cannot restore the connection.

    Args:
        qemu_host (tuple[Qemu, Mock]): Host collector and mock libvirt connection.
        monkeypatch (pytest.MonkeyPatch): Fixture for isolating reconnection attempts.

    Returns:
        None: No value is returned.
    """
    host, connection = qemu_host
    host._connection = None
    reconnect = Mock(return_value=None)
    monkeypatch.setattr(host, 'open', reconnect)

    assert host.collect_host_stats() is None
    assert reconnect.call_count == 3
    connection.getHostname.assert_not_called()


@pytest.fixture
def managed_domain() -> Mock:
    """
    Provide the real ownership metadata format without contacting libvirt.

    Returns:
        Mock: Libvirt domain with a verified stable identity and XML fixture.
    """
    domain = Mock(spec=libvirt.virDomain)
    domain.name.return_value = 'worker'
    domain.UUIDString.return_value = '11111111-1111-4111-8111-111111111111'
    domain.XMLDesc.return_value = (Path(__file__).parents[1] / 'data/metrics/managed-domain.xml').read_text()
    return domain


def test_request_is_targeted_raw_and_has_no_storage_conversion(qemu_host: tuple[Qemu, Mock], managed_domain: Mock) -> None:
    """
    Verify ownership before issuing one targeted request and retain the original counter keys.

    Args:
        qemu_host (tuple[Qemu, Mock]): Connected collector and mock transport.
        managed_domain (Mock): Owned VM metadata fixture.

    Returns:
        None: No value is returned.
    """
    host, connection = qemu_host
    unrelated = Mock(spec=libvirt.virDomain)
    unrelated.XMLDesc.return_value = managed_domain.XMLDesc.return_value.replace('cluster="cluster"', 'cluster="other"')
    connection.listAllDomains.return_value = [managed_domain, unrelated]
    counters = {'state.state': 5, 'state.reason': 1, 'cpu.time': 100, 'balloon.maximum': 4096}
    original = deepcopy(counters)
    connection.domainListGetStats.return_value = [(managed_domain, counters)]
    samples = host.request_domain_stats('cluster', frozenset({'workers'}))
    assert samples[0].counters == counters == original
    assert samples[0].counters is not counters
    assert samples[0].domain.host == 'test-host'
    assert samples[0].domain.address == '192.0.2.10'
    assert samples[0].time.utcoffset() is not None
    assert connection.domainListGetStats.call_args.args == ([managed_domain],)
    assert connection.domainListGetStats.call_args.kwargs['flags'] == 0
    connection.getAllDomainStats.assert_not_called()
    compiled = compile_domain(samples[0])
    assert compiled.observations[0].state == 5
    assert compiled.observations[0].memory_bytes == 4096 * 1024
    assert compiled.metrics[0].fields == {'cpu_time_ns': 100}
    assert counters == original


@pytest.mark.parametrize('cluster, groups', [('other', {'workers'}), ('cluster', {'elsewhere'}), ('cluster', set())])
def test_unmanaged_inventory_never_requests_statistics(qemu_host: tuple[Qemu, Mock], managed_domain: Mock,
                                                       cluster: str, groups: set[str]) -> None:
    """
    Avoid bulk metric requests when no domain belongs to this cluster and host assignment.

    Args:
        qemu_host (tuple[Qemu, Mock]): Connected collector and mock transport.
        managed_domain (Mock): Owned VM metadata fixture.
        cluster (str): Cluster to select.
        groups (set[str]): Groups assigned to this collector's host.

    Returns:
        None: No value is returned.
    """
    host, connection = qemu_host
    connection.listAllDomains.return_value = [managed_domain]
    assert host.request_domain_stats(cluster, frozenset(groups)) == ()
    connection.domainListGetStats.assert_not_called()
    connection.getAllDomainStats.assert_not_called()


def test_conflicting_ownership_and_disconnection_fail_explicitly(qemu_host: tuple[Qemu, Mock], managed_domain: Mock) -> None:
    """
    Distinguish invalid ownership or a failed host from a valid empty inventory.

    Args:
        qemu_host (tuple[Qemu, Mock]): Connected collector and mock transport.
        managed_domain (Mock): Owned VM metadata fixture.

    Returns:
        None: No value is returned.
    """
    host, connection = qemu_host
    connection.listAllDomains.return_value = [managed_domain]
    managed_domain.UUIDString.return_value = '22222222-2222-4222-8222-222222222222'
    with pytest.raises(ValueError, match='ownership'):
        host.request_domain_stats('cluster', frozenset({'workers'}))
    connection.domainListGetStats.assert_not_called()
    host._connection = None
    with pytest.raises(ConnectionError):
        host.request_domain_stats('cluster', frozenset({'workers'}))


def test_sparse_and_partial_counters_compile_without_fabricated_utilization() -> None:
    """
    Preserve units and sparse interfaces while omitting unsupported guest counters.

    Returns:
        None: No value is returned.
    """
    counters: dict[str, int | str] = {'cpu.time': 2 ** 55 + 1, 'cpu.user': -1, 'balloon.current': 2048,
                'net.7.name': 'vnet7', 'net.7.rx.bytes': 91, 'net.13.name': 'vnet13',
                'net.13.tx.bytes': 42, 'block.9.name': 'vda', 'block.9.capacity': 8192,
                'block.9.allocation': 1024, 'some.future.counter': 9}
    sample = RawDomainStats(ManagedDomain('11111111-1111-4111-8111-111111111111', 'worker', 'cluster', 'workers', 'host'),
                            datetime.now(timezone.utc), deepcopy(counters))
    batch = compile_domain(sample)
    assert batch.observations[0].storage_bytes == 8192
    assert batch.observations[0].state is None
    assert next(metric for metric in batch.metrics if metric.measurement == 'cpu').fields == {'cpu_time_ns': 2 ** 55 + 1}
    assert next(metric for metric in batch.metrics if metric.measurement == 'memory').fields == {'current_bytes': 2048 * 1024}
    interfaces = {metric.tags['interface']: metric.fields for metric in batch.metrics if metric.measurement == 'net'}
    assert interfaces == {'vnet7': {'rx_bytes': 91}, 'vnet13': {'tx_bytes': 42}}
    assert sample.counters == counters
