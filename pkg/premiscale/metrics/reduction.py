"""
Compile raw samples into relevant VM observations and database-independent metrics.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from premiscale.schemas.metrics import Metric, MetricBatch
from premiscale.schemas.observations import DomainObservation

if TYPE_CHECKING:
    from typing import Any
    from premiscale.schemas.qemu import RawDomainStats


DEVICE = re.compile(r'^(net|block)\.(\d+)\.(.+)$')


def _counter(values: dict[str, Any], name: str, multiplier: int = 1) -> int | None:
    """
    Read a supported nonnegative counter without inventing values for absent fields.

    Args:
        values (dict[str, Any]): Raw counters from a domain or device.
        name (str): Libvirt counter name.
        multiplier (int): Unit conversion applied after validating the integer.

    Returns:
        int | None: Converted counter, or None for missing or unsupported values.

    Raises:
        ValueError: If a present counter is not an integer.
    """
    value = values.get(name)
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError(f'Invalid libvirt counter: {name}')
    return value * multiplier if value >= 0 else None


def compile_domain(sample: RawDomainStats) -> MetricBatch:
    """
    Reduce one acquired sample without a live connection or database dependencies.

    CPU time and IO remain cumulative counters with explicit units. Utilization
    requires successive samples; configured memory is not reported as consumed
    memory. Sparse device indices and unsupported counters are preserved safely.

    Args:
        sample (RawDomainStats): Verified identity and original libvirt response.

    Returns:
        MetricBatch: Relevant observations and numeric measurements for independent subscribers.

    Raises:
        ValueError: If the timestamp or a present numeric counter is invalid.
    """
    if sample.time.utcoffset() is None:
        raise ValueError('Domain statistics require a timezone-aware acquisition timestamp')
    domain, counters = sample.domain, sample.counters
    tags = {'id': domain.id, 'name': domain.name, 'host': domain.host,
            'cluster': domain.cluster, 'group': domain.group}
    metrics = []
    families = {
        'cpu': {'cpu.time': ('cpu_time_ns', 1), 'cpu.user': ('cpu_user_ns', 1),
                'cpu.system': ('cpu_system_ns', 1), 'vcpu.current': ('vcpu_current', 1),
                'vcpu.maximum': ('vcpu_maximum', 1)},
        'memory': {f'balloon.{name}': (f'{name}_bytes', 1024)
                   for name in ('current', 'maximum', 'rss', 'unused', 'available', 'usable', 'disk_caches')},
        'state': {'state.state': ('state', 1), 'state.reason': ('reason', 1)},
    }
    for family, names in families.items():
        fields: dict[str, int | float] = {}
        for source, (target, factor) in names.items():
            value = _counter(counters, source, factor)
            if value is not None:
                fields[target] = value
        if family == 'memory':
            available, unused = fields.get('available_bytes'), fields.get('unused_bytes')
            if available and unused is not None and 0 <= unused <= available:
                fields['used_bytes'] = available - unused
                fields['used_percent'] = round((available - unused) / available * 100, 2)
        if fields:
            metrics.append(Metric(family, sample.time, tags.copy(), fields))
    devices: dict[tuple[str, int], dict[str, Any]] = {}
    for key, raw_value in counters.items():
        match = DEVICE.fullmatch(key)
        if match:
            devices.setdefault((match[1], int(match[2])), {})[match[3]] = raw_value
    capacities = []
    for (family, index), device in sorted(devices.items()):
        if family == 'block' and (_counter(device, 'backingIndex') or 0) != 0:
            continue
        label = str(device.get('name', index))
        device_names = ({'rx.bytes': 'rx_bytes', 'tx.bytes': 'tx_bytes', 'rx.pkts': 'rx_packets',
                  'tx.pkts': 'tx_packets', 'rx.errs': 'rx_errors', 'tx.errs': 'tx_errors',
                  'rx.drop': 'rx_drops', 'tx.drop': 'tx_drops'} if family == 'net' else
                 {'rd.bytes': 'read_bytes', 'wr.bytes': 'write_bytes', 'rd.reqs': 'read_requests',
                  'wr.reqs': 'write_requests', 'rd.times': 'read_time_ns', 'wr.times': 'write_time_ns',
                  'allocation': 'allocation_bytes', 'capacity': 'capacity_bytes', 'physical': 'physical_bytes'})
        fields = {}
        for source, target in device_names.items():
            value = _counter(device, source)
            if value is not None:
                fields[target] = value
        if family == 'block':
            capacities.append(_counter(device, 'capacity'))
        if fields:
            metrics.append(Metric(family, sample.time, {**tags, 'interface' if family == 'net' else 'device': label}, fields))
    storage = sum(value for value in capacities if value is not None) if capacities and all(value is not None for value in capacities) else None
    if _counter(counters, 'block.count') == 0:
        storage = 0
    observation = DomainObservation(domain, sample.time, _counter(counters, 'state.state'),
                                    _counter(counters, 'state.reason'), _counter(counters, 'vcpu.current'),
                                    _counter(counters, 'balloon.maximum', 1024), storage)
    return MetricBatch(tuple(metrics), (observation,))


def compile_domains(samples: tuple[RawDomainStats, ...]) -> MetricBatch:
    """
    Compile a host's samples after releasing its hypervisor connection.

    Args:
        samples (tuple[RawDomainStats, ...]): Unmodified raw samples from one acquisition.

    Returns:
        MetricBatch: State observations and measurements sharing their acquisition timestamps.
    """
    batches = [compile_domain(sample) for sample in samples]
    return MetricBatch(tuple(metric for batch in batches for metric in batch.metrics),
                       tuple(observation for batch in batches for observation in batch.observations))
