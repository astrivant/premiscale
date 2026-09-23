"""
Encode and validate metrics at the JSON broker boundary.
"""

from __future__ import annotations

from datetime import datetime
from math import isfinite
from typing import TYPE_CHECKING
from uuid import UUID

from attrs import asdict

from premiscale.schemas.metrics import Metric, MetricBatch
from premiscale.schemas.observations import DomainObservation
from premiscale.schemas.qemu import ManagedDomain

if TYPE_CHECKING:
    from typing import Any


OBSERVATION_FIELDS = ('state', 'reason', 'vcpus', 'memory_bytes', 'storage_bytes')
OBSERVATION_MEASUREMENT = 'premiscale_vm_observation'


def encode_batch(batch: MetricBatch) -> dict[str, Any]:
    """
    Preserve numeric counters and timestamps in a JSON-compatible batch.

    Args:
        batch (MetricBatch): Reduced measurements to publish.

    Returns:
        dict[str, Any]: Validated metric payload without the transport envelope.
    """
    payload = {'metrics': [
        {'measurement': metric.measurement, 'time': metric.time.isoformat(),
         'tags': metric.tags, 'fields': metric.fields} for metric in batch.metrics
    ]}
    # Preserve the existing metrics-only wire shape during mixed-version rollouts.
    # Older subscribers can still publish the ordinary measurements in this batch.
    payload['metrics'].extend({
        'measurement': OBSERVATION_MEASUREMENT, 'time': item.time.isoformat(), 'tags': asdict(item.domain),
        'fields': {'version': 1, **{key: getattr(item, key) for key in OBSERVATION_FIELDS if getattr(item, key) is not None}},
    } for item in batch.observations)
    decode_batch(payload)
    return payload


def decode_batch(payload: Any) -> MetricBatch:
    """
    Reject malformed metrics before any publisher receives a delivery.

    Args:
        payload (Any): Decoded JSON payload supplied by the broker.

    Returns:
        MetricBatch: Measurements with restored timezone-aware timestamps.

    Raises:
        ValueError: If keys, values, numeric fields, or timestamps are invalid.
    """
    if not isinstance(payload, dict) or set(payload) != {'metrics'} or not isinstance(payload['metrics'], list):
        raise ValueError('Invalid metrics batch')
    metrics = []
    observations = []
    for datum in payload['metrics']:
        if not isinstance(datum, dict) or set(datum) != {'measurement', 'time', 'tags', 'fields'}:
            raise ValueError('Invalid metric keys')
        if not isinstance(datum['measurement'], str) or not datum['measurement']:
            raise ValueError('A metric requires a measurement name')
        if not isinstance(datum['time'], str):
            raise ValueError('Metric timestamp must be an ISO string')
        timestamp = datetime.fromisoformat(datum['time'])
        if timestamp.utcoffset() is None:
            raise ValueError('Metric timestamp must include its timezone')
        tags, fields = datum['tags'], datum['fields']
        if not isinstance(tags, dict) or any(not isinstance(key, str) or not isinstance(value, str)
                                             for key, value in tags.items()):
            raise ValueError('Metric tags must map strings to strings')
        if any(key.startswith('_premiscale_') for key in tags):
            raise ValueError('Metric tags may not use reserved publisher identifiers')
        if not isinstance(fields, dict) or not fields or any(
                not isinstance(key, str) or type(value) not in (int, float)
                or (isinstance(value, float) and not isfinite(value)) for key, value in fields.items()):
            raise ValueError('Metric fields must contain finite numbers')
        if datum['measurement'] == OBSERVATION_MEASUREMENT:
            if type(fields.get('version')) is not int or fields['version'] != 1 or not set(fields) <= {'version', *OBSERVATION_FIELDS}:
                raise ValueError('Invalid VM observation version or fields')
            observations.append(_decode_observation({
                'domain': tags, 'time': datum['time'], **{key: fields.get(key) for key in OBSERVATION_FIELDS},
            }))
        else:
            metrics.append(Metric(datum['measurement'], timestamp, dict(tags), dict(fields)))
    return MetricBatch(tuple(metrics), tuple(observations))


def _decode_observation(item: Any) -> DomainObservation:
    """
    Validate VM identity, units, and timestamps before delivering state to storage.

    Args:
        item (Any): Untrusted JSON observation.

    Returns:
        DomainObservation: Typed observation retaining the original acquisition time.

    Raises:
        ValueError: If identity, state, resource values, or timestamp are malformed.
    """
    if not isinstance(item, dict) or set(item) != {'domain', 'time', *OBSERVATION_FIELDS}:
        raise ValueError('Invalid VM observation fields')
    domain = item['domain']
    if not isinstance(domain, dict) or set(domain) != {'id', 'name', 'cluster', 'group', 'host', 'address'}:
        raise ValueError('Invalid managed VM identity')
    if any(not isinstance(value, str) or (not value and key != 'address') for key, value in domain.items()):
        raise ValueError('VM identity fields must be nonempty strings')
    UUID(domain['id'])
    if not isinstance(item['time'], str):
        raise ValueError('VM observation timestamp must be an ISO string')
    timestamp = datetime.fromisoformat(item['time'])
    if timestamp.utcoffset() is None:
        raise ValueError('VM observation timestamp requires a timezone')
    for key in OBSERVATION_FIELDS:
        if item[key] is not None and (type(item[key]) is not int or item[key] < 0):
            raise ValueError('Observed VM fields must be nonnegative integers or null')
    return DomainObservation(ManagedDomain(**domain), timestamp, **{key: item[key] for key in OBSERVATION_FIELDS})
