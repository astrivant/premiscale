"""
Serialize numeric metrics into InfluxDB line protocol at the publication boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import datetime, timezone
from math import isfinite


if TYPE_CHECKING:
    from premiscale.schemas.metrics import MetricBatch


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _escape(value: str, characters: str) -> str:
    """
    Escape line-protocol identifiers without permitting record injection.

    Args:
        value (str): Measurement, field, tag name, or tag value.
        characters (str): Separators requiring backslash escaping.

    Returns:
        str: Escaped single-line identifier.

    Raises:
        ValueError: If an identifier is empty or contains a line separator.
    """
    if not value or '\n' in value or '\r' in value:
        raise ValueError('InfluxDB identifiers must be nonempty single-line strings')
    return ''.join('\\' + character if character in characters + '\\' else character for character in value)


def encode_lines(batch: MetricBatch) -> bytes:
    """
    Encode exact counters and original timestamps so retries overwrite identical points.

    Args:
        batch (MetricBatch): Numeric measurements; state observations are not time-series fields.

    Returns:
        bytes: UTF-8 line protocol with nanosecond timestamps derived without floating-point conversion.

    Raises:
        ValueError: If timestamps, fields, or identifiers cannot be represented safely.
    """
    lines = []
    for metric in batch.metrics:
        if metric.time.utcoffset() is None:
            raise ValueError('InfluxDB timestamps must include a timezone')
        series = _escape(metric.measurement, ' ,')
        for key, tag_value in sorted(metric.tags.items()):
            series += f',{_escape(key, " ,=")}={_escape(tag_value, " ,=")}'
        fields: list[str] = []
        for key, value in sorted(metric.fields.items()):
            if type(value) is int and -(2 ** 63) <= value < 2 ** 63:
                encoded = f'{value}i'
            elif type(value) is float and isfinite(value):
                encoded = repr(value)
            else:
                raise ValueError('InfluxDB fields require signed 64-bit integers or finite floats')
            fields.append(f'{_escape(key, " ,=")}={encoded}')
        if not fields:
            raise ValueError('InfluxDB points require at least one field')
        elapsed = metric.time.astimezone(timezone.utc) - EPOCH
        nanos = ((elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds) * 1000
        lines.append(f'{series} {",".join(fields)} {nanos}')
    return '\n'.join(lines).encode('utf-8')
