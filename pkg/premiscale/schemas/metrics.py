"""
Define database-independent measurements passed from reducers to publishers.
"""

from datetime import datetime

from attrs import define

from .observations import DomainObservation


@define(frozen=True)
class Metric:
    """
    Carry one timestamped measurement without database-specific point objects.

    Attributes:
        measurement (str): Measurement family, such as cpu, memory, net, or block.
        time (datetime): Timezone-aware collection timestamp.
        tags (dict[str, str]): Resource identifiers and other searchable dimensions.
        fields (dict[str, int | float]): Numeric counters or reduced measurements.
    """

    measurement: str
    time: datetime
    tags: dict[str, str]
    fields: dict[str, int | float]


@define(frozen=True)
class MetricBatch:
    """
    Deliver a collection of reduced measurements together to each subscriber.

    Attributes:
        metrics (tuple[Metric, ...]): Measurements from one host collection pass.
        observations (tuple[DomainObservation, ...]): VM state delivered to the independent state subscriber.
    """

    metrics: tuple[Metric, ...]
    observations: tuple[DomainObservation, ...] = ()
