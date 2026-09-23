"""
Translate reduced metrics into TinyFlux points inside the publisher process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from ._base import Publisher

if TYPE_CHECKING:
    from premiscale.config.databases import LocalMetrics
    from premiscale.schemas.metrics import MetricBatch


class LocalPublisher(Publisher):
    """
    Keep a single TinyFlux writer and skip samples already written by a retry.
    """

    def __init__(self, config: LocalMetrics) -> None:
        """
        Prepare storage without performing IO in the parent process.

        Args:
            config (LocalMetrics): CSV path and sample retention settings.
        """
        self.store = config.adapter()
        self.connected = False

    def publish(self, batch: MetricBatch, message_id: str) -> None:
        """
        Convert metrics to TinyFlux records and deduplicate each sample.

        Args:
            batch (MetricBatch): Reduced measurements to write.
            message_id (str): Stable batch UUID retained across broker redelivery.

        Returns:
            None: No value is returned after the database write completes.
        """
        if not self.connected:
            self.store.open()
            self.connected = True
        points = []
        for index, metric in enumerate(batch.metrics):
            sample_id = f'{message_id}:{index}'
            if not self.store.contains_sample(sample_id):
                points.append({'measurement': metric.measurement, 'time': metric.time,
                               'tags': {**metric.tags, '_premiscale_sample': sample_id},
                               'fields': dict(metric.fields)})
        self.store.insert_batch(tuple(points))

    def close(self) -> None:
        """
        Close the local store and allow the next delivery to reconnect.

        Returns:
            None: No value is returned.
        """
        if self.connected:
            try:
                self.store.close()
            finally:
                self.connected = False
