"""
Create one local database publisher and subscriber connection inside its child process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.status.store import ready

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.config.databases import LocalMetrics


def run(config: Config, name: str, settings: LocalMetrics) -> None:
    """
    Persist queued metrics independently of other destinations and collection.

    Args:
        config (Config): Controller broker settings.
        name (str): Stable subscriber name used to recover its queued deliveries.
        settings (LocalMetrics): Database adapter configuration for this subscriber.

    Returns:
        None: No value is returned before process shutdown.
    """
    from premiscale.metrics.fanout import subscriber_queue
    from premiscale.metrics.publisher import MetricsPublisher

    publisher = MetricsPublisher(subscriber_queue(config, name), settings.publisher())
    ready()
    publisher()
