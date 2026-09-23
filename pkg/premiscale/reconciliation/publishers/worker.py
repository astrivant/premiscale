"""
Publish one named remote subscriber without opening controller state or VM journals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.metrics.fanout import metrics_queue, subscriber_queue
from premiscale.metrics.publisher import MetricsPublisher
from premiscale.status.store import ready
from ..activity import Activity

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config, subscriber: str) -> None:
    """
    Own at most one database connection and consume independently leased deliveries.

    Args:
        config (Config): Broker and database connection settings.
        subscriber (str): Validated remote subscriber name.

    Returns:
        None: No value is returned before termination.
    """
    with subscriber_queue(config, subscriber) as queue, \
            metrics_queue(config.controller.broker, subscriber) as observations, Activity(observations) as activity:
        publisher = MetricsPublisher(queue, config.controller.databases.destinations[subscriber].publisher(), activity)
        ready()
        publisher()
