"""
Own the observation subscriber in the singular or elected reconciliation process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.config.databases import SQLiteState
from premiscale.metrics.fanout import subscriber_queue
from premiscale.metrics.publisher import MetricsPublisher
from premiscale.metrics.publishers.state import StatePublisher
from premiscale.status.store import ready

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config) -> None:
    """
    Persist observed state with one writer independently of metric destinations.

    HA uses the configured shared PostgreSQL journal database but writes a separate
    observations table. Singular mode uses the configured SQLite state database.

    Args:
        config (Config): State database, cluster identity, and metrics transport settings.

    Returns:
        None: No value is returned before shutdown.

    Raises:
        NotImplementedError: If the legacy unimplemented MySQL state backend is selected.
    """
    state = config.controller.databases.state
    kubernetes = config.controller.kubernetes
    if not isinstance(state, SQLiteState) and not kubernetes.stateDsn:
        raise NotImplementedError('VM observations require SQLite state or PostgreSQL journals')
    path = state.dbfile if isinstance(state, SQLiteState) else None
    publisher = StatePublisher(path or ':memory:', kubernetes.clusterName, kubernetes.stateDsn)
    consumer = MetricsPublisher(subscriber_queue(config, '_state'), publisher)
    ready()
    consumer()
