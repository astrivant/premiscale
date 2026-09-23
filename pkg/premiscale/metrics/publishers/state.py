"""
Persist observed VM state independently of metric databases and lifecycle journals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import timezone

from premiscale.connections.journal import Journal
from ._base import Publisher

if TYPE_CHECKING:
    from premiscale.schemas.metrics import MetricBatch


class StatePublisher(Publisher):
    """
    Retain fresh VM observations and increment revisions only when state changes.
    """

    def __init__(self, path: str, cluster: str, dsn: str = '') -> None:
        """
        Describe an observation store without opening a connection in the parent process.

        Args:
            path (str): SQLite state database path, or :memory: for ephemeral state.
            cluster (str): Owning cluster and PostgreSQL schema identity.
            dsn (str): Shared PostgreSQL destination for HA; takes precedence over path.
        """
        self.path = path
        self.cluster = cluster
        self.dsn = dsn
        self.connection: Journal | None = None

    def publish(self, batch: MetricBatch, message_id: str) -> None:
        """
        Commit newer observations without authorizing VM lifecycle mutations.

        Repeated or out-of-order deliveries do not overwrite newer state. Unchanged
        observations refresh freshness while retaining their change revision.
        Missing counters retain prior known values. Absent VMs are never deleted
        implicitly: an incomplete host response cannot authorize removal.

        Args:
            batch (MetricBatch): Compiled observations; time-series fields are ignored.
            message_id (str): Transport identity; sample timestamps make writes idempotent.

        Returns:
            None: No value is returned after the complete batch commits.

        Raises:
            ValueError: If an observation belongs to another cluster.
            BaseException: If database initialization fails after opening its session.
        """
        if not batch.observations:
            return
        if any(item.domain.cluster != self.cluster for item in batch.observations):
            raise ValueError('VM observation belongs to a different cluster')
        if self.connection is None:
            connection = Journal(self.path, self.cluster, self.dsn)
            try:
                with connection.transaction():
                    connection.execute('observations/create.sql')
            except BaseException:
                connection.close()
                raise
            self.connection = connection
        with self.connection.transaction():
            for observation in batch.observations:
                domain = observation.domain
                timestamp = observation.time.astimezone(timezone.utc).isoformat(timespec='microseconds')
                self.connection.execute('observations/upsert.sql', (
                    domain.cluster, domain.id, domain.name, domain.group, domain.host, domain.address,
                    observation.state, observation.reason, observation.vcpus, observation.memory_bytes,
                    observation.storage_bytes, timestamp, timestamp,
                ))

    def close(self) -> None:
        """
        Release a failed or terminating session so queued deliveries can reconnect.

        Returns:
            None: No value is returned.
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
