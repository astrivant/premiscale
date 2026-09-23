"""
Publish metrics to PostgreSQL, including CloudNativePG-managed databases.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import psycopg
from psycopg.types.json import Jsonb

from premiscale.support.sql import load_sql
from ._base import Publisher

if TYPE_CHECKING:
    from premiscale.config.databases import PostgreSQLMetrics
    from premiscale.schemas.metrics import MetricBatch


class PostgreSQLPublisher(Publisher):
    """
    Commit each batch transactionally and deduplicate redeliveries by sample identity.
    """

    def __init__(self, config: PostgreSQLMetrics) -> None:
        """
        Store connection settings without opening a database connection.

        Args:
            config (PostgreSQLMetrics): DSN, retention, and bounded database timeouts.
        """
        self.config = config
        self.connection: psycopg.Connection | None = None

    def publish(self, batch: MetricBatch, message_id: str) -> None:
        """
        Insert metrics and apply retention in a single committed transaction.

        Args:
            batch (MetricBatch): Reduced measurements to store as numeric JSON fields.
            message_id (str): Stable UUID used with each sample index as the primary key.

        Returns:
            None: No value is returned after the transaction commits.
        """
        if self.connection is None or self.connection.closed:
            self.connection = psycopg.connect(
                self.config.dsn, autocommit=True, connect_timeout=self.config.connectTimeout,
                options=f'-c statement_timeout={self.config.statementTimeout}',
            )
            with self.connection.transaction():
                self.connection.execute(load_sql('metrics/lock_schema.sql'))
                self.connection.execute(load_sql('metrics/create_metrics.sql'))
                self.connection.execute(load_sql('metrics/create_time_index.sql'))
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.executemany(load_sql('metrics/insert_metric.sql'), [
                (message_id, index, metric.measurement, metric.time, Jsonb(metric.tags), Jsonb(metric.fields))
                for index, metric in enumerate(batch.metrics)
            ])
            cursor.execute(load_sql('metrics/prune_metrics.sql'), (self.config.retention,))

    def close(self) -> None:
        """
        Close the connection so the next delivery reconnects after failure.

        Returns:
            None: No value is returned.
        """
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None
