"""
Validate deployment composition independently of infrastructure configuration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from attrs import frozen, field

from premiscale.config.databases import LocalMetrics

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


@frozen
class Execution:
    """
    Select singular, consolidated HA, or independently scaled HHA responsibilities.

    Attributes:
        mode (str): Deployment composition: singular, ha, or hha.
        role (str): Controller, collector, or publisher container role.
        publishers (tuple[str, ...]): Remote database subscribers distributed across replicas.
        publisher (str): Subscriber consumed by a dedicated publisher container.
        connections (int): Default maximum outbound database connections per subscriber and pod.
        publisher_limits (dict[str, int]): Per-subscriber connection overrides.
    """

    mode: str = 'singular'
    role: str = 'controller'
    publishers: tuple[str, ...] = ()
    publisher: str = ''
    connections: int = 2
    publisher_limits: dict[str, int] = field(factory=dict)

    @property
    def distributed(self) -> bool:
        """
        Report whether collection and publication share work across replicas.

        Returns:
            bool: True for both HA compositions.
        """
        return self.mode != 'singular'

    def validate(self, config: Config) -> None:
        """
        Reject compositions which could duplicate infrastructure or local-file writers.

        Args:
            config (Config): Parsed infrastructure and database configuration.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If composition, transport, journal, or connection bounds are invalid.
        """
        if self.mode not in {'singular', 'ha', 'hha'}:
            raise ValueError('Deployment mode must be singular, ha, or hha')
        if self.role not in {'controller', 'collector', 'publisher'} or not 1 <= self.connections <= 64:
            raise ValueError('Invalid worker role or database connection limit')
        if self.mode != 'hha' and self.role != 'controller':
            raise ValueError('Separate collector and publisher roles require hha mode')
        if self.mode == 'singular':
            if self.publishers or self.publisher_limits:
                raise ValueError('Distributed publishers require ha or hha mode')
            return
        if config.controller.mode != 'kubernetes':
            raise ValueError('HA requires kubernetes mode with internal metrics')
        if not config.controller.kafka.enabled:
            raise ValueError('HA requires Kafka for metrics publication')
        if self.role == 'controller' and not config.controller.kubernetes.stateDsn:
            raise ValueError('HA controllers require a shared PostgreSQL stateDsn')
        publishers = config.controller.databases.destinations
        selected = self.publishers if self.role == 'controller' else (() if self.role == 'collector' else (self.publisher,))
        if len(set(selected)) != len(selected):
            raise ValueError('Distributed publisher names must be unique')
        for name in selected:
            if name not in publishers or publishers[name].type not in {'postgresql', 'influxdb'}:
                raise ValueError('Scalable publishers require a configured PostgreSQL or InfluxDB subscriber')
        if any(name not in selected or isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 64
               for name, limit in self.publisher_limits.items()):
            raise ValueError('Publisher limits require selected names and connection counts from 1 to 64')
        if self.role == 'controller' and any(isinstance(item, LocalMetrics) and item.dbfile for item in publishers.values()):
            raise ValueError('HA local metrics must be ephemeral; use PostgreSQL for durable publication')
