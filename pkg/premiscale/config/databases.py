"""
Describe database backends with attrs and construct process-local adapters lazily.
"""

from datetime import timedelta
import logging
import os
from pathlib import Path
import re
# cattrs needs Literal at runtime to select the configured attrs backend.
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from attrs import define, field, validators

if TYPE_CHECKING:
    from premiscale.metrics.state.local import Local as SQLiteAdapter
    from premiscale.metrics.state.mysql import MySQL as MySQLAdapter
    from premiscale.metrics.timeseries.local import Local as LocalMetricsAdapter
    from premiscale.metrics.publishers.local import LocalPublisher
    from premiscale.metrics.publishers.postgresql import PostgreSQLPublisher
    from premiscale.metrics.publishers.influxdb import InfluxDBPublisher


log = logging.getLogger(__name__)


def _path(value: str | None) -> str | None:
    """
    Resolve environment references and user directories in an optional database path.

    Args:
        value (str | None): Configured database path or in-memory selection.

    Returns:
        str | None: Expanded path, preserving None for process-local storage.
    """
    return os.path.expanduser(os.path.expandvars(value)) if value is not None else None


def _environment(value: str) -> str:
    """
    Resolve environment references without opening a connection.

    Args:
        value (str): Connection setting that may refer to environment variables.

    Returns:
        str: Expanded connection setting.
    """
    return os.path.expandvars(value)


@define
class DatabaseCredentials:
    """
    Hold remote database credentials resolved from the process environment.

    Attributes:
        username (str): Database authentication username.
        password (str): Database password, excluded from representations.
    """

    username: str = field(converter=_environment)
    password: str = field(converter=_environment, repr=False)


@define
class Connection:
    """
    Describe the existing nested MySQL connection configuration.

    Attributes:
        url (str): Database server address.
        database (str): Database name.
        credentials (DatabaseCredentials): Resolved authentication settings.
    """

    url: str = field(converter=_environment)
    database: str = field(converter=_environment)
    credentials: DatabaseCredentials


@define(kw_only=True)
class SQLiteState:
    """
    Configure the SQLite state adapter without holding a live connection.

    Attributes:
        type (Literal['memory']): Existing YAML discriminator for SQLite state.
        dbfile (str | None): Persistent SQLite file or in-memory storage.
    """

    type: Literal['memory'] = field(default='memory', validator=validators.in_(('memory',)))
    dbfile: str | None = field(default=None, converter=_path)

    def adapter(self) -> 'SQLiteAdapter':
        """
        Create an unopened state adapter for the calling process.

        Returns:
            SQLiteAdapter: Fresh adapter opened by its context manager.
        """
        from premiscale.metrics.state.local import Local

        return Local(dbfile=self.dbfile)


@define(kw_only=True)
class MySQLState:
    """
    Configure the MySQL state adapter from nested connection settings.

    Attributes:
        type (Literal['mysql']): Existing YAML discriminator for MySQL state.
        connection (Connection): Required server, database, and credentials.
    """

    type: Literal['mysql'] = field(default='mysql', validator=validators.in_(('mysql',)))
    connection: Connection

    def adapter(self) -> 'MySQLAdapter':
        """
        Pass the configured credentials explicitly to a fresh MySQL adapter.

        Returns:
            MySQLAdapter: Unopened adapter; MySQL state operations remain unimplemented.
        """
        from premiscale.metrics.state.mysql import MySQL

        return MySQL(url=self.connection.url, database=self.connection.database,
                     username=self.connection.credentials.username, password=self.connection.credentials.password)


@define(kw_only=True)
class LocalMetrics:
    """
    Configure TinyFlux reads and publication from the same settings object.

    Attributes:
        type (Literal['memory']): Existing YAML discriminator for local time-series data.
        retention (int): Maximum sample age in seconds, at least five minutes.
        dbfile (str | None): CSV destination or process-local in-memory storage.
    """

    type: Literal['memory'] = field(default='memory', validator=validators.in_(('memory',)))
    retention: int = field(default=300, validator=validators.ge(300))
    dbfile: str | None = field(default=None, converter=_path)

    def adapter(self) -> 'LocalMetricsAdapter':
        """
        Create an unopened time-series read adapter for reconciliation.

        Returns:
            LocalMetricsAdapter: Fresh TinyFlux adapter using this retention and path.
        """
        from premiscale.metrics.timeseries.local import Local

        return Local(retention=timedelta(seconds=self.retention), file=self.dbfile)

    def publisher(self) -> 'LocalPublisher':
        """
        Create a publisher which opens its own adapter on the first delivery.

        Returns:
            LocalPublisher: Fresh publisher using the same settings as the read adapter.
        """
        from premiscale.metrics.publishers.local import LocalPublisher

        return LocalPublisher(self)


@define(kw_only=True)
class PostgreSQLMetrics:
    """
    Configure independent PostgreSQL metrics publication.

    Attributes:
        type (Literal['postgresql']): Existing YAML discriminator for PostgreSQL publication.
        dsn (str): Required connection string, expanded and excluded from representations.
        retention (int): Maximum sample age in seconds, at least five minutes.
        connectTimeout (int): Positive connection timeout in seconds.
        statementTimeout (int): Positive statement timeout in milliseconds.
    """

    type: Literal['postgresql'] = field(default='postgresql', validator=validators.in_(('postgresql',)))
    dsn: str = field(converter=_environment, repr=False, validator=validators.min_len(1))
    retention: int = field(default=300, validator=validators.ge(300))
    connectTimeout: int = field(default=5, validator=validators.ge(1))
    statementTimeout: int = field(default=10000, validator=validators.ge(1))

    def publisher(self) -> 'PostgreSQLPublisher':
        """
        Create a publisher which connects only when handling a delivery.

        Returns:
            PostgreSQLPublisher: Fresh publisher holding only these serializable settings.
        """
        from premiscale.metrics.publishers.postgresql import PostgreSQLPublisher

        return PostgreSQLPublisher(self)


@define(kw_only=True)
class InfluxDBMetrics:
    """
    Configure an optional InfluxDB 2.x destination with process-local HTTP publication.

    Attributes:
        type (Literal['influxdb']): Named publisher discriminator.
        url (str): HTTP origin and optional reverse-proxy prefix.
        organization (str): InfluxDB organization name or identifier.
        bucket (str): Existing bucket whose retention is managed by InfluxDB.
        token (str): Write token resolved from the environment, hidden from representations.
        timeoutSeconds (int): Connection and response timeout in seconds.
        caFile (str): Optional CA bundle; normal certificate validation remains enabled.
    """

    type: Literal['influxdb'] = field(default='influxdb', validator=validators.in_(('influxdb',)))
    url: str = field(converter=_environment)
    organization: str = field(converter=_environment)
    bucket: str = field(converter=_environment)
    token: str = field(converter=_environment, repr=False)
    timeoutSeconds: int = field(default=10, validator=validators.ge(1))
    caFile: str = field(default='', converter=_environment)

    def __attrs_post_init__(self) -> None:
        """
        Reject ambiguous URLs or missing credentials before opening the publisher.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the URL or required destination settings are invalid.
        """
        parsed = urlsplit(self.url)
        if parsed.scheme not in {'http', 'https'} or not parsed.netloc or parsed.username or parsed.query or parsed.fragment:
            raise ValueError('InfluxDB requires an HTTP(S) URL without embedded credentials, query, or fragment')
        if not self.organization or not self.bucket or not self.token:
            raise ValueError('InfluxDB organization, bucket, and write token are required')

    def publisher(self) -> 'InfluxDBPublisher':
        """
        Describe a lazy HTTP publisher without sharing sessions across processes.

        Returns:
            InfluxDBPublisher: Process-local adapter using these attrs settings.
        """
        from premiscale.metrics.publishers.influxdb import InfluxDBPublisher

        return InfluxDBPublisher(self)


StateSettings = SQLiteState | MySQLState
PublisherSettings = LocalMetrics | PostgreSQLMetrics | InfluxDBMetrics


@define
class Databases:
    """
    Carry concrete database configurations through process startup.

    Attributes:
        collectionInterval (int): Seconds between metrics collection passes.
        hostConnectionTimeout (int): Host connection timeout in seconds.
        maxHostConnectionThreads (int): Maximum concurrent host connections per collector process.
        state (StateSettings): Configured state backend instance.
        timeseries (LocalMetrics): Primary time-series read and publication settings.
        hostConnectionQueueSize (int | None): Maximum queued connections; defaults to the thread limit.
        publishers (dict[str, PublisherSettings]): Additional named publication settings.
    """

    collectionInterval: int
    hostConnectionTimeout: int
    maxHostConnectionThreads: int
    state: StateSettings
    timeseries: LocalMetrics
    hostConnectionQueueSize: int | None = None
    publishers: dict[str, PublisherSettings] = field(factory=dict)

    def __attrs_post_init__(self) -> None:
        """
        Check concurrency bounds and prevent multiple writers to the same CSV file.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If thread limits, subscriber names, or CSV destinations are invalid.
        """
        if self.maxHostConnectionThreads < 1:
            raise ValueError('At least one host collection thread is required')
        files = [str(Path(self.timeseries.dbfile).resolve())] if self.timeseries.dbfile else []
        for name, publisher in self.publishers.items():
            if name == 'primary' or re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}', name) is None:
                raise ValueError('Publisher names must be stable identifiers other than primary')
            if isinstance(publisher, LocalMetrics) and publisher.dbfile:
                path = str(Path(publisher.dbfile).resolve())
                if path in files:
                    raise ValueError('Each TinyFlux publisher must have its own CSV file')
                files.append(path)
        if self.hostConnectionQueueSize is None:
            self.hostConnectionQueueSize = self.maxHostConnectionThreads
        elif self.hostConnectionQueueSize < self.maxHostConnectionThreads:
            log.warning('Host connection queue size must be at least the thread limit; using %s.',
                        self.maxHostConnectionThreads)
            self.hostConnectionQueueSize = self.maxHostConnectionThreads

    @property
    def destinations(self) -> dict[str, PublisherSettings]:
        """
        Include the primary store without copying or reconstructing its configuration.

        Returns:
            dict[str, PublisherSettings]: Stable subscriber names mapped to their settings instances.
        """
        return {'primary': self.timeseries, **self.publishers}
