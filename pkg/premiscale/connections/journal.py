"""
Open local or shared SQL journals without embedding database statements in Python.
"""

from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from premiscale.support.sql import load_sql

if TYPE_CHECKING:
    from typing import Any, Iterator


def schema_name(cluster: str) -> str:
    """
    Derive an isolated, stable PostgreSQL schema for one managed cluster.

    Args:
        cluster (str): Persistent managed-cluster identity.

    Returns:
        str: Identifier independent of pod names and failover order.
    """
    return f'premiscale_{sha256(cluster.encode()).hexdigest()[:32]}'


def connect_postgresql(dsn: str) -> psycopg.Connection:
    """
    Bound journal IO so process supervision can enforce leadership deadlines.

    Args:
        dsn (str): PostgreSQL connection settings, including any TLS configuration.

    Returns:
        psycopg.Connection: Autocommit session with bounded connection and statement waits.
    """
    return psycopg.connect(dsn, autocommit=True, connect_timeout=3,
                           options='-c statement_timeout=3000 -c lock_timeout=3000',
                           keepalives=1, keepalives_idle=5, keepalives_interval=2,
                           keepalives_count=2, tcp_user_timeout=5000)


class Journal:
    """
    Share journal transactions across SQLite and PostgreSQL implementations.
    """

    def __init__(self, path: str, cluster: str, dsn: str = '', mappings: bool = False) -> None:
        """
        Open a database and select its packaged SQL dialect.

        Args:
            path (str): SQLite path, ignored when dsn is supplied.
            cluster (str): Stable cluster identity used for PostgreSQL schema isolation.
            dsn (str): Optional PostgreSQL connection string.
            mappings (bool): Return named rows instead of tuples.

        Raises:
            BaseException: If database initialization fails after opening the session.
        """
        self.postgresql = bool(dsn)
        self.connection: Any
        if dsn:
            self.connection = connect_postgresql(dsn)
            try:
                if mappings:
                    self.connection.row_factory = dict_row
                identifier = sql.Identifier(schema_name(cluster))
                # Serialize first-use DDL across the operation and volume connections.
                with self.connection.transaction():
                    self.connection.execute(load_sql('postgresql/lock_schema.sql'), (schema_name(cluster),))
                    self.connection.execute(sql.SQL(load_sql('postgresql/create_schema.sql')).format(schema=identifier))
                self.connection.execute(sql.SQL(load_sql('postgresql/search_path.sql')).format(schema=identifier))
            except BaseException:
                self.connection.close()
                raise
        else:
            if path != ':memory:':
                target = Path(path).expanduser()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.touch(mode=0o600, exist_ok=True)
                path = str(target)
            self.connection = sqlite3.connect(path, check_same_thread=False)
            if mappings:
                self.connection.row_factory = sqlite3.Row

    def execute(self, name: str, parameters: tuple[Any, ...] = ()) -> Any:
        """
        Execute a packaged statement with separately bound values.

        Args:
            name (str): Statement path relative to the SQL package and optional dialect.
            parameters (tuple[Any, ...]): Bound values in statement placeholder order.

        Returns:
            Any: Database cursor with the requested row representation.
        """
        return self.connection.execute(load_sql(f'postgresql/{name}' if self.postgresql else name), parameters)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Commit successful changes or roll back without closing the PostgreSQL session.

        Yields:
            None: Control inside the database transaction.
        """
        with self.connection.transaction() if self.postgresql else self.connection:
            yield

    def close(self) -> None:
        """
        Close the underlying database session.

        Returns:
            None: No value is returned.
        """
        self.connection.close()


class Ownership:
    """
    Hold a PostgreSQL session lock for a complete service lifetime.
    """

    def __init__(self, dsn: str, cluster: str, purpose: str) -> None:
        """
        Acquire exclusive ownership independently of the Kubernetes Lease.

        Args:
            dsn (str): Shared PostgreSQL connection settings.
            cluster (str): Stable cluster identity.
            purpose (str): Separate lock domain for the leader or VM provider.

        Raises:
            RuntimeError: If another session still owns this service.
            BaseException: If acquisition fails after the connection opens.
        """
        self.connection = connect_postgresql(dsn)
        try:
            row = self.connection.execute(load_sql('postgresql/acquire_owner.sql'),
                                          (f'{schema_name(cluster)}:{purpose}',)).fetchone()
            if not row or not row[0]:
                raise RuntimeError(f'Another process still owns the {purpose} journal lock')
        except BaseException:
            self.connection.close()
            raise

    def check(self) -> None:
        """
        Verify the original session remains alive without reconnecting or reacquiring.

        Returns:
            None: No value is returned.
        """
        self.connection.execute(load_sql('postgresql/check_owner.sql')).fetchone()

    def close(self) -> None:
        """
        Release ownership only after the owned processes have stopped.

        Returns:
            None: No value is returned.
        """
        self.connection.close()
