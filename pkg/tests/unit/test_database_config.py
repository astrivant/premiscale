"""
Exercise typed database configuration, lazy adapters, and configuration round trips.
"""

from __future__ import annotations

from datetime import datetime, timezone
import pickle
from typing import TYPE_CHECKING
from uuid import uuid4

from attrs import has
import pytest
from ruamel.yaml import YAML

from premiscale.config import parse
from premiscale.config.databases import LocalMetrics, MySQLState, PostgreSQLMetrics, SQLiteState
from premiscale.metrics.state.mysql import MySQL
from premiscale.schemas.metrics import Metric, MetricBatch
from .test_config_parse import config_file

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def test_yaml_selects_serializable_backend_settings(config_file: tuple[Path, dict[str, Any]],
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Preserve the YAML format while selecting concrete attrs classes without database IO.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Valid configuration path and source mapping.
        monkeypatch (pytest.MonkeyPatch): Environment and database connection overrides.

    Returns:
        None: No value is returned.
    """
    path, data = config_file
    monkeypatch.setenv('PREMISCALE_TEST_DATABASE_PASSWORD', 'test-password')
    monkeypatch.setenv('PREMISCALE_TEST_DATABASE_DSN', 'postgresql://database/metrics')
    databases = data['controller']['databases']
    databases['state'] = {'type': 'mysql', 'connection': {
        'url': 'database', 'database': 'state',
        'credentials': {'username': 'premiscale', 'password': '$PREMISCALE_TEST_DATABASE_PASSWORD'},
    }}
    databases['publishers'] = {
        'archive': {'type': 'postgresql', 'dsn': '$PREMISCALE_TEST_DATABASE_DSN'},
        'csv': {'type': 'memory', 'dbfile': str(path.parent / 'metrics.csv')},
    }

    def unexpected_connection(*_args: Any, **_kwargs: Any) -> None:
        """
        Fail if configuration parsing or adapter construction opens a database.

        Args:
            *_args (Any): Unexpected positional connection arguments.
            **_kwargs (Any): Unexpected keyword connection arguments.

        Returns:
            None: No value is returned.

        Raises:
            AssertionError: When a database connection is attempted before process ownership.
        """
        raise AssertionError('Configuration and adapter construction must not open databases')

    monkeypatch.setattr('psycopg.connect', unexpected_connection)
    monkeypatch.setattr('premiscale.metrics.timeseries.local.Local.open', unexpected_connection)
    monkeypatch.setattr(MySQL, 'open', unexpected_connection)
    YAML(typ='safe').dump(data, path)
    config = pickle.loads(pickle.dumps(parse.configParse(str(path))))
    settings = config.controller.databases
    assert isinstance(settings.state, MySQLState)
    assert isinstance(settings.timeseries, LocalMetrics)
    assert all(has(type(item)) for item in settings.destinations.values())
    assert settings.destinations['primary'] is settings.timeseries
    adapter = settings.state.adapter()
    assert (adapter.url, adapter.database, adapter._username, adapter._password) == (
        'database', 'state', 'premiscale', 'test-password',
    )
    assert 'test-password' not in repr(settings)
    remote = settings.publishers['archive']
    assert isinstance(remote, PostgreSQLMetrics)
    assert remote.dsn == 'postgresql://database/metrics'
    assert remote.publisher().connection is None
    settings.timeseries.adapter()
    settings.destinations['csv'].publisher()
    assert not (path.parent / 'metrics.csv').exists()


def test_backend_instances_open_separate_adapters_with_shared_configuration(tmp_path: Path) -> None:
    """
    Persist through one adapter and reopen from the same serializable settings.

    Args:
        tmp_path (Path): Isolated SQLite and TinyFlux file destinations.

    Returns:
        None: No value is returned.
    """
    state_path = tmp_path / 'state.db'
    settings = SQLiteState(dbfile=str(state_path))
    first = settings.adapter()
    assert not state_path.exists()
    with first as state:
        state.initialize()
        state.host_create('host', 'localhost', 'ssh', 22, 'qemu', 8, 4096, 8192)
    with pickle.loads(pickle.dumps(settings)).adapter() as state:
        assert state is not first
        assert state.host_exists('host', 'localhost')

    metrics = LocalMetrics(dbfile=str(tmp_path / 'metrics.csv'))
    publisher = metrics.publisher()
    batch = MetricBatch((Metric('cpu', datetime.now(timezone.utc), {'host': 'host'}, {'usage': 3.0}),))
    identity = str(uuid4())
    try:
        publisher.publish(batch, identity)
    finally:
        publisher.close()
    with metrics.adapter() as reader:
        assert reader.contains_sample(f'{identity}:0')


@pytest.mark.parametrize('field, value', [
    ('state', {'type': 'unknown'}),
    ('state', {'type': 'mysql'}),
    ('timeseries', {'type': 'postgresql', 'retention': 300}),
    ('publishers', {'archive': {'type': 'postgresql'}}),
    ('publishers', {'archive': {'type': 'postgresql', 'dsn': 'postgresql://db', 'dbfile': 'metrics.csv'}}),
    ('publishers', {'archive': {'type': 'memory', 'dsn': 'postgresql://db'}}),
])
def test_invalid_backend_combinations_fail_during_parsing(config_file: tuple[Path, dict[str, Any]],
                                                        field: str, value: dict[str, Any]) -> None:
    """
    Reject unsupported backends or mixed settings before launching subprocesses.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Valid configuration path and source mapping.
        field (str): Database configuration field to replace.
        value (dict[str, Any]): Invalid backend selection or incompatible settings.

    Returns:
        None: No value is returned.
    """
    path, data = config_file
    data['controller']['databases'][field] = value
    YAML(typ='safe').dump(data, path)
    with pytest.raises(SystemExit) as error:
        parse.configParse(str(path))
    assert error.value.code == 1
