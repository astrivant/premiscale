"""
Exercise packaged schemas and conversion of validated configuration files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile, Path as ZipPath

import pytest
from ruamel.yaml import YAML

from premiscale.config import parse
from premiscale.config.v1alpha1 import AutoscalingGroup, CollectionControl, Host
from premiscale.config.databases import PostgreSQLMetrics
from premiscale.daemon.processes import build

if TYPE_CHECKING:
    from typing import Any


CONFIG_DIR = Path(parse.__file__).parent
FIXTURES = Path(__file__).parents[1] / 'data' / 'config'


@pytest.mark.parametrize('settings', [None, {'initialThreads': 3, 'targetThroughput': 2.5, 'maxStep': 1}])
def test_collection_control_defaults_and_overrides(settings: dict[str, Any] | None,
                                                  config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Parse existing configurations and explicitly tuned collection controllers.

    Args:
        settings (dict[str, Any] | None): Optional PID configuration overrides.
        config_file (tuple[Path, dict[str, Any]]): Temporary YAML path and configuration mapping.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    reconciliation = config['controller']['reconciliation']
    reconciliation.pop('collection', None)
    if settings is not None:
        reconciliation['collection'] = settings
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path))
    parsed = parse.configParse(str(path))
    assert parsed.controller.reconciliation.collection == CollectionControl(**(settings or {}))


@pytest.mark.parametrize('settings', [
    {'minThreads': 0}, {'minThreads': 3, 'initialThreads': 2}, {'initialThreads': 11},
    {'maxStep': 0}, {'smoothing': 0}, {'smoothing': 1.1}, {'deadband': 1},
    {'targetThroughput': 0}, {'targetThroughput': float('inf')},
    {'proportionalGain': -1}, {'integralGain': float('nan')}, {'derivativeGain': float('inf')},
])
def test_invalid_collection_control_is_rejected(settings: dict[str, Any],
                                                 config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Reject unsafe bounds, invalid measurements, and nonfinite controller gains.

    Args:
        settings (dict[str, Any]): Invalid PID configuration overrides.
        config_file (tuple[Path, dict[str, Any]]): Temporary YAML path and configuration mapping.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    config['controller']['reconciliation']['collection'] = settings
    YAML(typ='safe').dump(config, path)
    with pytest.raises(SystemExit) as error:
        parse.configParse(str(path))
    assert error.value.code == 1


@pytest.fixture
def config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, Any]]:
    """
    Use a real certificate path while exercising configuration parsing without transport setup.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        tuple[Path, dict[str, Any]]: Temporary YAML file path and its parsed configuration mapping.
    """
    certificate = tmp_path / 'ca.pem'
    certificate.touch()
    monkeypatch.setenv('PREMISCALE_CACERT', str(certificate))
    config = YAML(typ='safe').load((CONFIG_DIR / 'default.yaml').read_text())
    path = tmp_path / 'config.yaml'
    YAML(typ='safe').dump(config, path)
    return path, config


@pytest.mark.parametrize('path', [CONFIG_DIR / 'default.yaml', *sorted(FIXTURES.glob('*.yaml'))])
def test_bundled_configurations_validate_and_parse(path: Path, config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Verify bundled configurations validate and parse.

    Args:
        path (Path): Filesystem path to the requested resource.
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.

    Returns:
        None: No value is returned.
    """
    assert parse.validateConfig(str(path))
    config = parse.configParse(str(path))
    expected = YAML(typ='safe').load(path.read_text())
    assert config.version == expected['version']
    assert config.controller.kubernetes.autoscalerHost == expected['controller']['kubernetes']['autoscalerHost']
    assert config.controller.kubernetes.autoscalerPort == expected['controller']['kubernetes']['autoscalerPort']
    assert len(config.controller.autoscale.hosts) == len(expected['controller']['autoscale']['hosts'])
    assert config.controller.autoscale.groups == {}


def test_missing_config_is_created_and_parsed(config_file: tuple[Path, dict[str, Any]], tmp_path: Path) -> None:
    """
    Verify missing config is created and parsed.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        None: No value is returned.
    """
    path = tmp_path / 'nested' / 'default.yaml'
    assert parse.configParse(str(path)).version == 'v1alpha1'
    assert parse.validateConfig(str(path))


@pytest.mark.parametrize('value', ['on', 'off', 'yes', 'no'])
def test_validation_and_parsing_share_yaml12_string_semantics(value: str,
                                                            config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Keep YAML 1.2 string scalars consistent between validation and model construction.

    Args:
        value (str): Unquoted scalar that YAML 1.1 would interpret as a boolean.
        config_file (tuple[Path, dict[str, Any]]): Temporary YAML path and configuration mapping.

    Returns:
        None: No value is returned.
    """
    path, _ = config_file
    path.write_text(path.read_text().replace('clusterName: premiscale', f'clusterName: {value}'))
    assert parse.validateConfig(str(path))
    assert parse.configParse(str(path)).controller.kubernetes.clusterName == value


@pytest.mark.parametrize('content', [
    '', '# comment only', 'null', '42', 'true', 'a scalar', '[]', '- version',
    '{}', 'controller: {}', 'version: [', 'version: v1alpha1\n---\nversion: v1alpha1',
    'version: v1alpha1\nversion: v1alpha1',
])
def test_invalid_documents_fail_cleanly(content: str, tmp_path: Path) -> None:
    """
    Verify invalid documents fail cleanly.

    Args:
        content (str): Binary reader containing the volume contents.
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        None: No value is returned.
    """
    path = tmp_path / 'invalid.yaml'
    path.write_text(content)
    assert parse.validateConfig(str(path)) is False
    with pytest.raises(SystemExit) as error:
        parse.configParse(str(path))
    assert error.value.code == 1


@pytest.mark.parametrize('version', ['v999', '../default', None, 1, [], {}])
def test_unsupported_versions_fail_cleanly(version: Any, config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Verify unsupported versions fail cleanly.

    Args:
        version (Any): Controller version advertised during platform registration.
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    config['version'] = version
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path), version=version) is False
    with pytest.raises(SystemExit) as error:
        parse.configParse(str(path))
    assert error.value.code == 1


def test_strict_validation(config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Verify strict validation.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    config['controller']['unknown'] = True
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path)) is False
    assert parse.validateConfig(str(path), strict=False)


def test_included_schema_rejects_invalid_port(config_file: tuple[Path, dict[str, Any]]) -> None:
    """
    Verify included schema rejects invalid port.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    config['controller']['kubernetes']['autoscalerPort'] = 65536
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path)) is False


def test_broker_settings_use_environment_defaults_and_yaml_overrides(config_file: tuple[Path, dict[str, Any]], monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify broker settings use environment defaults and yaml overrides.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    monkeypatch.setenv('PREMISCALE_REDIS_URL', 'redis://dragonfly.example:6379/1')
    monkeypatch.setenv('PREMISCALE_QUEUE_NAMESPACE', 'namespace:release')
    broker = parse.configParse(str(path)).controller.broker
    assert broker.url == 'redis://dragonfly.example:6379/1'
    assert broker.namespace == 'namespace:release'
    config['controller']['broker'].update(url='rediss://other.example:6379/0', namespace='override', leaseSeconds=90)
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path))
    broker = parse.configParse(str(path)).controller.broker
    assert broker.url == 'rediss://other.example:6379/0'
    assert broker.namespace == 'override'
    assert broker.leaseSeconds == 90
    config['controller']['broker']['leaseSeconds'] = 0
    YAML(typ='safe').dump(config, path)
    assert not parse.validateConfig(str(path))


def test_schema_can_be_loaded_from_zip(config_file: tuple[Path, dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify schema can be loaded from zip.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    path, _ = config_file
    archive = tmp_path / 'schemas.zip'
    with ZipFile(archive, 'w') as packaged:
        packaged.write(CONFIG_DIR / 'schemas' / 'schema.v1alpha1.json', 'schema.v1alpha1.json')
    with ZipFile(archive) as packaged:
        monkeypatch.setattr(parse.resources, 'files', lambda package: ZipPath(packaged))
        assert parse.validateConfig(str(path))


@pytest.mark.parametrize('inline_hosts', [False, True])
def test_named_groups_retain_schema_fields(config_file: tuple[Path, dict[str, Any]], inline_hosts: bool) -> None:
    """
    Verify named groups retain schema fields.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Temporary configuration path and its mutable YAML mapping.
        inline_hosts (bool): Whether to place complete host definitions in the node group.

    Returns:
        None: No value is returned.
    """
    path, config = config_file
    host = dict(name='host-1', address='127.0.0.1', port=22, protocol='ssh', hypervisor='qemu')
    group = {
        'image': '/images/template.qcow2',
        'name': 'template-domain',
        'imageMigration': 'migrate',
        'cloud-init': {'inline': '#cloud-config', 'file': '/cloud-init.yaml'},
        'hosts': [host] if inline_hosts else ['host-1'],
        'replacement': {'strategy': 'rollingUpdate', 'maxUnavailable': 1, 'maxSurge': 2},
        'networking': {
            'type': 'static', 'addresses': ['192.168.1.10'],
            'subnet': '192.168.1.0/24', 'gateway': '192.168.1.1',
        },
        'scaling': {
            'method': 'random', 'minNodes': 1, 'maxNodes': 5, 'increment': 1, 'cooldown': 60,
            'resourceTarget': {'cpu': 80, 'memory': 70, 'storage': 60},
        },
    }
    second_group = deepcopy(group)
    second_group['name'] = 'another-template'
    config['controller']['autoscale'] = {
        'hosts': [host], 'groups': {'asg-1': group, 'asg-2': second_group},
    }
    YAML(typ='safe').dump(config, path)
    assert parse.validateConfig(str(path))

    groups = parse.configParse(str(path)).controller.autoscale.groups
    assert set(groups) == {'asg-1', 'asg-2'}
    actual = groups['asg-1']
    assert isinstance(actual, AutoscalingGroup)
    assert actual.name == 'template-domain'
    assert actual.imageMigration == 'migrate'
    assert actual.cloudInit.inline == '#cloud-config'
    assert actual.cloudInit.file == '/cloud-init.yaml'
    assert actual.replacement.maxSurge == 2
    assert actual.networking.type == 'static'
    assert actual.networking.addresses == ['192.168.1.10']
    assert actual.scaling.minNodes == 1
    assert actual.scaling.maxNodes == 5
    assert actual.scaling.resourceTarget.cpu == 80
    assert groups['asg-2'].name == 'another-template'
    if inline_hosts:
        assert isinstance(actual.hosts[0], Host)
        assert actual.hosts[0].name == 'host-1'
    else:
        assert actual.hosts == ['host-1']

def test_metric_publishers_parse_and_select_independent_workers(config_file: tuple[Path, dict[str, Any]], monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Parse named publisher settings and construct worker specifications without IO.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Isolated configuration file and mapping.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring DSN environment variables.

    Returns:
        None: No value is returned.
    """
    path, data = config_file
    data['controller']['mode'] = 'kubernetes'
    data['controller']['databases']['publishers'] = {
        'archive': {'type': 'postgresql', 'dsn': '$PREMISCALE_METRICS_POSTGRES_DSN', 'retention': 3600},
    }
    monkeypatch.setenv('PREMISCALE_METRICS_POSTGRES_DSN', 'postgresql://user:secret@database/metrics')
    YAML(typ='safe').dump(data, path)
    assert parse.validateConfig(str(path))
    config = parse.configParse(str(path))
    settings = config.controller.databases.publishers['archive']
    assert isinstance(settings, PostgreSQLMetrics)
    assert settings.dsn == 'postgresql://user:secret@database/metrics'
    assert 'secret' not in repr(settings)
    specs = build(config, 'test', '')
    from premiscale.reconciliation.runtime import pipeline_processes
    from premiscale.daemon.settings import Execution

    assert 'reconciliation' in {spec.name for spec in specs}
    subscribers = next(spec for spec in pipeline_processes(config, Execution()) if spec.name == 'publishers')
    assert subscribers.args[1] == ('_state', 'primary', 'archive')
