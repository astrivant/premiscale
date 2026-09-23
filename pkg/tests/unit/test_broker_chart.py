"""
Verify the bundled broker, external overrides, and Minikube dependency wiring.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import pytest
from jsonschema import Draft7Validator
from ruamel.yaml import YAML

if TYPE_CHECKING:
    from typing import Any


ROOT = Path(__file__).resolve().parents[3]
HELM = os.environ.get('PREMISCALE_TEST_HELM', shutil.which('helm'))


@pytest.mark.parametrize('field', ['nameOverride', 'fullnameOverride'])
def test_broker_names_reject_unquoted_yaml_scalars(field: str) -> None:
    """
    Reject names the dependency would emit as numbers, booleans, or null.

    Args:
        field (str): Dependency resource name override to validate.

    Returns:
        None: No value is returned.
    """
    schema = YAML(typ='safe').load(ROOT / 'charts/premiscale/values.schema.json')
    validator = Draft7Validator(schema['properties']['dragonfly'])
    for value in ['0', '123', 'y', 'n', 'yes', 'no', 'true', 'false', 'on', 'off', 'null']:
        assert not validator.is_valid({field: value}), value
    for value in ['', 'queue', 'queue-123']:
        assert validator.is_valid({field: value}), value


def render(tmp_path: Path, values: dict[str, Any], chart: str = 'charts/premiscale') -> list[dict[str, Any]]:
    """
    Render a chart with built dependencies and parse its Kubernetes resources.

    Args:
        tmp_path (Path): Directory for the temporary values file.
        values (dict[str, Any]): Overrides merged with the chart defaults.
        chart (str): Chart directory relative to the repository root.

    Returns:
        list[dict[str, Any]]: Rendered resources for release broker-test in namespace brokers.
    """
    if HELM is None:
        pytest.skip('Helm is required for broker chart tests')
    if not list((ROOT / chart / 'charts').glob('*.tgz')):
        pytest.skip('Build locked chart dependencies before running broker chart tests')
    path = tmp_path / 'values.yaml'
    YAML().dump(values, path)
    result = subprocess.run([HELM, 'template', 'broker-test', str(ROOT / chart), '--namespace', 'brokers',
                             '--kube-version', '1.35.0', '--values', str(path)],
                            capture_output=True, text=True, check=False, timeout=30)
    assert result.returncode == 0, result.stderr
    return [resource for resource in YAML(typ='safe').load_all(result.stdout) if resource]


def broker_environment(resources: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Find the controller's effective broker environment entry.

    Args:
        resources (list[dict[str, Any]]): Rendered Kubernetes resources.

    Returns:
        dict[str, Any]: Redis URL value or Secret reference injected into the controller.
    """
    controller = next(resource for resource in resources
                      if resource['kind'] == 'Deployment' and resource['metadata']['name'] == 'premiscale')
    environment = controller['spec']['template']['spec']['containers'][0]['env']
    return next(entry for entry in environment if entry['name'] == 'PREMISCALE_REDIS_URL')


@pytest.mark.parametrize('overrides', [
    {}, {'nameOverride': 'queue'}, {'fullnameOverride': 'shared-broker', 'service': {'port': 6380}},
])
def test_bundled_broker_url_targets_its_persistent_service(tmp_path: Path, overrides: dict[str, Any]) -> None:
    """
    Keep controller discovery, Service ports, and durable broker storage consistent.

    Args:
        tmp_path (Path): Directory for the temporary values file.
        overrides (dict[str, Any]): Dependency name and port overrides.

    Returns:
        None: No value is returned.
    """
    resources = render(tmp_path, {'dragonfly': overrides})
    brokers = [resource for resource in resources if resource['kind'] == 'StatefulSet']
    assert len(brokers) == 1
    broker = brokers[0]
    name = broker['metadata']['name']
    service = next(resource for resource in resources
                   if resource['kind'] == 'Service' and resource['metadata']['name'] == name)
    url = urlsplit(broker_environment(resources)['value'])
    assert url.scheme == 'redis'
    assert url.hostname == f'{name}.brokers.svc'
    assert url.port == service['spec']['ports'][0]['port']
    assert broker['spec']['serviceName'] == name
    assert broker['spec']['replicas'] == 1
    claim = broker['spec']['volumeClaimTemplates'][0]
    assert claim['metadata']['name'] == f'{name}-data'
    assert claim['spec']['resources']['requests']['storage'] == '1Gi'
    container = broker['spec']['template']['spec']['containers'][0]
    assert '--cache_mode=false' in container['args']
    assert '--snapshot_cron=*/1 * * * *' in container['args']
    assert container['volumeMounts'][0]['name'] == claim['metadata']['name']


@pytest.mark.parametrize('url', ['', 'redis://external-broker:6380/2'])
def test_external_broker_disables_bundled_resources(tmp_path: Path, url: str) -> None:
    """
    Allow external URLs and the existing external default without creating a broker.

    Args:
        tmp_path (Path): Directory for the temporary values file.
        url (str): Explicit external URL or empty for the existing default.

    Returns:
        None: No value is returned.
    """
    resources = render(tmp_path, {'dragonfly': {'enabled': False}, 'controller': {'broker': {'url': url}}})
    assert not any(resource['metadata'].get('labels', {}).get('app.kubernetes.io/name') == 'dragonfly'
                   for resource in resources)
    assert broker_environment(resources)['value'] == (url or 'redis://dragonfly:6379/0')


def test_broker_secret_takes_precedence_over_explicit_url(tmp_path: Path) -> None:
    """
    Keep broker credentials in a Secret when an explicit URL also exists.

    Args:
        tmp_path (Path): Directory for the temporary values file.

    Returns:
        None: No value is returned.
    """
    resources = render(tmp_path, {'dragonfly': {'enabled': False}, 'controller': {'broker': {
        'url': 'redis://unused:6379/0', 'existingSecret': 'broker-auth', 'secretKey': 'connection',
    }}})
    entry = broker_environment(resources)
    assert 'value' not in entry
    assert entry['valueFrom']['secretKeyRef'] == {'name': 'broker-auth', 'key': 'connection'}


def test_bundled_tls_selects_rediss(tmp_path: Path) -> None:
    """
    Discover a TLS broker using the matching client URL scheme.

    Args:
        tmp_path (Path): Directory for the temporary values file.

    Returns:
        None: No value is returned.
    """
    resources = render(tmp_path, {'dragonfly': {'tls': {'enabled': True, 'existing_secret': 'broker-tls'}}})
    assert broker_environment(resources)['value'].startswith('rediss://')
    broker = next(resource for resource in resources if resource['kind'] == 'StatefulSet')
    assert '--tls' in broker['spec']['template']['spec']['containers'][0]['args']


def test_minikube_reuses_one_broker_dependency(tmp_path: Path) -> None:
    """
    Use the parent chart's broker and standard local storage without duplicate resources.

    Args:
        tmp_path (Path): Directory for the temporary values file.

    Returns:
        None: No value is returned.
    """
    config = (ROOT / '.config/minikube/controller.yaml').read_text(encoding='utf-8')
    resources = render(tmp_path, {'premiscale': {'configMap': {'config': config}}}, 'integrations/minikube/chart')
    brokers = [resource for resource in resources if resource['kind'] == 'StatefulSet']
    assert len(brokers) == 1
    broker = brokers[0]
    assert broker['metadata']['name'] == 'broker-test-dragonfly'
    assert broker['spec']['volumeClaimTemplates'][0]['spec']['storageClassName'] == 'standard'
    assert broker_environment(resources)['value'] == 'redis://broker-test-dragonfly.brokers.svc:6379/0'
