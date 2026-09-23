"""
Verify Helm configuration, CRD projection, rollout protection, and node placement.
"""

from __future__ import annotations

from copy import deepcopy
from io import StringIO
import json
from typing import TYPE_CHECKING
from unittest.mock import Mock

from jsonschema import Draft7Validator
import pytest
from requests import HTTPError, Response
from ruamel.yaml import YAML

from premiscale.config.projection import project
from premiscale.config.v1alpha1 import Config
from premiscale.daemon.settings import Execution
from premiscale.operator.configuration import Projector
from .test_broker_chart import ROOT, render

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def custom_resources() -> dict[str, Any]:
    """
    Load a complete ControllerConfig, Host, and AutoscalingGroup example.

    Returns:
        dict[str, Any]: Independent chart values for multiple named custom resources.
    """
    return YAML(typ='safe').load(ROOT / 'charts/premiscale-crds/examples/multiple-resources.yaml')


def test_structured_values_cover_runtime_schema_and_follow_listener_overrides(tmp_path: Path) -> None:
    """
    Render typed settings into the mounted file and derive probes from its listener ports.

    Args:
        tmp_path (Path): Temporary Helm values directory.

    Returns:
        None: No value is returned.
    """
    schema = json.loads((ROOT / 'charts/premiscale/values.schema.json').read_text())
    runtime_schema = json.loads((ROOT / 'pkg/premiscale/config/schemas/schema.v1alpha1.json').read_text())
    assert schema['properties']['config'] == runtime_schema
    resources = render(tmp_path, {'configMap': {'name': 'operator-config'}, 'config': {'controller': {
        'healthcheck': {'port': 8185, 'apiPort': 9190, 'stateDirectory': '/run/custom-status'},
        'broker': {'leaseSeconds': 90}, 'reconciliation': {'collection': {'initialThreads': 3}},
    }}})
    configmap = next(item for item in resources if item['kind'] == 'ConfigMap' and item['metadata']['name'] == 'operator-config')
    data = YAML(typ='safe').load(configmap['data']['config.yaml'])
    Draft7Validator(runtime_schema).validate(data)
    assert data['controller']['broker']['leaseSeconds'] == 90
    assert data['controller']['reconciliation']['collection']['initialThreads'] == 3
    deployment = next(item for item in resources if item['kind'] == 'Deployment' and item['metadata']['name'] == 'premiscale')
    assert deployment['metadata']['annotations']['configmap.reloader.stakater.com/reload'] == 'operator-config'
    pod = deployment['spec']['template']['spec']
    container = pod['containers'][0]
    assert {item['name']: item['containerPort'] for item in container['ports']} == {'healthcheck': 8185, 'metrics': 9190}
    assert container['readinessProbe']['httpGet']['port'] == 'metrics'
    assert next(mount for mount in container['volumeMounts'] if mount['name'] == 'runtime-status')['mountPath'] == '/run/custom-status'
    assert next(item for item in pod['volumes'] if item['name'] == 'config')['configMap']['name'] == 'operator-config'


def test_helm_and_operator_project_identical_custom_resources(tmp_path: Path) -> None:
    """
    Resolve resource references identically before installation and during live reconciliation.

    Args:
        tmp_path (Path): Temporary Helm values directory.

    Returns:
        None: No value is returned.
    """
    crds = custom_resources()
    resources = render(tmp_path, {'configMap': {'source': 'crds', 'controllerConfig': 'primary'}, 'premiscale-crds': crds})
    configmap = next(item for item in resources if item['kind'] == 'ConfigMap' and item['metadata']['name'] == 'premiscale')
    actual = YAML(typ='safe').load(configmap['data']['config.yaml'])
    expected = project(crds['controllerConfigs']['primary']['spec'],
                       {name: item['spec'] for name, item in crds['hosts'].items()},
                       {name: item['spec'] for name, item in crds['autoscalingGroups'].items()})
    assert actual == expected
    assert actual['controller']['autoscale']['hosts'][0]['name'] == 'hypervisor-primary'
    assert actual['controller']['autoscale']['groups']['workers-primary']['hosts'] == ['hypervisor-primary']
    role = next(item for item in resources if item['kind'] == 'Role' and item['metadata']['name'] == 'broker-test-operator')
    permission = next(rule for rule in role['rules'] if rule['resources'] == ['configmaps'])
    assert permission['resourceNames'] == ['premiscale']
    assert set(permission['verbs']) == {'get', 'patch'}


@pytest.mark.parametrize('mode', ['ha', 'hha'])
def test_ha_placement_budgets_and_reload_match_every_workload(tmp_path: Path, mode: str) -> None:
    """
    Protect controllers and independent worker pools using their exact replica selectors.

    Args:
        tmp_path (Path): Temporary Helm values directory.
        mode (str): Consolidated or split HA composition.

    Returns:
        None: No value is returned.
    """
    values = YAML(typ='safe').load(ROOT / '.config/keda/values.yaml')
    values['mode'] = mode
    values['config'] = YAML(typ='safe').load(ROOT / '.config/keda/controller.yaml')
    values['configMap']['name'] = 'shared-runtime'
    values['scheduling'] = {'collectors': {'nodeSelector': {'premiscale.com/network': 'hypervisors'}}}
    resources = render(tmp_path, values)
    deployments = [item for item in resources if item['kind'] == 'Deployment'
                   and item['spec']['template']['spec']['containers'][0]['name'] in {'premiscale', 'worker'}]
    assert len(deployments) == (1 if mode == 'ha' else 3)
    for deployment in deployments:
        spec = deployment['spec']
        labels = spec['selector']['matchLabels']
        pod = spec['template']['spec']
        budgets = [item for item in resources if item['kind'] == 'PodDisruptionBudget'
                   and item['spec']['selector']['matchLabels'] == labels]
        assert len(budgets) == 1
        assert all(constraint['labelSelector']['matchLabels'] == labels for constraint in pod['topologySpreadConstraints'])
        assert deployment['metadata']['annotations']['configmap.reloader.stakater.com/reload'] == 'shared-runtime'
        assert next(item for item in pod['volumes'] if item['name'] == 'config')['configMap']['name'] == 'shared-runtime'
        assert spec['minReadySeconds'] >= 10
        if deployment['metadata']['name'] == 'premiscale':
            anti = pod['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution']
            assert anti[0] == {'topologyKey': 'kubernetes.io/hostname', 'labelSelector': {'matchLabels': labels}}
            assert budgets[0]['spec']['minAvailable'] == 1
            assert spec['strategy']['rollingUpdate'] == {'maxSurge': 0, 'maxUnavailable': 1}
        else:
            assert budgets[0]['spec']['maxUnavailable'] == 1
            assert spec['strategy']['rollingUpdate'] == {'maxSurge': 1, 'maxUnavailable': 0}
            assert 'preferredDuringSchedulingIgnoredDuringExecution' in pod['affinity']['podAntiAffinity']
            if labels['app.kubernetes.io/component'] == 'collectors':
                assert pod['nodeSelector'] == {'premiscale.com/network': 'hypervisors'}


@pytest.fixture
def projection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Projector, dict[str, Any], list[dict[str, Any]]]:
    """
    Model Kubernetes optimistic updates while using real configuration parsing and projection.

    Args:
        tmp_path (Path): Test certificate directory.
        monkeypatch (pytest.MonkeyPatch): Process environment overrides.

    Returns:
        tuple[Projector, dict[str, Any], list[dict[str, Any]]]: Projector, mutable API objects, and accepted patches.
    """
    certificate = tmp_path / 'ca.pem'
    certificate.touch()
    monkeypatch.setenv('PREMISCALE_CACERT', str(certificate))
    monkeypatch.setenv('PREMISCALE_NAMESPACE', 'test')
    monkeypatch.setenv('PREMISCALE_TEST_METRICS_DSN', 'postgresql://user:private-password@database/metrics')
    crds = custom_resources()
    controller = crds['controllerConfigs']['primary']['spec']
    controller['databases']['publishers'] = {'archive': {'type': 'postgresql', 'dsn': '${PREMISCALE_TEST_METRICS_DSN}'}}
    hosts = {name: item['spec'] for name, item in crds['hosts'].items()}
    groups = {name: item['spec'] for name, item in crds['autoscalingGroups'].items()}
    data = project(controller, hosts, groups)
    output = StringIO()
    YAML().dump(data, output)
    objects = {
        'controllerconfigs/primary': {'metadata': {'generation': 1}, 'spec': controller},
        'hosts': {'items': [{'metadata': {'name': name}, 'spec': spec} for name, spec in hosts.items()]},
        'autoscalinggroups': {'items': [{'metadata': {'name': name}, 'spec': spec} for name, spec in groups.items()]},
        'configmaps/runtime': {'metadata': {'resourceVersion': '1', 'annotations': {'premiscale.com/controller-config': 'primary'}},
                               'data': {'config.yaml': output.getvalue()}},
    }
    patches: list[dict[str, Any]] = []

    def request(method: str, path: str, body: dict[str, Any] | None = None) -> Response:
        """
        Implement GET and a resource-version-checked ConfigMap merge patch.

        Args:
            method (str): Requested HTTP method.
            path (str): Namespaced Kubernetes API path.
            body (dict[str, Any] | None): Optional requested ConfigMap update.

        Returns:
            Response: Kubernetes-style success or conflict response.
        """
        key = path.split('/namespaces/test/', 1)[1]
        resource = objects[key]
        response = Response()
        response.status_code = 200
        if method == 'PATCH' and body is not None:
            if body['metadata']['resourceVersion'] != resource['metadata']['resourceVersion'] or objects.get('conflict'):
                response.status_code = 409
            else:
                patches.append(deepcopy(body))
                resource['data'].update(body['data'])
                resource['metadata']['resourceVersion'] = str(int(resource['metadata']['resourceVersion']) + 1)
        response._content = json.dumps(resource).encode()
        return response

    client = Mock(request=Mock(side_effect=request))
    return Projector(Config.from_dict(data), 'primary', 'runtime', Execution(), client), objects, patches


def test_live_projection_is_idempotent_atomic_and_preserves_secret_references(projection: tuple[Projector, dict[str, Any], list[dict[str, Any]]]) -> None:
    """
    Trigger one ConfigMap update for a source change and retain unexpanded credential references.

    Args:
        projection (tuple[Projector, dict[str, Any], list[dict[str, Any]]]): Projector and mutable test API.

    Returns:
        None: No value is returned.
    """
    projector, objects, patches = projection
    projector.reconcile()
    assert not patches
    objects['controllerconfigs/primary']['spec']['reconciliation']['interval'] = 120
    objects['controllerconfigs/primary']['metadata']['generation'] = 2
    digest, generation = projector.reconcile()
    assert len(patches) == 1 and generation == 2 and len(digest) == 64
    assert patches[0]['metadata']['resourceVersion'] == '1'
    content = patches[0]['data']['config.yaml']
    assert '${PREMISCALE_TEST_METRICS_DSN}' in content and 'private-password' not in content
    projector.reconcile()
    assert len(patches) == 1


@pytest.mark.parametrize('invalid', ['missing-host', 'outside-inventory', 'threads', 'listener', 'ownership', 'immutable', 'conflict'])
def test_invalid_projection_retains_last_known_configuration(projection: tuple[Projector, dict[str, Any], list[dict[str, Any]]],
                                                          invalid: str) -> None:
    """
    Keep running configuration intact on invalid resources or conflicting ConfigMap updates.

    Args:
        projection (tuple[Projector, dict[str, Any], list[dict[str, Any]]]): Projector and mutable test API.
        invalid (str): Reference, runtime, ownership, or optimistic update failure to inject.

    Returns:
        None: No value is returned.
    """
    projector, objects, patches = projection
    controller = objects['controllerconfigs/primary']['spec']
    current = objects['configmaps/runtime']
    original = current['data']['config.yaml']
    controller['reconciliation']['interval'] = 120
    if invalid == 'missing-host':
        objects['hosts']['items'] = []
    elif invalid == 'outside-inventory':
        controller['autoscale']['hosts'] = []
    elif invalid == 'threads':
        controller['reconciliation']['collection']['initialThreads'] = 999
    elif invalid == 'listener':
        controller['healthcheck']['port'] += 1
    elif invalid == 'ownership':
        current['metadata']['annotations']['premiscale.com/controller-config'] = 'somebody-else'
    elif invalid == 'immutable':
        current['immutable'] = True
    else:
        objects['conflict'] = True
    with pytest.raises(HTTPError if invalid == 'conflict' else ValueError):
        projector.reconcile()
    assert not patches
    assert current['data']['config.yaml'] == original
