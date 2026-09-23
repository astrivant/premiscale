"""
Validate CRD-derived schemas and Helm generation of arbitrary resource collections.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING

from jsonschema import Draft7Validator
import pytest
from ruamel.yaml import YAML

if TYPE_CHECKING:
    from typing import Any


ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / 'charts/premiscale-crds'
HELM = os.environ.get('PREMISCALE_TEST_HELM', shutil.which('helm'))
COLLECTIONS = {'controllerConfigs': 'ControllerConfig', 'hosts': 'Host', 'autoscalingGroups': 'AutoscalingGroup'}


def _render(values: dict[str, Any], path: Path) -> subprocess.CompletedProcess[str]:
    """
    Render the chart and installed CRDs using an isolated values file.

    Args:
        values (dict[str, Any]): Chart values to render.
        path (Path): Destination for the temporary values file.

    Returns:
        subprocess.CompletedProcess[str]: Helm output and validation status.
    """
    if HELM is None:
        pytest.skip('Helm is required for chart rendering')
    YAML().dump(values, path)
    return subprocess.run([HELM, 'template', 'test', str(CHART), '--namespace', 'premiscale-test',
                           '--include-crds', '--values', str(path)],
                          capture_output=True, text=True, check=False, timeout=30)


def test_generated_schemas_match_canonical_crds() -> None:
    """
    Detect drift between the CRDs, Helm contracts, and packaged local schema.

    Returns:
        None: No value is returned.
    """
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/schemas/generate-config-schemas.py'), '--check'],
                            capture_output=True, text=True, check=False, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    for path in [CHART / 'values.schema.json', *sorted((CHART / 'schemas').glob('*.json')),
                 ROOT / 'pkg/premiscale/config/schemas/schema.v1alpha1.json']:
        Draft7Validator.check_schema(json.loads(path.read_text()))


@pytest.mark.parametrize('count', [0, 2, 12])
def test_values_render_any_number_of_each_kind(count: int, tmp_path: Path) -> None:
    """
    Render empty or repeated resource collections without a fixed chart limit.

    Args:
        count (int): Number of resources to render for every kind.
        tmp_path (Path): Directory for the test values file.

    Returns:
        None: No value is returned.
    """
    examples = YAML(typ='safe').load(CHART / 'examples/multiple-resources.yaml')
    values: dict[str, Any] = {'commonLabels': {'team': 'infrastructure'},
                              'commonAnnotations': {'example.premiscale.com/test': 'true'}}
    for collection in COLLECTIONS:
        template = next(iter(examples[collection].values()))
        values[collection] = {f'resource-{index}': deepcopy(template) for index in range(count)}
        if count:
            values[collection]['resource-0'].update(namespace='another-namespace',
                                                    labels={'team': 'custom'}, annotations={'owner': 'test'})
    result = _render(values, tmp_path / 'values.yaml')
    assert result.returncode == 0, result.stderr
    resources = list(YAML(typ='safe').load_all(result.stdout))
    kinds = Counter(resource['kind'] for resource in resources)
    assert kinds['CustomResourceDefinition'] == 3
    for kind in COLLECTIONS.values():
        assert kinds[kind] == count
    for resource in resources:
        if resource['kind'] == 'CustomResourceDefinition':
            assert resource['spec']['scope'] == 'Namespaced'
            continue
        schema = json.loads((CHART / 'schemas' / f'{resource["kind"]}.json').read_text())
        Draft7Validator(schema).validate(resource)
        metadata = resource['metadata']
        overridden = metadata['name'] == 'resource-0'
        assert metadata['namespace'] == ('another-namespace' if overridden else 'premiscale-test')
        assert metadata['labels']['team'] == ('custom' if overridden else 'infrastructure')
        assert metadata['annotations']['example.premiscale.com/test'] == 'true'
        if overridden:
            assert metadata['annotations']['owner'] == 'test'


@pytest.mark.parametrize('entry', [
    {}, {'spec': {'address': '192.0.2.1'}},
    {'spec': {'address': '192.0.2.1', 'port': 65536, 'protocol': 'ssh', 'hypervisor': 'qemu'}},
    {'spec': {'address': 7, 'port': 22, 'protocol': 'ssh', 'hypervisor': 'qemu'}},
    {'spec': {'address': '192.0.2.1', 'port': 22, 'protocol': 'ssh', 'hypervisor': 'qemu', 'unknown': True}},
])
def test_chart_rejects_invalid_resource_specs(entry: dict[str, Any], tmp_path: Path) -> None:
    """
    Reject malformed custom resources before Helm emits manifests.

    Args:
        entry (dict[str, Any]): Invalid Host chart entry.
        tmp_path (Path): Directory for the test values file.

    Returns:
        None: No value is returned.
    """
    result = _render({'hosts': {'invalid': entry}}, tmp_path / 'values.yaml')
    assert result.returncode != 0
    assert 'schema' in result.stderr.lower()


def test_autoscaling_group_hosts_are_resource_references() -> None:
    """
    Require named Host references in CRs while retaining local inline-host compatibility.

    Returns:
        None: No value is returned.
    """
    example = YAML(typ='safe').load(CHART / 'examples/multiple-resources.yaml')
    group = example['autoscalingGroups']['workers-primary']['spec']
    schema = json.loads((CHART / 'schemas/AutoscalingGroup.json').read_text())['properties']['spec']
    validator = Draft7Validator(schema)
    assert validator.is_valid(group)
    group['hosts'] = [{'name': 'hypervisor-primary'}]
    assert not validator.is_valid(group)
    group['hosts'] = ['hypervisor-primary', 'hypervisor-primary']
    assert not validator.is_valid(group)
