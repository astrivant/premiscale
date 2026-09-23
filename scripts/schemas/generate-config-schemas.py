"""
Derive Helm, custom-resource, and local configuration schemas from canonical CRDs.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
from typing import TYPE_CHECKING

from ruamel.yaml import YAML

if TYPE_CHECKING:
    from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DRAFT = 'http://json-schema.org/draft-07/schema#'
RESOURCE_NAME = {
    'type': 'string', 'minLength': 1, 'maxLength': 253,
    'pattern': r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$',
}
NAMESPACE = {'type': 'string', 'minLength': 1, 'maxLength': 63,
             'pattern': r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'}
STRING_MAP = {'type': 'object', 'additionalProperties': {'type': 'string'}}


def json_schema(value: Any) -> Any:
    """
    Convert structural OpenAPI constraints to strict JSON Schema draft seven.

    Args:
        value (Any): CRD schema node or scalar to convert.

    Returns:
        Any: Independent JSON Schema with Kubernetes extensions removed.
    """
    if isinstance(value, list):
        return [json_schema(item) for item in value]
    if not isinstance(value, dict):
        return value
    converted = {key: json_schema(item) for key, item in value.items() if not key.startswith('x-kubernetes-')}
    for limit in ('Minimum', 'Maximum'):
        exclusive = converted.get(f'exclusive{limit}')
        if isinstance(exclusive, bool):
            del converted[f'exclusive{limit}']
            if exclusive:
                converted[f'exclusive{limit}'] = converted.pop(limit.lower())
    if converted.get('type') == 'object' and 'properties' in converted:
        converted.setdefault('additionalProperties', False)
    if value.get('x-kubernetes-list-type') == 'set':
        converted['uniqueItems'] = True
    return converted


def object_schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    """
    Define a closed object with explicit required fields.

    Args:
        properties (dict[str, Any]): Field schemas indexed by name.
        required (list[str]): Fields every object must contain.

    Returns:
        dict[str, Any]: Strict object schema.
    """
    return {'type': 'object', 'properties': properties, 'required': required, 'additionalProperties': False}


def resource_schema(kind: str, schema: dict[str, Any]) -> dict[str, Any]:
    """
    Add Kubernetes resource identity to a CRD-derived JSON schema.

    Args:
        kind (str): Custom resource kind.
        schema (dict[str, Any]): Structural schema converted from its CRD.

    Returns:
        dict[str, Any]: Whole-resource schema for rendered manifest validation.
    """
    result = deepcopy(schema)
    result.update({'$schema': DRAFT, 'title': kind, 'required': ['apiVersion', 'kind', 'metadata', 'spec']})
    result['properties']['apiVersion'] = {'type': 'string', 'const': 'premiscale.com/v1alpha1'}
    result['properties']['kind'] = {'type': 'string', 'const': kind}
    result['properties']['metadata'] = {
        'type': 'object', 'required': ['name'],
        'properties': {'name': RESOURCE_NAME, 'namespace': NAMESPACE,
                       'labels': STRING_MAP, 'annotations': STRING_MAP},
    }
    return result


def local_schema(specs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Adapt resource identities and references to the existing local YAML format.

    Args:
        specs (dict[str, dict[str, Any]]): Strict resource specifications indexed by kind.

    Returns:
        dict[str, Any]: Packaged validation schema for file-based controller startup.
    """
    host = deepcopy(specs['Host'])
    host['properties']['name'] = {'type': 'string', 'minLength': 1}
    host['required'].append('name')
    group = deepcopy(specs['AutoscalingGroup'])
    group['properties']['hosts'] = {'anyOf': [
        {'type': 'array', 'items': host},
        {'type': 'array', 'minItems': 1, 'items': {'type': 'string', 'minLength': 1}},
    ]}
    controller = deepcopy(specs['ControllerConfig'])
    controller['properties']['autoscale'] = object_schema({
        'hosts': {'type': 'array', 'items': host},
        'groups': {'type': 'object', 'additionalProperties': group},
    }, ['hosts', 'groups'])
    return {'$schema': DRAFT, '$comment': 'Generated from charts/premiscale-crds/crds; do not edit.',
            **object_schema({'version': {'type': 'string', 'const': 'v1alpha1'},
                             'controller': controller}, ['version', 'controller'])}


def artifacts(root: Path) -> dict[Path, dict[str, Any]]:
    """
    Build every derived artifact using only CRDs as field-definition sources.

    Args:
        root (Path): Repository root containing the CRD chart and Python package.

    Returns:
        dict[Path, dict[str, Any]]: Destination paths and generated schema documents.
    """
    chart = root / 'charts/premiscale-crds'
    result = {}
    specs = {}
    properties: dict[str, Any] = {
        'global': {'type': 'object', 'description': 'Inherited parent-chart values; namespace sets the default resource namespace.'},
        'commonLabels': {**STRING_MAP, 'description': 'Labels applied to every custom resource.'},
        'commonAnnotations': {**STRING_MAP, 'description': 'Annotations applied to every custom resource.'},
    }
    value_keys = {'ControllerConfig': 'controllerConfigs', 'Host': 'hosts', 'AutoscalingGroup': 'autoscalingGroups'}
    for path in sorted((chart / 'crds').glob('*.yaml')):
        crd = YAML(typ='safe').load(path)
        kind = crd['spec']['names']['kind']
        version = next(version for version in crd['spec']['versions'] if version['name'] == 'v1alpha1')
        schema = json_schema(version['schema']['openAPIV3Schema'])
        specs[kind] = schema['properties']['spec']
        result[chart / 'schemas' / f'{kind}.json'] = resource_schema(kind, schema)
        properties[value_keys[kind]] = {
            'type': 'object', 'description': f'Named {kind} resources; map keys become metadata.name.',
            'propertyNames': RESOURCE_NAME,
            'patternProperties': {RESOURCE_NAME['pattern']: object_schema({
                'namespace': NAMESPACE, 'labels': STRING_MAP, 'annotations': STRING_MAP,
                'spec': specs[kind],
            }, ['spec'])},
            'additionalProperties': False,
        }
    result[chart / 'values.schema.json'] = {
        '$schema': DRAFT, '$comment': 'Generated from crds/; run scripts/schemas/generate-config-schemas.py.',
        **object_schema(properties, list(properties)),
    }
    runtime = local_schema(specs)
    result[root / 'pkg/premiscale/config/schemas/schema.v1alpha1.json'] = runtime
    parent = root / 'charts/premiscale/values.schema.json'
    parent_schema = json.loads(parent.read_text(encoding='utf-8'))
    parent_schema['properties']['config'] = deepcopy(runtime)
    result[parent] = parent_schema
    return result


def main() -> int:
    """
    Write derived schemas or fail when checked-in artifacts differ from the CRDs.

    Returns:
        int: Zero when artifacts are current or updated, otherwise one.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Report stale generated files without changing them.')
    options = parser.parse_args()
    stale = []
    for path, schema in artifacts(ROOT).items():
        content = json.dumps(schema, indent=2, ensure_ascii=False, allow_nan=False) + '\n'
        if not path.exists() or path.read_text(encoding='utf-8') != content:
            if options.check:
                stale.append(str(path.relative_to(ROOT)))
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding='utf-8')
    for stale_path in stale:
        print(f'Generated schema is stale: {stale_path}')
    return int(bool(stale))


if __name__ == '__main__':
    raise SystemExit(main())
