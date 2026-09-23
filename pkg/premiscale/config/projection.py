"""
Resolve namespaced configuration resources into the operator's existing YAML format.
"""

from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from importlib import resources
import json
from typing import TYPE_CHECKING

from jsonschema import Draft7Validator

if TYPE_CHECKING:
    from typing import Any


@lru_cache(maxsize=1)
def _validator() -> Draft7Validator:
    """
    Load the packaged schema derived from the canonical configuration CRDs.

    Returns:
        Draft7Validator: Reusable validator for complete runtime configurations.
    """
    schema = resources.files('premiscale.config.schemas').joinpath('schema.v1alpha1.json')
    return Draft7Validator(json.loads(schema.read_text(encoding='utf-8')))


def project(controller: dict[str, Any], hosts: dict[str, dict[str, Any]],
            groups: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Resolve resource references without expanding credentials into the ConfigMap.

    Args:
        controller (dict[str, Any]): Selected ControllerConfig specification.
        hosts (dict[str, dict[str, Any]]): Host specifications indexed by metadata name.
        groups (dict[str, dict[str, Any]]): AutoscalingGroup specifications indexed by metadata name.

    Returns:
        dict[str, Any]: Complete validated runtime mapping with inline host definitions.

    Raises:
        ValueError: If references are missing, inventory boundaries are crossed, or validation fails.
    """
    config: dict[str, Any] = {'version': 'v1alpha1', 'controller': deepcopy(controller)}
    try:
        references = controller['autoscale']
        inventory = {name: {**deepcopy(hosts[name]), 'name': name} for name in references['hosts']}
        selected = {name: deepcopy(groups[name]) for name in references['groups']}
        if len(inventory) != len(references['hosts']) or len(selected) != len(references['groups']):
            raise ValueError('Configuration references must be unique')
        for group in selected.values():
            if any(name not in inventory for name in group['hosts']):
                raise ValueError('AutoscalingGroup references a Host outside the ControllerConfig inventory')
        config['controller']['autoscale'] = {'hosts': list(inventory.values()), 'groups': selected}
    except (KeyError, TypeError) as error:
        raise ValueError('A referenced configuration resource or required field is missing') from error
    invalid = next(_validator().iter_errors(config), None)
    if invalid is not None:
        raise ValueError(f'Projected configuration violates {invalid.validator} at {invalid.json_path}')
    return config
