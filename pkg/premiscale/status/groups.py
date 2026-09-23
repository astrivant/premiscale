"""
Compare observed provider configuration and derive AutoscalingGroup conditions.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Any
    from .store import StatusStore


COUNTS = ('targetReplicas', 'runningReplicas', 'pendingReplicas', 'deletingReplicas', 'failedReplicas')


def configuration_digest(spec: dict[str, Any]) -> str:
    """
    Normalize local attrs representations and CR defaults before comparing them.

    Args:
        spec (dict[str, Any]): Unstructured local group or Kubernetes group specification.

    Returns:
        str: SHA-256 digest used only inside the private runtime store.
    """
    normalized = deepcopy(spec)
    cloud_init = normalized.pop('cloudInit', normalized.get('cloud-init', {}))
    normalized['cloud-init'] = {'inline': '', 'file': '', **cloud_init}
    normalized.setdefault('nodeLabels', {})
    normalized.setdefault('nodeTaints', [])
    normalized.setdefault('maxPods', 110)
    normalized['hosts'] = sorted(host if isinstance(host, str) else host['name'] for host in normalized['hosts'])
    encoded = json.dumps(normalized, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def group_status(name: str, generation: int, spec: dict[str, Any], previous: dict[str, Any],
                 store: StatusStore, now: datetime | None = None) -> dict[str, Any]:
    """
    Describe observed lifecycle state and explicitly identify unapplied CR configuration.

    Args:
        name (str): CR metadata name, also used as the local autoscaling group key.
        generation (int): Kubernetes generation whose specification is being evaluated.
        spec (dict[str, Any]): Current CR specification.
        previous (dict[str, Any]): Existing status used to preserve condition transition times.
        store (StatusStore): Reader for the provider's cached shared observation.
        now (datetime | None): Current UTC time, optionally supplied by tests.

    Returns:
        dict[str, Any]: Status merge patch containing counts, freshness, and typed conditions.
    """
    timestamp = (now or datetime.now(timezone.utc)).isoformat().replace('+00:00', 'Z')
    snapshot = store.read('autoscaler')
    if snapshot is not None and not isinstance(snapshot['data'].get('groups'), dict):
        snapshot = None
    observation = snapshot['data'].get('groups', {}).get(name) if snapshot else None
    if observation is not None and (
        not isinstance(observation, dict) or not isinstance(observation.get('configurationDigest'), str)
        or any(type(observation.get(key)) is not int or observation[key] < 0 for key in COUNTS)
    ):
        snapshot = None
        observation = None
    result: dict[str, Any] = {'observedGeneration': generation, **dict.fromkeys(COUNTS)}
    result['lastObservationTime'] = None
    states = {
        'Configured': ('Unknown', 'ProviderUnavailable', 'No fresh provider observation is available.'),
        'Ready': ('Unknown', 'ProviderUnavailable', 'No fresh provider observation is available.'),
        'Progressing': ('Unknown', 'ProviderUnavailable', 'No fresh provider observation is available.'),
        'Degraded': ('Unknown', 'ProviderUnavailable', 'No fresh provider observation is available.'),
    }
    if snapshot is not None:
        result['lastObservationTime'] = datetime.fromtimestamp(snapshot['updatedAt'], timezone.utc).isoformat().replace('+00:00', 'Z')
        if observation is None:
            states['Configured'] = ('False', 'GroupNotConfigured', 'This group is absent from the running provider configuration.')
            states['Ready'] = states['Configured']
        else:
            result.update({key: observation[key] for key in COUNTS})
            configured = observation['configurationDigest'] == configuration_digest(spec)
            progressing = bool(observation['pendingReplicas'] or observation['deletingReplicas'])
            failed = bool(observation['failedReplicas'])
            bounded = spec['scaling']['minNodes'] <= observation['targetReplicas'] <= spec['scaling']['maxNodes']
            states['Configured'] = (('True', 'ConfigurationMatches', 'The provider has loaded this group specification.') if configured else
                                    ('False', 'ConfigurationMismatch', 'Update the controller configuration and restart to apply this specification.'))
            states['Progressing'] = (('True', 'OperationsPending', 'VM lifecycle operations are pending.') if progressing else
                                     ('False', 'NoPendingOperations', 'No VM lifecycle operations are pending.'))
            states['Degraded'] = (('True', 'OperationFailed', 'At least one VM lifecycle operation failed; inspect the provider logs.') if failed else
                                  ('False', 'NoOperationFailures', 'No failed VM lifecycle operations are recorded.'))
            if not configured:
                states['Ready'] = states['Configured']
            elif failed:
                states['Ready'] = ('False', 'OperationFailed', 'VM lifecycle failures prevent readiness.')
            elif progressing:
                states['Ready'] = ('False', 'OperationsPending', 'Waiting for VM lifecycle operations to complete.')
            elif not bounded:
                states['Ready'] = ('False', 'OutsideScalingBounds', 'The current target is outside the configured node limits.')
            else:
                states['Ready'] = ('True', 'ProviderConverged', 'Provider operations are complete; Kubernetes Node readiness is not assessed.')
    old_conditions = {condition['type']: condition for condition in previous.get('conditions', [])}
    conditions = [condition for kind, condition in old_conditions.items() if kind not in states]
    for kind, (state, reason, message) in states.items():
        old = old_conditions.get(kind, {})
        transitioned = old.get('lastTransitionTime', timestamp) if old.get('status') == state else timestamp
        conditions.append({'type': kind, 'status': state, 'reason': reason, 'message': message,
                           'observedGeneration': generation, 'lastTransitionTime': transitioned})
    result['conditions'] = conditions
    return result
