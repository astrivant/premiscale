"""
Derive liveness and readiness from the parent's and children's shared observations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .store import StatusStore

if TYPE_CHECKING:
    from typing import Any


def report(store: StatusStore | None, require_ready: bool = True) -> dict[str, Any]:
    """
    Fail closed when supervision is stale or required services are unavailable.

    Args:
        store (StatusStore | None): Cached reader for this controller run.
        require_ready (bool): Require completed initialization as well as live processes.

    Returns:
        dict[str, Any]: Health status and non-sensitive per-service state.
    """
    snapshot = store.read('supervisor') if store is not None else None
    if snapshot is None or snapshot['data'].get('phase') != 'running':
        return {'status': 'unavailable', 'reason': 'SupervisorUnavailable', 'services': {}}
    expected = snapshot['data'].get('services')
    if not isinstance(expected, dict) or not expected:
        return {'status': 'unavailable', 'reason': 'SupervisorUnavailable', 'services': {}}
    services = {}
    healthy = True
    for name, process in expected.items():
        if not isinstance(process, dict) or 'pid' not in process or 'required' not in process:
            return {'status': 'unavailable', 'reason': 'InvalidProcessState', 'services': {}}
        observed = store.read(f'process-{name}', max_age=None) if store is not None else None
        phase = observed['data'].get('phase', 'starting') if observed else 'starting'
        if observed and observed.get('pid') != process['pid']:
            phase = 'starting'
        services[name] = phase
        if process['required']:
            healthy &= bool(observed and observed.get('pid') == process['pid'])
            healthy &= phase == 'ready' if require_ready else phase in {'starting', 'ready'}
    if require_ready and 'kubernetes' in services and store is not None:
        healthy &= store.read('autoscaler') is not None
    if 'leadership' in services and store is not None:
        election = store.read('leadership')
        healthy &= election is not None
        if election is not None and election['data'].get('phase') == 'leader':
            healthy &= report(StatusStore(store.directory / 'leader'), require_ready)['status'] == 'OK'
    return {'status': 'OK' if healthy else 'unavailable', 'services': services}
