"""
Own independently acknowledged consumers and bounded database connection processes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from . import local, state, worker

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


def run(config: Config, names: tuple[str, ...], execution: Execution) -> int:
    """
    Spawn bounded remote database consumers and exactly one writer for each local destination.

    Args:
        config (Config): Destination and transport settings.
        names (tuple[str, ...]): Database subscribers owned by this reconciliation pool.
        execution (Execution): Per-subscriber connection limits.

    Returns:
        int: Process pool shutdown or failure status.
    """
    from premiscale.daemon.processes import ProcessSpec
    from premiscale.reconciliation.supervision import run_pool

    destinations = config.controller.databases.destinations
    specs = []
    for name in names:
        if name == '_state':
            specs.append(ProcessSpec('publisher-state', state.run, (config,)))
            continue
        settings = destinations[name]
        count = execution.publisher_limits.get(name, execution.connections) if settings.type in {'postgresql', 'influxdb'} else 1
        for index in range(count):
            remote = settings.type in {'postgresql', 'influxdb'}
            specs.append(ProcessSpec(f'publisher-{name}-{index}', worker.run if remote else local.run,
                                     (config, name) if remote else (config, name, settings)))
    return run_pool(specs, 'publishers')
