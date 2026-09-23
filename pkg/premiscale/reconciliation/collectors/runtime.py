"""
Own one collection subprocess per available CPU in either transport composition.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from premiscale.reconciliation.capacity import available_cores
from . import partitioned, queued

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.processes import ProcessSpec


def processes(config: Config) -> 'list[ProcessSpec]':
    """
    Partition hosts over one collection subprocess per available logical CPU.

    Args:
        config (Config): Controller mode and configured hypervisor hosts.

    Returns:
        list[ProcessSpec]: Required collectors, or no collectors in external-metrics modes.
    """
    from premiscale.daemon.processes import ProcessSpec

    if config.controller.mode.endswith('-external-metrics'):
        return []
    count = available_cores()
    hosts = config.controller.autoscale.hosts
    return [ProcessSpec(f'collector-{index}', partitioned.run, (config, hosts[index::count], index))
            for index in range(count)]


def run(config: Config, shared: bool = False) -> int:
    """
    Supervise partitioned singular collectors or shared HA request consumers.

    Args:
        config (Config): Host inventory, transport, and concurrency settings.
        shared (bool): Consume centrally scheduled requests across replicas when True.

    Returns:
        int: Process pool shutdown or failure status.
    """
    from premiscale.daemon.processes import ProcessSpec
    from premiscale.reconciliation.supervision import run_pool

    count = available_cores()
    specs = [ProcessSpec(f'collector-{index}', queued.run, (config,)) for index in range(count)] if shared else processes(config)
    return run_pool(specs, 'collectors')
