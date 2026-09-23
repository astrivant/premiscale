"""
Own per-core collection processes and schedule standalone reconciliation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from contextlib import ExitStack
import logging
import multiprocessing as mp
from time import monotonic, sleep

from setproctitle import setproctitle

from premiscale.daemon.settings import Execution
from premiscale.daemon.processes import ProcessSpec
from premiscale.daemon.shutdown import Shutdown, signals
from premiscale.daemon.supervisor import Supervisor
from premiscale.messaging.channels import action_queue, platform_queue
from premiscale.status.store import ready
from .internal import Reconcile
from .collectors import runtime as collectors, schedule
from .publishers import runtime as publishers

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


log = logging.getLogger(__name__)


def pipeline_processes(config: Config, execution: Execution, scope: str = 'full') -> list[ProcessSpec]:
    """
    Select independent collection and publication supervisors for one reconciliation process.

    Args:
        config (Config): Controller mode and configured destinations.
        execution (Execution): Container role and composition.
        scope (str): Full singular pipeline, replica work, or elected leader services.

    Returns:
        list[ProcessSpec]: Required collection, publication, and scheduling supervisors.
    """
    if config.controller.mode.endswith('-external-metrics'):
        return []
    specs = []
    if scope != 'leader' and execution.role != 'publisher':
        specs.append(ProcessSpec('collectors', collectors.run, (config, execution.distributed)))
    names: tuple[str, ...] = ()
    if execution.role == 'publisher':
        names = (execution.publisher,)
    elif execution.role == 'controller':
        if scope == 'full':
            names = tuple(config.controller.databases.destinations)
        elif scope == 'leader':
            names = tuple(name for name in config.controller.databases.destinations if name not in execution.publishers)
        else:
            names = execution.publishers
    if execution.role == 'controller' and scope in {'full', 'leader'}:
        names = ('_state', *names)
    if names:
        specs.append(ProcessSpec('publishers', publishers.run, (config, names, execution)))
    if scope == 'leader':
        specs.append(ProcessSpec('collection-scheduler', schedule.run, (config,)))
    return specs


def run(config: Config, execution: Execution | None = None, scope: str = 'full') -> int:
    """
    Supervise collection and propagate child failure to the controller daemon.

    Nested collectors retain this service's process group so the daemon can
    terminate the entire tree even if reconciliation exits without cleanup.

    Args:
        config (Config): Parsed controller configuration.
        execution (Execution | None): Optional composition and subscriber ownership.
        scope (str): Full singular pipeline, replica work, or elected leader services.

    Returns:
        int: Zero for requested shutdown, or a nonzero collector failure status.
    """
    setproctitle('premiscale-reconciliation')
    settings = execution or Execution()
    specs = pipeline_processes(config, settings, scope)
    if config.controller.mode.startswith('kubernetes'):
        from .supervision import run_pool

        return run_pool(specs, 'reconciliation')
    stopped = Shutdown()
    with signals(stopped), ExitStack() as resources:
        supervisor = Supervisor(specs, mp.get_context('spawn'),
                                shutdown_timeout=5, kill_timeout=1, isolate_process_groups=False)
        resources.callback(supervisor.close)
        supervisor.start(stopped)
        reconcile = None
        if config.controller.mode.startswith('standalone') and not stopped.is_set():
            actions = resources.enter_context(action_queue(config.controller.broker))
            messages = resources.enter_context(platform_queue(config.controller.broker))
            reconcile = Reconcile(config, actions, messages)
        next_pass = monotonic()
        ready()
        log.info('Reconciliation supervising %s pipeline services', len(supervisor.specs))
        while not stopped.is_set():
            if supervisor.specs:
                status = supervisor.poll()
                if stopped.is_set():
                    break
                if status is not None:
                    return status
            else:
                sleep(0.1)
            if reconcile is not None and not stopped.is_set() and monotonic() >= next_pass:
                started = monotonic()
                reconcile.reconcile_once()
                next_pass = started + config.controller.reconciliation.interval
        return 0
