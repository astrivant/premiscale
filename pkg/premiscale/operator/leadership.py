"""
Start and stop all single-writer services under one elected controller lifetime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import logging
import multiprocessing as mp
import os
from time import monotonic, sleep

from requests import RequestException

from premiscale.connections.journal import Ownership, schema_name
from premiscale.daemon.processes import leader_processes
from premiscale.daemon.shutdown import Shutdown, signals
from premiscale.daemon.supervisor import Supervisor
from premiscale.status.health import report
from premiscale.status.store import StatusStore, current_store, ready
from .election import Lease

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


log = logging.getLogger(__name__)


def run(config: Config, version: str, token: str, execution: Execution) -> int:
    """
    Keep workers active on followers and fail closed when controller ownership is lost.

    Args:
        config (Config): Infrastructure, shared journal, and namespace settings.
        version (str): Version advertised to the optional platform.
        token (str): Optional platform registration token.
        execution (Execution): HA composition and distributed subscribers.

    Returns:
        int: Zero for requested shutdown, otherwise a child failure status.

    Raises:
        RuntimeError: If supervision is unavailable or leadership expires or changes.
    """
    parent = current_store()
    if parent is None:
        raise RuntimeError('Leader election requires supervised process state')
    settings = config.controller.kubernetes
    name = os.getenv('PREMISCALE_LEASE_NAME', schema_name(settings.clusterName).replace('_', '-'))
    lease = Lease(settings.namespace, name)
    directory = parent.directory / 'leader'
    directory.mkdir(mode=0o700, exist_ok=True)
    store = StatusStore(directory)
    stopped = Shutdown()
    supervisor = None
    owner = None
    advertised = False
    next_renewal = 0.0
    next_report = 0.0
    with signals(stopped):
        try:
            lease.advertise(False)
            parent.write('leadership', {'phase': 'candidate', 'lease': name})
            ready()
            while not stopped.is_set():
                if supervisor is not None and not lease.valid:
                    raise RuntimeError('Controller leadership expired; stopping all single-writer services')
                if monotonic() >= next_renewal:
                    try:
                        acquired = lease.step()
                    except RequestException as error:
                        log.warning('Lease renewal unavailable (%s)', type(error).__name__)
                        acquired = False
                    next_renewal = monotonic() + lease.retry_period
                    if supervisor is not None and not lease.valid:
                        raise RuntimeError('Controller leadership changed or expired')
                    if acquired and supervisor is None:
                        owner = Ownership(settings.stateDsn, settings.clusterName, 'leader')
                        if not lease.valid:
                            raise RuntimeError('Leadership expired while acquiring journal ownership')
                        supervisor = Supervisor(leader_processes(config, version, token, execution),
                                                mp.get_context('spawn'), shutdown_timeout=5, kill_timeout=1,
                                                isolate_process_groups=False, status_store=store)
                        supervisor.start(stopped)
                    if owner is not None:
                        owner.check()
                if supervisor is not None:
                    status = supervisor.poll()
                    if status is not None:
                        return status
                    if not advertised and report(store)['status'] == 'OK' and lease.valid:
                        lease.advertise(True)
                        advertised = True
                else:
                    sleep(0.1)
                if monotonic() >= next_report:
                    parent.write('leadership', {'phase': 'leader' if advertised else 'candidate', 'lease': name})
                    next_report = monotonic() + 1
            return 0
        finally:
            try:
                try:
                    lease.advertise(False)
                except RequestException:
                    log.warning('Could not clear the provider routing label; local provider is stopping')
                if supervisor is not None:
                    supervisor.close()
                # Never release ahead of child cleanup, including the Go provider and VM thread.
                if owner is not None:
                    owner.close()
                try:
                    lease.release()
                except RequestException:
                    log.warning('Lease release unavailable; another candidate will wait for expiry')
            finally:
                lease.close()
