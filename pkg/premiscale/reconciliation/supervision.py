"""
Supervise nested collection and publication pools inside their parent's process group.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import multiprocessing as mp
from pathlib import Path

from premiscale.daemon.shutdown import Shutdown, signals
from premiscale.daemon.supervisor import Supervisor
from premiscale.status.health import report
from premiscale.status.store import StatusStore, current_store, ready

if TYPE_CHECKING:
    from premiscale.daemon.processes import ProcessSpec


def run_pool(specs: list[ProcessSpec], name: str) -> int:
    """
    Propagate pool readiness and failures while preserving process-group cleanup.

    Args:
        specs (list[ProcessSpec]): Importable child process specifications.
        name (str): Private status directory for this nested pool.

    Returns:
        int: Zero for requested shutdown, otherwise the failed child status.
    """
    parent = current_store()
    store = None
    if parent is not None:
        directory = Path(parent.directory) / name
        directory.mkdir(mode=0o700, exist_ok=True)
        store = StatusStore(directory)
    stopped = Shutdown()
    supervisor = Supervisor(specs, mp.get_context('spawn'), shutdown_timeout=2, kill_timeout=1,
                            isolate_process_groups=False, status_store=store)
    with signals(stopped):
        try:
            supervisor.start(stopped)
            initialized = False
            while not stopped.is_set():
                status = supervisor.poll()
                if status is not None:
                    return status
                if not initialized and (store is None or report(store)['status'] == 'OK'):
                    ready()
                    initialized = True
            return 0
        finally:
            supervisor.close()
