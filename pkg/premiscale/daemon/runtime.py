"""
Coordinate parent signal handling and supervise every service as a child process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import logging
import multiprocessing as mp
from pathlib import Path
from tempfile import TemporaryDirectory

from setproctitle import setproctitle

from premiscale.status.store import StatusStore
from .processes import build
from .shutdown import Shutdown, signals
from .supervisor import Supervisor

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


log = logging.getLogger(__name__)


def start(config: Config, version: str, token: str, execution: Execution | None = None) -> int:
    """
    Run the controller until a signal, startup failure, or unexpected service exit.

    Requested shutdown returns zero. Service or cleanup failures return nonzero.
    This entry point runs in the main thread and uses spawned POSIX processes.

    Args:
        config (Config): Parsed controller configuration.
        version (str): Controller version advertised during platform registration.
        token (str): Platform registration token; an empty token disables registration.
        execution (Execution | None): Optional controller or scalable worker process selection.

    Returns:
        int: Zero for requested shutdown; nonzero for service startup, runtime, or cleanup failure.
    """
    setproctitle('premiscale')
    stopped = Shutdown()
    context = mp.get_context('spawn')
    try:
        directory = Path(config.controller.healthcheck.stateDirectory).expanduser()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with signals(stopped), TemporaryDirectory(prefix='run-', dir=directory) as shared:
            specs = build(config, version, token) if execution is None else build(config, version, token, execution)
            supervisor = Supervisor(specs, context, status_store=StatusStore(shared))
            try:
                supervisor.start(stopped)
                return supervisor.wait(stopped)
            finally:
                supervisor.close()
    except (Exception, SystemExit):
        log.exception('Controller failed; shutting down')
        return 1
