"""
Build and serve the controller API inside its supervised child process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from setproctitle import setproctitle
from werkzeug.serving import make_server

from premiscale.api import create_app
from premiscale.status.store import current_store, ready
from premiscale.reconciliation.demand import Demand

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


def run(config: Config, execution: Execution | None = None) -> None:
    """
    Serve in the child's main thread and release the listener on termination.

    Args:
        config (Config): Parsed controller configuration.
        execution (Execution | None): Optional role and distributed worker settings.

    Returns:
        None: No value is returned.
    """
    setproctitle('premiscale-api')
    settings = config.controller.healthcheck
    kubernetes = config.controller.mode.startswith('kubernetes')
    port = settings.apiPort if kubernetes else settings.port
    worker = execution is not None and execution.role != 'controller'
    demand = Demand(config, execution) if execution is not None and execution.distributed and not worker else None
    try:
        app = create_app(status_store=current_store(),
                         liveness=worker or not kubernetes or (execution is not None and execution.distributed),
                         demand=demand)
        with make_server(settings.host, port, app, threaded=True) as server:
            ready()
            server.serve_forever()
    finally:
        if demand is not None:
            demand.close()
