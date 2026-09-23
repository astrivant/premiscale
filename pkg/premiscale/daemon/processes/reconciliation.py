"""
Instantiate internal reconciliation inside its worker process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


def run(config: Config, execution: Execution | None = None, scope: str = 'full') -> int:
    """
    Start reconciliation with ownership of its nested collection processes.

    Args:
        config (Config): Parsed controller configuration.
        execution (Execution | None): Deployment composition and subscriber ownership.
        scope (str): Full singular pipeline, shared work, or elected leader responsibilities.

    Returns:
        int: Reconciliation's shutdown or failure status.
    """
    from premiscale.reconciliation.runtime import run as reconcile

    return reconcile(config, execution, scope)
