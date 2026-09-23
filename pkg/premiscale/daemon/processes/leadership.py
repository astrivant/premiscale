"""
Start election and supervision of the controller's single-writer process tree.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


def run(config: Config, version: str, token: str, execution: Execution) -> int:
    """
    Elect and supervise the operator and provider inside a dedicated process group.

    Args:
        config (Config): Infrastructure and shared journal settings.
        version (str): Version advertised during optional registration.
        token (str): Optional platform registration token.
        execution (Execution): Deployment composition and distributed subscribers.

    Returns:
        int: Leadership service shutdown or failure status.
    """
    from premiscale.operator.leadership import run as elect

    return elect(config, version, token, execution)
