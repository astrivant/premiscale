"""
Start Kopf inside its own supervised process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config) -> None:
    """
    Construct the operator after the child process has been spawned.

    Args:
        config (Config): Namespaced controller settings.

    Returns:
        None: No value is returned before operator shutdown.
    """
    from premiscale.operator.runtime import run as operate

    operate(config)
