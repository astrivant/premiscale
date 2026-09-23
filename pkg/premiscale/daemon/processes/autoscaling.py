"""
Instantiate the action consumer inside its worker process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.messaging.channels import action_queue
from premiscale.status.store import ready

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config) -> None:
    """
    Run the autoscaling action queue until shutdown.

    Args:
        config (Config): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    from premiscale.autoscaling.group import Autoscaler

    with action_queue(config.controller.broker) as actions:
        ready()
        Autoscaler(config)(actions)
