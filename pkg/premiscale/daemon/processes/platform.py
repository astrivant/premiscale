"""
Register and instantiate the platform client inside its worker process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.messaging.channels import platform_queue
from premiscale.status.store import ready

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config, version: str, token: str) -> None:
    """
    Allow registration to decline, but treat a connected service's return as a failure.

    Args:
        config (Config): Parsed controller configuration.
        version (str): Controller version advertised during platform registration.
        token (str): Platform registration token; an empty token disables registration.

    Returns:
        None: No value is returned.

    Raises:
        RuntimeError: Platform service exited unexpectedly.
    """
    from premiscale.platform import Platform

    client = Platform.register(
        version=version, token=token, host=config.controller.platform.domain,
        cacert=config.controller.platform.certificates.path,
    )
    if client is not None:
        with platform_queue(config.controller.broker) as messages:
            ready()
            client(messages)
        raise RuntimeError('Platform service exited unexpectedly')
