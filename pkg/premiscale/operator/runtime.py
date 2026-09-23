"""
Serve Kopf health and resource watches in a supervised child's main event loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import asyncio
import kopf
from setproctitle import setproctitle

from premiscale.status.store import current_store
from .handlers import build_registry

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config) -> None:
    """
    Run a namespaced operator with native signal handling and HTTP liveness probing.

    Args:
        config (Config): Listener, namespace, and provider identity settings.

    Returns:
        None: No value is returned before the operator stops.

    Raises:
        RuntimeError: If no parent-owned runtime directory is available.
    """
    setproctitle('premiscale-operator')
    store = current_store()
    if store is None:
        raise RuntimeError('The operator requires a supervised runtime directory')
    settings = config.controller.healthcheck
    host = f'[{settings.host}]' if ':' in settings.host else settings.host
    asyncio.run(kopf.operator(
        registry=build_registry(config, store),
        standalone=True,
        namespaces=[config.controller.kubernetes.namespace],
        liveness_endpoint=f'http://{host}:{settings.port}/healthz',
    ))
