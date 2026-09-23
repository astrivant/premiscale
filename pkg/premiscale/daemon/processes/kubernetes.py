"""
Instantiate the Kubernetes provider and its Go child inside its worker process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


def run(config: Config) -> None:
    """
    Supervise the external gRPC cloud provider and VM lifecycle worker.

    Args:
        config (Config): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    from premiscale_cluster_autoscaler import KubernetesAutoscaler

    KubernetesAutoscaler(config)()
