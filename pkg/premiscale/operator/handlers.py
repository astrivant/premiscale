"""
Register operator health probes and AutoscalingGroup status timers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import os

import kopf

from premiscale.status.groups import group_status
from premiscale.status.health import report
from premiscale.status.store import ready
from .configuration import register_projection

if TYPE_CHECKING:
    from typing import Any
    from premiscale.config.v1alpha1 import Config
    from premiscale.status.store import StatusStore


def build_registry(config: Config, store: StatusStore) -> kopf.OperatorRegistry:
    """
    Build an isolated registry scoped to this provider's labelled resources.

    Args:
        config (Config): Controller identity and operator settings.
        store (StatusStore): Cached reader for shared runtime observations.

    Returns:
        kopf.OperatorRegistry: Independent startup, probing, and status handlers.
    """
    registry = kopf.OperatorRegistry()
    register_projection(registry, config)
    projected = bool(os.getenv('PREMISCALE_CONTROLLER_CONFIG'))

    @kopf.on.startup(id='configure', registry=registry)
    async def startup(settings: kopf.OperatorSettings, **kwargs: Any) -> None:
        """
        Configure namespace-only discovery and keep framework bookkeeping in annotations.

        Args:
            settings (kopf.OperatorSettings): Operator settings supplied by Kopf.
            **kwargs (Any): Additional framework context.

        Returns:
            None: No value is returned.
        """
        settings.scanning.disabled = True
        settings.posting.enabled = False
        settings.persistence.progress_storage = kopf.AnnotationsProgressStorage(prefix='premiscale.com')
        settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(prefix='premiscale.com')
        ready()

    @kopf.on.probe(id='controller', registry=registry, errors=kopf.ErrorsMode.PERMANENT)
    async def probe(**kwargs: Any) -> dict[str, Any]:
        """
        Report cached process liveness and fail HTTP probes when supervision is stale.

        Args:
            **kwargs (Any): Additional framework context.

        Returns:
            dict[str, Any]: Shared controller health observations.

        Raises:
            kopf.PermanentError: If required process supervision is unavailable.
        """
        health = report(store, require_ready=False)
        if health['status'] != 'OK':
            raise kopf.PermanentError('Controller supervision is unavailable')
        return health

    @kopf.timer('premiscale.com', 'v1alpha1', 'autoscalinggroups', id='autoscalinggroup-status', interval=5, registry=registry,
                labels=None if projected else {'premiscale.com/cluster': config.controller.kubernetes.clusterName})
    async def publish_status(name: str, meta: kopf.Meta, spec: kopf.Spec,
                             status: kopf.Status, patch: kopf.Patch, **kwargs: Any) -> None:
        """
        Patch the status subresource from fresh, committed provider observations.

        Args:
            name (str): AutoscalingGroup metadata name.
            meta (kopf.Meta): Resource metadata including its current generation.
            spec (kopf.Spec): Current custom resource specification.
            status (kopf.Status): Status previously stored by Kubernetes.
            patch (kopf.Patch): Status changes submitted by Kopf after the handler returns.
            **kwargs (Any): Additional framework context.

        Returns:
            None: No value is returned.
        """
        if projected and name not in config.controller.autoscale.groups:
            return
        desired = group_status(name, int(meta.get('generation', 1)), dict(spec), dict(status), store)
        for key, value in desired.items():
            if status.get(key) != value:
                patch.status[key] = value

    return registry
