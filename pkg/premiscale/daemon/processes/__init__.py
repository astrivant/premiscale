"""
Select importable process entry points for each controller mode.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from premiscale.daemon.settings import Execution
from . import api, autoscaling, kubernetes, leadership, operator, platform, reconciliation

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any
    from premiscale.config.v1alpha1 import Config


@dataclass(frozen=True)
class ProcessSpec:
    """
    Describe a service without constructing live clients in the parent process.

    Attributes:
        name (str): Configured resource name.
        target (Callable[..., int | None]): Importable service entry point.
        args (tuple[Any, ...]): Arguments supplied to the service entry point.
        required (bool): Whether any unexpected exit must stop the controller.
    """

    name: str
    target: Callable[..., int | None]
    args: tuple[Any, ...] = ()
    required: bool = True


def build(config: Config, version: str, token: str, execution: Execution | None = None) -> list[ProcessSpec]:
    """
    Select services and their arguments without performing network or database IO.

    Args:
        config (Config): Parsed controller configuration.
        version (str): Controller version advertised during platform registration.
        token (str): Platform registration token; an empty token disables registration.
        execution (Execution | None): Optional distributed controller or worker responsibilities.

    Returns:
        list[ProcessSpec]: Required and optional service specifications for the selected controller mode.

    Raises:
        ValueError: If the controller mode is unsupported.
    """
    mode = config.controller.mode
    if mode not in {'standalone', 'standalone-external-metrics', 'kubernetes', 'kubernetes-external-metrics'}:
        raise ValueError(f'Unknown controller mode: {mode}')
    settings = execution or Execution()
    settings.validate(config)
    api_args = (config,) if execution is None else (config, settings)
    if settings.distributed:
        processes = [ProcessSpec('api', api.run, api_args)]
        if settings.role == 'controller':
            processes.append(ProcessSpec('leadership', leadership.run, (config, version, token, settings)))
        if settings.mode == 'ha' or settings.role != 'controller':
            processes.append(ProcessSpec('reconciliation', reconciliation.run, (config, settings, 'work')))
        return processes
    processes = [
        ProcessSpec('api', api.run, api_args),
        ProcessSpec('platform', platform.run, (config, version, token), required=False),
        ProcessSpec('autoscaling', autoscaling.run, (config,)),
    ]
    if mode != 'kubernetes-external-metrics':
        processes.append(ProcessSpec('reconciliation', reconciliation.run, (config,)))
    if mode.startswith('kubernetes'):
        processes.append(ProcessSpec('kubernetes', kubernetes.run, (config,)))
        processes.append(ProcessSpec('operator', operator.run, (config,)))
    return processes


def leader_processes(config: Config, version: str, token: str, execution: Execution) -> list[ProcessSpec]:
    """
    Select services whose entire process lifetimes require confirmed leadership.

    Args:
        config (Config): Infrastructure and connection settings.
        version (str): Version advertised during optional registration.
        token (str): Optional platform registration token.
        execution (Execution): HA composition and distributed subscribers.

    Returns:
        list[ProcessSpec]: Single-writer services to stop before releasing leadership.
    """
    return [
        ProcessSpec('platform', platform.run, (config, version, token), required=False),
        ProcessSpec('autoscaling', autoscaling.run, (config,)),
        ProcessSpec('reconciliation', reconciliation.run, (config, execution, 'leader')),
        ProcessSpec('kubernetes', kubernetes.run, (config,)),
        ProcessSpec('operator', operator.run, (config,)),
    ]
