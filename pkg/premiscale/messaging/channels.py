"""
Define explicit JSON payloads for the controller's work streams.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from premiscale.autoscaling.actions import Null
from .queue import RedisQueue

if TYPE_CHECKING:
    from typing import Any
    from premiscale.autoscaling.actions import Action
    from premiscale.config.v1alpha1 import Broker


def encode_action(action: Action) -> dict[str, Any]:
    """
    Serialize supported standalone actions; abstract VM action stubs are not executable.

    Kubernetes VM operations use the external gRPC provider and its durable journal.
    New standalone action implementations must define an explicit codec here.

    Args:
        action (Action): Infrastructure action to represent or encode.

    Returns:
        dict[str, Any]: JSON-compatible representation of a supported standalone action.

    Raises:
        ValueError: Unsupported standalone action.
    """
    if type(action) is not Null or action.modifier != 0:
        raise ValueError('Unsupported standalone action')
    return {'action': 'null'}


def decode_action(payload: Any) -> Action:
    """
    Construct only known actions, never arbitrary classes supplied through the broker.

    Args:
        payload (Any): Application data carried by the message.

    Returns:
        Action: Validated standalone action reconstructed from the payload.

    Raises:
        ValueError: Unsupported standalone action payload.
    """
    if payload != {'action': 'null'}:
        raise ValueError('Unsupported standalone action payload')
    return Null()


def platform_message(payload: Any) -> str:
    """
    Preserve the platform websocket's text message contract.

    Args:
        payload (Any): Application data carried by the message.

    Returns:
        str: Validated text payload, unchanged.

    Raises:
        ValueError: Platform messages must be strings.
    """
    if not isinstance(payload, str):
        raise ValueError('Platform messages must be strings')
    return payload


def action_queue(config: Broker) -> RedisQueue[Action]:
    """
    Build a worker-local action queue without opening a connection yet.

    Args:
        config (Broker): Parsed controller configuration.

    Returns:
        RedisQueue[Action]: A worker-local action queue without opening a connection yet.
    """
    return RedisQueue(config, 'autoscaling', encode=encode_action, decode=decode_action)


def platform_queue(config: Broker) -> RedisQueue[str]:
    """
    Build a worker-local platform queue without opening a connection yet.

    Args:
        config (Broker): Parsed controller configuration.

    Returns:
        RedisQueue[str]: A worker-local platform queue without opening a connection yet.
    """
    return RedisQueue(config, 'platform', encode=platform_message, decode=platform_message)
