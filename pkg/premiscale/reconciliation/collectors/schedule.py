"""
Schedule at most one unfinished collection request per configured host.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import json
from time import sleep
from uuid import uuid4

from premiscale.support.lua import load_lua
from premiscale.messaging.queue import RedisQueue
from premiscale.status.store import ready

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Broker, Config


def host_name(value: object) -> str:
    """
    Validate the host identity without accepting connection settings from the broker.

    Args:
        value (object): Decoded collection payload.

    Returns:
        str: Configured host name to resolve locally.

    Raises:
        ValueError: If the payload is not a nonempty host name.
    """
    if not isinstance(value, str) or not value:
        raise ValueError('Collection requests require a host name')
    return value


def collection_queue(config: Broker) -> RedisQueue[str]:
    """
    Construct the shared collection stream without opening a connection.

    Args:
        config (Broker): Broker connection and namespace settings.

    Returns:
        RedisQueue[str]: Queue shared by every collection replica.
    """
    return RedisQueue(config, 'collection', decode=host_name)


def schedule_once(config: Config, queue: RedisQueue[str]) -> int:
    """
    Atomically enqueue due hosts while retaining unfinished work across restarts.

    Args:
        config (Config): Host inventory and collection interval.
        queue (RedisQueue[str]): Shared host collection stream.

    Returns:
        int: Number of newly scheduled host requests.
    """
    scheduled = 0
    for host in config.controller.autoscale.hosts:
        body = json.dumps({'version': 1, 'id': str(uuid4()), 'data': host.name})
        scheduled += int(queue.client.eval(load_lua('workers/schedule.lua'), 2, queue.key,
                                           f'{queue.key}:schedule', host.name,
                                           config.controller.databases.collectionInterval, body))
    return scheduled


def run(config: Config) -> None:
    """
    Maintain the shared schedule from the single controller deployment.

    Args:
        config (Config): Host inventory, cadence, and broker settings.

    Returns:
        None: No value is returned before termination.
    """
    with collection_queue(config.controller.broker) as queue:
        ready()
        while True:
            schedule_once(config, queue)
            sleep(1)
