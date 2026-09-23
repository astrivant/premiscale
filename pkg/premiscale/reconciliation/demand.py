"""
Read aggregate connection activity and unfinished work for KEDA.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from premiscale.messaging.kafka import KafkaLag
from premiscale.support.lua import load_lua
from premiscale.metrics.fanout import metrics_queue
from .collectors.schedule import collection_queue
if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.messaging.queue import RedisQueue
    from premiscale.daemon.settings import Execution


class Demand:
    """
    Limit scaling observations to workloads delegated by this controller.
    """

    def __init__(self, config: Config, execution: 'Execution') -> None:
        """
        Create lazy broker clients for delegated streams.

        Args:
            config (Config): Broker connection settings.
            execution (Execution): Validated controller deployment responsibilities.
        """
        self.queues: dict[str, RedisQueue] = {'collectors': collection_queue(config.controller.broker)}
        self.queues.update((f'publishers/{name}', metrics_queue(config.controller.broker, name))
                           for name in execution.publishers)
        self.lags = {f'publishers/{name}': KafkaLag(config.controller.kafka, name)
                     for name in execution.publishers} if config.controller.kafka.enabled else {}

    def snapshot(self, workload: str) -> dict[str, int]:  # noqa: DOC502
        """
        Count active operations and unfinished batches using one atomic broker read.

        Args:
            workload (str): Collectors or publishers followed by the subscriber name.

        Returns:
            dict[str, int]: Active outbound operations and queued or leased requests.

        Raises:
            KeyError: If the workload is not delegated by this controller.
        """
        queue = self.queues[workload]
        active, outstanding = queue.client.eval(load_lua('workers/demand.lua'), 3,
                                                queue.key, f'{queue.key}:activity', f'{queue.key}:counts')
        if workload in self.lags:
            outstanding = self.lags[workload].count()
        return {'activeConnections': int(active), 'outstandingRequests': int(outstanding)}

    def close(self) -> None:
        """
        Close process-local pools without changing workloads or broker observations.

        Returns:
            None: No value is returned.
        """
        for queue in self.queues.values():
            queue.close()
        for lag in self.lags.values():
            lag.close()
