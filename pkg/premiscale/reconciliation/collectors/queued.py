"""
Consume shared host requests with per-core, PID-controlled thread pools.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Empty
from time import monotonic, sleep

from premiscale.messaging.queue import InvalidMessage
from premiscale.metrics.collector import MetricsCollector
from premiscale.metrics.fanout import build_fanout
from premiscale.reconciliation.control import ThroughputController
from premiscale.schemas.collection import CollectionReport
from premiscale.status.store import ready
from ..activity import Activity
from .schedule import collection_queue

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


log = logging.getLogger(__name__)


def collect_once(config: Config, collector: MetricsCollector) -> bool | None:
    """
    Claim one host request and acknowledge it only after its fanout succeeds.

    Args:
        config (Config): Local authoritative host inventory and broker settings.
        collector (MetricsCollector): Raw-data collector with shared activity reporting.

    Returns:
        bool | None: Collection success, failure, or None when no request was available.

    Raises:
        ConnectionError: Raised internally to retain failed delivery, then caught and reported as False.
    """
    try:
        with collection_queue(config.controller.broker) as queue, queue.delivery() as delivery:
            host = next((host for host in config.controller.autoscale.hosts if host.name == delivery.payload), None)
            if host is None:
                # Removed hosts must not retain their previous credentials or a permanent backlog.
                return True
            if not collector.collect_host(host):
                raise ConnectionError('Host collection did not complete')
            return True
    except Empty:
        return None
    except InvalidMessage:
        log.warning('Malformed collection request moved to dead-letter stream')
    except Exception as error:
        log.warning('Queued host collection failed (%s); request remains recoverable', type(error).__name__)
    return False


def run(config: Config) -> None:
    """
    Adapt thread concurrency while sharing host work across all pods and CPU processes.

    Args:
        config (Config): Collection settings and the locally resolved host credentials.

    Returns:
        None: No value is returned before termination.
    """
    settings = config.controller.databases
    control = ThroughputController(config.controller.reconciliation.collection,
                                   max(1, len(config.controller.autoscale.hosts)),
                                   settings.maxHostConnectionThreads, settings.collectionInterval)
    fanout = build_fanout(config)
    try:
        with collection_queue(config.controller.broker) as queue, Activity(queue) as activity:
            collector = MetricsCollector(config, fanout, activity)
            ready()
            while True:
                started = monotonic()
                with ThreadPoolExecutor(max_workers=control.threads, thread_name_prefix='host-connection') as executor:
                    futures = [executor.submit(collect_once, config, collector) for _ in range(control.threads)]
                    results = [future.result() for future in futures]
                attempted = sum(result is not None for result in results)
                control.update(CollectionReport(attempted, sum(result is True for result in results),
                                                monotonic() - started))
                if not attempted:
                    sleep(0.1)
    finally:
        fanout.close()
