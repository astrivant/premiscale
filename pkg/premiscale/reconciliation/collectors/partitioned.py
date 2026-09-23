"""
Collect one stable host partition with a process-local PID-controlled thread pool.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import logging
from time import sleep

from setproctitle import setproctitle

from premiscale.metrics.collector import MetricsCollector
from premiscale.metrics.fanout import build_fanout
from premiscale.status.store import ready
from ..control import ThroughputController

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config, Host


log = logging.getLogger(__name__)


def run(config: Config, hosts: list[Host], index: int) -> None:
    """
    Create local clients and repeatedly collect the worker's assigned hosts.

    Each completed pass supplies successful-host throughput to a PID controller.
    A new bounded executor adopts its selected thread count on the next pass.
    Empty partitions idle without constructing clients or opening databases.

    Args:
        config (Config): Collection, transport, state, and controller settings.
        hosts (list[Host]): Disjoint partition assigned by reconciliation.
        index (int): Stable zero-based worker index for process naming and logs.

    Returns:
        None: No value is returned before termination.
    """
    setproctitle(f'premiscale-collector-{index}')
    settings = config.controller.databases
    ready()
    while not hosts:
        sleep(settings.collectionInterval)
    control = ThroughputController(config.controller.reconciliation.collection, len(hosts),
                                   settings.maxHostConnectionThreads, settings.collectionInterval)
    fanout = build_fanout(config)
    try:
        collector = MetricsCollector(config, fanout)
        while True:
            threads = control.threads
            report = collector.collect_once(hosts, threads)
            next_threads = control.update(report)
            log.info('Collector %s: hosts=%s failed=%s elapsed=%.3fs throughput=%.3f target=%.3f threads=%s next=%s',
                     index, report.attempted, report.attempted - report.succeeded, report.elapsed,
                     report.throughput, control.target, threads, next_threads)
            sleep(max(0, settings.collectionInterval - report.elapsed))
    finally:
        fanout.close()
