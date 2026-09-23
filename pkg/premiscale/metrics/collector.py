"""
Collect raw hypervisor data and submit reduced metrics to the broker.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
import logging
from time import monotonic
from typing import TYPE_CHECKING

from premiscale.hypervisor import build_hypervisor_connection
from premiscale.schemas.collection import CollectionReport
from .reduction import compile_domains

if TYPE_CHECKING:
    from concurrent.futures import Future
    from premiscale.config.v1alpha1 import Config, Host
    from .fanout import MetricsFanout
    from premiscale.messaging.kafka import KafkaFanout
    from premiscale.reconciliation.activity import Activity


log = logging.getLogger(__name__)


class MetricsCollector:
    """
    Sample hosts and submit measurements without opening time-series databases.
    """

    def __init__(self, config: Config, fanout: MetricsFanout | KafkaFanout, activity: 'Activity | None' = None) -> None:
        """
        Prepare collection settings and the process-local metrics transport.

        Args:
            config (Config): Hosts, collection interval, and state database settings.
            fanout (MetricsFanout | KafkaFanout): Broker publisher independent of database adapters.
            activity (Activity | None): Optional shared outbound-operation reporter.
        """
        self.config = config
        self.fanout = fanout
        self.activity = activity

    def collect_once(self, hosts: list[Host], workers: int) -> CollectionReport:
        """
        Visit configured hosts with bounded concurrency and isolate host failures.

        Args:
            hosts (list[Host]): Disjoint host partition owned by this worker process.
            workers (int): Concurrency selected by this worker's PID controller.

        Returns:
            CollectionReport: Success counts and duration after every assigned host has been attempted.
        """
        start = monotonic()
        succeeded = 0
        pending_hosts = iter(hosts)
        limit = max(workers, self.config.controller.databases.hostConnectionQueueSize or workers)
        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='metrics-host')
        try:
            pending: set[Future[bool]] = set()
            exhausted = False
            while pending or not exhausted:
                while len(pending) < limit and not exhausted:
                    host = next(pending_hosts, None)
                    if host is None:
                        exhausted = True
                    else:
                        pending.add(executor.submit(self.collect_host, host))
                if not pending:
                    break
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    try:
                        succeeded += bool(future.result())
                    except Exception as error:
                        log.warning('Host metrics collection failed (%s); retrying next interval', type(error).__name__)
        finally:
            # SIGTERM unwinds this pass; queued connections must not start during shutdown.
            executor.shutdown(wait=True, cancel_futures=True)
        return CollectionReport(len(hosts), succeeded, monotonic() - start)

    def collect_host(self, host: Host) -> bool:
        """
        Close the hypervisor connection before reducing and enqueueing its counters.

        Args:
            host (Host): Hypervisor connection settings and host identity.

        Returns:
            bool: True after successful collection and publication; False when the host is unavailable.
        """
        groups = frozenset(name for name, group in self.config.controller.autoscale.groups.items()
                           if host.name in {member if isinstance(member, str) else member.name for member in group.hosts})
        if not groups:
            return True
        with self.activity.connection() if self.activity is not None else nullcontext(), \
                build_hypervisor_connection(host, readonly=True) as connection:
            if connection is None:
                return False
            samples = connection.request_domain_stats(self.config.controller.kubernetes.clusterName, groups)
        if samples:
            self.fanout.publish(compile_domains(samples))
        return True
