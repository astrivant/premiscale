"""
Implement a Libvirt connection to a Qemu-based hypervisor/host.

https://www.qemu.org/
"""


from __future__ import annotations

import logging
from datetime import datetime, timezone
from xml.etree import ElementTree

import libvirt

from typing import TYPE_CHECKING
from libvirt import (
    VIR_DOMAIN_NOSTATE,      # 0
)
from xmltodict import parse as xmlparse
from cachetools import cached, TTLCache
from premiscale.hypervisor._base import Libvirt, retry_libvirt_connection
from premiscale.schemas.qemu import (OWNERSHIP_NAMESPACE, DomainState, HostInfo, HostResourceStats,
                                     HostStats, ManagedDomain, RawDomainStats)

if TYPE_CHECKING:
    from typing import Dict
    from ipaddress import IPv4Address


log = logging.getLogger(__name__)


class Qemu(Libvirt):
    """
    A subclass for interacting with a Qemu-based hypervisor/host.
    """

    def __init__(self,
                 name: str,
                 address: IPv4Address,
                 port: int,
                 protocol: str,
                 timeout: int = 60,
                 user: str | None = None,
                 readonly: bool = False,
                 resources: Dict | None = None) -> None:
        """
        Initialize Qemu with the supplied settings.

        Args:
            name (str): Name identifying the requested resource.
            address (IPv4Address): Host address or bound listener address.
            port (int): TCP port; zero requests an available local port.
            protocol (str): Protocol.
            timeout (int): Maximum time to wait, in seconds.
            user (str | None): User.
            readonly (bool): Readonly.
            resources (Dict | None): Resources.
        """
        super().__init__(
            name=name,
            address=address,
            port=port,
            protocol=protocol,
            hypervisor='qemu',
            timeout=timeout,
            user=user,
            readonly=readonly,
            resources=resources
        )

    @cached(cache=TTLCache(maxsize=1, ttl=5))
    @retry_libvirt_connection()
    def collect_host_stats(self) -> HostStats | None:
        """
        Get a report of schedulable resource utilization on the host.

        Returns:
            HostStats | None: Host resources and domain states, or None when no connection is available.
        """
        if self._connection is None:
            return None

        return HostStats(
            host=HostInfo(
                name=self._connection.getHostname(),
                type=self._connection.getType(),
                uri=self._connection.getURI(),
                version=self._connection.getVersion(),
                libvirt_version=self._connection.getLibVersion(),
                capabilities=xmlparse(self._connection.getCapabilities()),
                node_info=self._connection.getInfo(),
                max_vcpus=self._connection.getMaxVcpus(None),
                free_memory=self._connection.getFreeMemory(),
                node_memory=self._connection.getMemoryStats(-1, 0),
                node_cpu_stats=self._connection.getCPUStats(True),
                stats=HostResourceStats(
                    cpu=self._connection.getCPUStats(
                        cpuNum=-1,
                        flags=VIR_DOMAIN_NOSTATE
                    ),
                    memory=self._connection.getMemoryStats(
                        cellNum=-1,
                        flags=VIR_DOMAIN_NOSTATE
                    )
                )
            ),
            # Get a picture of the state of the host.
            vms=[
                DomainState(name=vm.name(), state=vm.info())
                for vm in self._connection.listAllDomains(flags=VIR_DOMAIN_NOSTATE)
            ]
        )

    def request_domain_stats(self, cluster: str, groups: frozenset[str]) -> tuple[RawDomainStats, ...]:
        """
        Request counters only after verifying each VM's ownership and host assignment.

        Empty inventories succeed without a statistics request. Inactive managed
        domains are included so a stopped VM still produces a state observation.
        Domain handles never escape this connection, and counter data is not parsed.

        Args:
            cluster (str): Cluster identity expected in the ownership metadata.
            groups (frozenset[str]): Configured groups allowed on this host.

        Returns:
            tuple[RawDomainStats, ...]: Unparsed samples, or an empty tuple when no managed VM exists.

        Raises:
            ConnectionError: If the host connection is unavailable.
            ValueError: If a managed VM's ownership marker disagrees with its UUID.
            libvirt.libvirtError: If inventory or statistics acquisition fails.
        """
        if self._connection is None:
            raise ConnectionError('Hypervisor connection is unavailable')
        if not groups:
            return ()
        domains = []
        identities = {}
        for domain in self._connection.listAllDomains(0):
            try:
                root = ElementTree.fromstring(domain.XMLDesc(0))
                mark = root.find(f'metadata/{{{OWNERSHIP_NAMESPACE}}}instance')
                if mark is None or mark.get('cluster') != cluster or mark.get('group') not in groups:
                    continue
                identity = domain.UUIDString()
                if mark.get('id') != identity:
                    raise ValueError('Managed VM ownership does not match its UUID')
                identities[identity] = ManagedDomain(identity, domain.name(), cluster, mark.attrib['group'],
                                                       self.name, mark.get('address', ''))
                domains.append(domain)
            except libvirt.libvirtError as error:
                if error.get_error_code() != libvirt.VIR_ERR_NO_DOMAIN:
                    raise
        if not domains:
            return ()
        acquired = datetime.now(timezone.utc)
        statistics = self._connection.domainListGetStats(
            domains, stats=libvirt.VIR_DOMAIN_STATS_STATE | libvirt.VIR_DOMAIN_STATS_CPU_TOTAL
            | libvirt.VIR_DOMAIN_STATS_BALLOON | libvirt.VIR_DOMAIN_STATS_VCPU
            | libvirt.VIR_DOMAIN_STATS_INTERFACE | libvirt.VIR_DOMAIN_STATS_BLOCK, flags=0)
        return tuple(RawDomainStats(identities[domain.UUIDString()], acquired, dict(counters))
                     for domain, counters in statistics)
