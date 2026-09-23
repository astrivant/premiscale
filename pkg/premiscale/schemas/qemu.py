"""
Describe raw QEMU samples and host snapshots without storage-specific conversion.
"""

from datetime import datetime
# cattrs resolves HostInfo field types when converting host snapshots.
from typing import Any

from attrs import define, field, frozen


OWNERSHIP_NAMESPACE = 'https://premiscale.com/cluster-autoscaler/v1'


@frozen
class ManagedDomain:
    """
    Identify a VM whose ownership was verified before requesting its counters.

    Attributes:
        id (str): Libvirt UUID matching the PremiScale ownership marker.
        name (str): Current domain name.
        cluster (str): Owning PremiScale cluster.
        group (str): Owning configured autoscaling group.
        host (str): Configured hypervisor host name.
        address (str): VM address recorded by the provider, possibly empty.
    """

    id: str
    name: str
    cluster: str
    group: str
    host: str
    address: str = ''


@frozen
class RawDomainStats:
    """
    Transfer an acquired sample out of the connection lifetime without parsing it.

    Attributes:
        domain (ManagedDomain): Verified VM identity independent of its libvirt handle.
        time (datetime): Timezone-aware acquisition timestamp shared by the host batch.
        counters (dict[str, int | str]): Unmodified libvirt field names and values.
    """

    domain: ManagedDomain
    time: datetime
    counters: dict[str, int | str]


@define
class HostResourceStats:
    """
    Store aggregate resource counters returned by libvirt for a host.

    Counter names remain dynamic because supported fields vary by hypervisor.

    Attributes:
        cpu (dict[str, int]): CPU counters for all host CPUs, as reported by libvirt.
        memory (dict[str, int]): Memory counters for all NUMA cells, in KiB.
    """

    cpu: dict[str, int]
    memory: dict[str, int]


@define
class HostInfo:
    """
    Describe a QEMU host and the resources observed during collection.

    Attributes:
        name (str): Hostname reported by the connected hypervisor.
        type (str): Hypervisor driver name.
        uri (str): Libvirt connection URI.
        version (int): Encoded hypervisor version reported by libvirt.
        libvirt_version (int): Encoded version of the remote libvirt library.
        capabilities (dict[str, Any]): Parsed capabilities XML, including driver-specific fields.
        node_info (list[str | int]): CPU model, memory in MiB, CPU count, MHz, NUMA nodes, sockets, cores, and threads.
        max_vcpus (int): Maximum virtual CPUs supported for a guest.
        free_memory (int): Free host memory in bytes.
        node_memory (dict[str, int]): Host memory counters in KiB.
        node_cpu_stats (dict[str, int]): CPU counters for the selected host CPU.
        stats (HostResourceStats): Aggregate host CPU and memory counters.
    """

    name: str
    type: str
    uri: str
    version: int
    libvirt_version: int
    capabilities: dict[str, Any]
    node_info: list[str | int]
    max_vcpus: int
    free_memory: int
    node_memory: dict[str, int]
    node_cpu_stats: dict[str, int]
    stats: HostResourceStats


@define
class DomainState:
    """
    Capture the identity and libvirt state of a domain in a host snapshot.

    Attributes:
        name (str): Domain name reported by libvirt.
        state (list[int]): State code, maximum and current memory in KiB, virtual CPU count, and CPU time in nanoseconds.
    """

    name: str
    state: list[int]


@define
class HostStats:
    """
    Combine host resource information with the states of its domains.

    Attributes:
        host (HostInfo): Host identity, capabilities, and resource measurements.
        vms (list[DomainState]): Domains observed on the host, including inactive domains.
    """

    host: HostInfo
    vms: list[DomainState] = field(factory=list)
