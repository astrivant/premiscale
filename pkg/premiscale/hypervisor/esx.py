"""
Implement a Libvirt connection to an ESX-based hypervisor/host.
"""


from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.hypervisor._base import Libvirt
from premiscale.schemas.qemu import RawDomainStats, HostStats


if TYPE_CHECKING:
    from typing import Dict, List
    from ipaddress import IPv4Address


# class ESX(Libvirt):
#     """
#     Connect to an ESX hypervisor.
#     """

#     def __init__(self,
#                  name: str,
#                  address: IPv4Address,
#                  port: int,
#                  protocol: str,
#                  timeout: int = 60,
#                  user: str | None = None,
#                  readonly: bool = False,
#                  resources: Dict | None = None) -> None:
#         super().__init__(
#             name=name,
#             address=address,
#             port=port,
#             protocol=protocol,
#             hypervisor='esx',
#             timeout=timeout,
#             user=user,
#             readonly=readonly,
#             resources=resources
#         )

#     def _getHostState(self) -> Dict:
#         """
#         Get the state of the VMs on the host.

#         Returns:
#             Dict: The state of the VMs on the host.
#         """
#         return {}

#     def collect_host_stats(self) -> HostStats | None:
#         """
#         Get a report of schedulable resource utilization on the host.

#         Returns:
#             HostStats | None: Host resources and domain states, or None when no connection is available.
#         """
#         return None

#     def request_domain_stats(self) -> tuple[RawDomainStats, ...]:
#         """
#         Get a report of resource utilization for a VM.

#         Returns:
#             tuple[RawDomainStats, ...]: Stats of all VMs on this particular host connection.
#         """
#         return []

#     def state(self) -> None:
#         return None
