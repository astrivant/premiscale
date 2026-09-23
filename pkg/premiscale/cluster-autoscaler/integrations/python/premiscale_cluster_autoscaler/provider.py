"""
Coordinate autoscaler requests with durable libvirt lifecycle operations.
"""

from __future__ import annotations

from dataclasses import replace
import ipaddress
import logging
import random
import re
from threading import Event, RLock, Thread
from typing import Protocol, TYPE_CHECKING
from uuid import uuid4
from attrs import asdict

from premiscale.schemas.status import GroupObservation
from premiscale.status.groups import configuration_digest
from premiscale_cluster_autoscaler.state import Instance, State

if TYPE_CHECKING:
    from typing import Any
    from premiscale.config.v1alpha1 import Config, AutoscalingGroup, Host


log = logging.getLogger(__name__)
DELETING = {'deleting', 'delete_failed'}


class Driver(Protocol):
    """
    Hypervisor operations used by the lifecycle worker.
    """

    def discover(self) -> list[Instance]:
        """
        Discover managed VM identities on configured hypervisor hosts.

        Returns:
            list[Instance]: Managed VM identities and their observed lifecycle states.
        """
        ...
    def provision(self, instance: Instance, group: AutoscalingGroup) -> None:
        """
        Provision the requested VM from its node-group template.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.
            group (AutoscalingGroup): Node group configuration or identifier.

        Returns:
            None: No value is returned.
        """
        ...
    def delete(self, instance: Instance) -> None:
        """
        Delete the managed VM and its owned storage volumes.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.
        """
        ...
    def template(self, group: str) -> dict[str, Any]:
        """
        Describe the capacity, labels, and taints of a node-group template.

        Args:
            group (str): Node group configuration or identifier.

        Returns:
            dict[str, Any]: The capacity, labels, and taints of a node-group template.
        """
        ...
    def vacancies(self, group: str) -> dict[str, int]:
        """
        Report available template-sized VM slots by host.

        Args:
            group (str): Node group configuration or identifier.

        Returns:
            dict[str, int]: Available template-sized VM slots by host.
        """
        ...


class Provider:
    """
    Accept bounded scaling requests and complete them in a durable worker.
    """

    def __init__(self, config: Config, driver: Driver, state_path: str) -> None:
        """
        Initialize Provider with the supplied settings.

        Args:
            config (Config): Parsed controller configuration.
            driver (Driver): Hypervisor implementation used for discovery and VM operations.
            state_path (str): Path to the persistent operation journal.

        Raises:
            ValueError: If node limits, hosts, static addresses, or existing journal ownership conflict with configuration.
        """
        self.groups = config.controller.autoscale.groups
        hosts = {host.name: host for host in config.controller.autoscale.hosts}
        self.hosts: dict[str, list[Host]] = {}
        for name, group in self.groups.items():
            if not 0 <= group.scaling.minNodes <= group.scaling.maxNodes:
                raise ValueError(f'Invalid node count bounds for {name}')
            candidates = [hosts[item] if isinstance(item, str) else item for item in group.hosts]
            if not candidates or len({host.name for host in candidates}) != len(candidates):
                raise ValueError(f'Node group {name} requires distinct configured hosts')
            if any(host.hypervisor != 'qemu' for host in candidates):
                raise ValueError('VM provisioning currently requires QEMU/libvirt hosts')
            self.hosts[name] = candidates
            if group.networking.type == 'static':
                network = ipaddress.ip_network(group.networking.subnet, strict=False)
                addresses = [ipaddress.ip_address(address) for address in group.networking.addresses]
                if len(set(addresses)) != len(addresses) or any(address not in network for address in addresses):
                    raise ValueError(f'Invalid static address pool for {name}')
        self.driver = driver
        self.state = State(state_path, config.controller.kubernetes.clusterName, getattr(config.controller.kubernetes, 'stateDsn', ''))
        for node in self.state.instances():
            if node.group not in self.groups or node.host not in {host.name for host in self.hosts[node.group]}:
                self.state.close()
                raise ValueError('A group or host with managed instances was removed from configuration')
        self.lock = RLock()
        self.stopped = Event()
        self.wake = Event()
        self.worker: Thread | None = None

    def observations(self) -> dict[str, Any]:
        """
        Snapshot committed lifecycle counts and the configuration actually in use.

        Returns:
            dict[str, Any]: Group observations suitable for the private shared status store.
        """
        with self.lock:
            result = {}
            for name, group in self.groups.items():
                nodes = self.state.instances(name)
                result[name] = asdict(GroupObservation(
                    configuration_digest(asdict(group)),
                    sum(node.phase not in DELETING for node in nodes),
                    sum(node.phase == 'running' for node in nodes),
                    sum(node.phase in {'queued', 'creating'} for node in nodes),
                    sum(node.phase in DELETING for node in nodes),
                    sum(node.phase in {'create_failed', 'delete_failed'} for node in nodes),
                ))
            return {'groups': result}

    def group(self, name: str) -> AutoscalingGroup:
        """
        Find a group, raising KeyError for unknown names.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            AutoscalingGroup: Configured node group with the requested name.
        """
        return self.groups[name]

    def nodes(self, name: str) -> list[Instance]:
        """
        Return managed and requested instances in one group.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            list[Instance]: Managed and requested instances in one group.
        """
        self.group(name)
        with self.lock:
            return self.state.instances(name)

    def target_size(self, name: str) -> int:
        """
        Count accepted instances, excluding requested deletions.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            int: Accepted instances, excluding requested deletions.
        """
        return sum(node.phase not in DELETING for node in self.nodes(name))

    def group_for_node(self, provider_id: str, name: str) -> str | None:
        """
        Resolve only identities recorded as owned by this controller.

        Args:
            provider_id (str): Kubelet provider ID identifying the managed VM.
            name (str): Name identifying the requested resource.

        Returns:
            str | None: Owning group name, or None when the node is not managed by this controller.
        """
        with self.lock:
            for node in self.state.instances():
                if (provider_id and node.provider_id == provider_id) or (not provider_id and node.name == name):
                    return node.group
        return None

    def increase(self, name: str, delta: int) -> None:
        """
        Atomically reserve identities before acknowledging a scale-up.

        Args:
            name (str): Name identifying the requested resource.
            delta (int): Requested change in the node group target size.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the delta is nonpositive or the request exceeds node, host-capacity, or address limits.
        """
        group = self.group(name)
        if delta <= 0:
            raise ValueError('Scale-up delta must be positive')
        with self.lock, self.state.connection.transaction():
            if self.target_size(name) + delta > group.scaling.maxNodes:
                raise ValueError('Scale-up exceeds maxNodes')
            for _ in range(delta):
                nodes = self.state.instances(name)
                candidates = self.hosts[name]
                counts = {host.name: sum(node.host == host.name for node in nodes) for host in candidates}
                method = group.scaling.method
                if method == 'vacancy':
                    free = self.driver.vacancies(name)
                    pending = {host.name: sum(node.host == host.name and node.phase in {'queued', 'creating', 'create_failed'}
                                             for node in nodes) for host in candidates}
                    host = max(candidates, key=lambda candidate: free[candidate.name] - pending[candidate.name])
                    if free[host.name] - pending[host.name] <= 0:
                        raise ValueError('No host has capacity for another template VM')
                elif method == 'random':
                    host = random.choice(candidates)
                else:
                    least = min(counts.values())
                    available = [host for host in candidates if counts[host.name] == least]
                    host = random.choice(available) if method == 'linear-random' else available[0]
                address = ''
                if group.networking.type == 'static':
                    used = {node.address for node in self.state.instances()}
                    address = next((ip for ip in group.networking.addresses if ip not in used), '')
                    if not address:
                        raise ValueError('Static address pool exhausted')
                identity = str(uuid4())
                prefix = re.sub('[^a-z0-9-]', '-', name.lower()).strip('-')[:35] or 'premiscale'
                self.state.put(Instance(identity, f'{prefix}-{identity[:12]}', name, host.name, address))
        self.wake.set()

    def decrease_target(self, name: str, delta: int) -> None:
        """
        Cancel queued creations without deleting any existing VM.

        Args:
            name (str): Name identifying the requested resource.
            delta (int): Requested change in the node group target size.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the delta is invalid or the reduction would remove active VMs or violate minNodes.
        """
        group = self.group(name)
        if delta >= 0:
            raise ValueError('Target decrease delta must be negative')
        with self.lock, self.state.connection.transaction():
            queued = [node for node in self.nodes(name) if node.phase == 'queued']
            if -delta > len(queued) or self.target_size(name) + delta < group.scaling.minNodes:
                raise ValueError('Cannot reduce target below existing nodes or minNodes')
            for node in queued[delta:]:
                self.state.remove(node)

    def delete_nodes(self, name: str, identities: list[tuple[str, str]]) -> None:
        """
        Validate the entire request before marking any VM for deletion.

        Args:
            name (str): Name identifying the requested resource.
            identities (list[tuple[str, str]]): Identities.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If a node is not owned by the group or deletion would violate minNodes.
        """
        group = self.group(name)
        with self.lock, self.state.connection.transaction():
            nodes = self.nodes(name)
            selected: dict[str, Instance] = {}
            for provider_id, node_name in identities:
                match = next((node for node in nodes if
                              (provider_id and node.provider_id == provider_id) or
                              (not provider_id and node.name == node_name)), None)
                if match is None:
                    raise ValueError('A requested node does not belong to this node group')
                selected[match.id] = match
            deleting = [node for node in selected.values() if node.phase not in DELETING]
            if self.target_size(name) - len(deleting) < group.scaling.minNodes:
                raise ValueError('Deletion would violate minNodes')
            for node in deleting:
                self.state.put(replace(node, phase='deleting', error=''))
        self.wake.set()

    def refresh(self) -> None:
        """
        Recover managed VM identities without adopting unmarked domains.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If discovered instance ownership conflicts with the persistent journal.
        """
        with self.lock, self.state.connection.transaction():
            discovered = self.driver.discover()
            existing = {node.id: node for node in self.state.instances()}
            for node in discovered:
                if node.group not in self.groups:
                    continue
                previous = existing.get(node.id)
                if previous is not None and (previous.host != node.host or previous.group != node.group):
                    raise ValueError(f'Conflicting VM ownership for {node.id}')
                if previous is None:
                    self.state.put(node)
                elif previous.phase == 'running' and node.phase != 'running':
                    self.state.put(replace(previous, phase='creating'))
            present = {node.id for node in discovered}
            for node in existing.values():
                if node.phase == 'running' and node.id not in present:
                    self.state.put(replace(node, phase='creating'))
        self.wake.set()

    def run_once(self) -> bool:
        """
        Complete one pending operation; leave failed work available to retry.

        Returns:
            bool: True after completing an operation; False when idle or when an operation fails.
        """
        with self.lock, self.state.connection.transaction():
            pending = [node for node in self.state.instances() if node.phase != 'running']
            if not pending:
                return False
            # State updates move a row to the end, so a failing host cannot starve others.
            node = pending[0]
            deleting = node.phase in DELETING
            node = replace(node, phase='deleting' if deleting else 'creating', error='')
            self.state.put(node)
        try:
            if deleting:
                self.driver.delete(node)
            else:
                self.driver.provision(node, self.group(node.group))
        except Exception as error:
            log.exception('VM operation failed for %s', node.name)
            with self.lock, self.state.connection.transaction():
                current = next(item for item in self.state.instances() if item.id == node.id)
                # A deletion received while provisioning must still win.
                phase = 'delete_failed' if current.phase in DELETING else 'create_failed'
                self.state.put(replace(current, phase=phase, error=str(error)))
            return False
        with self.lock, self.state.connection.transaction():
            current = next(item for item in self.state.instances() if item.id == node.id)
            if deleting:
                self.state.remove(current)
            elif current.phase not in DELETING:
                self.state.put(replace(current, phase='running', error=''))
        return True

    def start(self) -> None:
        """
        Start one worker for the durable operation journal.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If the lifecycle worker is already running.
        """
        if self.worker is not None:
            raise RuntimeError('Provider worker is already running')
        self.refresh()
        self.worker = Thread(target=self._work, name='autoscaler-vms', daemon=True)
        self.worker.start()

    def _work(self) -> None:
        """
        Process pending lifecycle operations until shutdown is requested.

        Returns:
            None: No value is returned.
        """
        while not self.stopped.is_set():
            self.wake.clear()
            if not self.run_once():
                self.wake.wait(5)

    def close(self) -> None:
        """
        Stop accepting work and close the journal after the worker exits.

        Returns:
            None: No value is returned.
        """
        self.stopped.set()
        self.wake.set()
        if self.worker is not None:
            self.worker.join()
        self.state.close()
