"""
Exercise durable scaling and the real Go/Python gRPC boundary without live hosts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from io import BytesIO
import os
from pathlib import Path
from types import SimpleNamespace
import time

import grpc
import pycdlib
import pytest
from ruamel.yaml import YAML

from premiscale.config.v1alpha1 import (
    Autoscale, AutoscalingGroup, CloudInit, Host, HostReplacementStrategy,
    Kubernetes, Network, Resources, ScaleStrategy,
)
from premiscale_cluster_autoscaler.cloudinit import seed, mac_address
from premiscale_cluster_autoscaler.provider import Provider
from premiscale_cluster_autoscaler.protos import externalgrpc_pb2 as pb
from premiscale_cluster_autoscaler.protos import externalgrpc_pb2_grpc as rpc
from premiscale_cluster_autoscaler.runtime import serve

if TYPE_CHECKING:
    from typing import Any, Iterator
    from premiscale_cluster_autoscaler.state import Instance


class FakeDriver:
    """
    Deterministic infrastructure with injectable operation failures.
    """

    def __init__(self) -> None:
        """
        Initialize FakeDriver with the supplied settings.
        """
        self.instances: dict[str, Instance] = {}
        self.failure: str | None = None
        self.deleted: list[str] = []

    def discover(self) -> list[Instance]:
        """
        Discover managed VM identities on configured hypervisor hosts.

        Returns:
            list[Instance]: Copies of the managed instance records in the fake infrastructure.
        """
        return list(self.instances.values())

    def provision(self, instance: Instance, group: Any) -> None:
        """
        Provision the requested VM from its node-group template.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.
            group (Any): Node group configuration or identifier.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If the test has configured a provisioning failure.
        """
        if self.failure:
            raise RuntimeError(self.failure)
        self.instances[instance.id] = replace(instance, phase='running')

    def delete(self, instance: Instance) -> None:
        """
        Delete the managed VM and its owned storage volumes.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If the test has configured a deletion failure.
        """
        if self.failure:
            raise RuntimeError(self.failure)
        self.deleted.append(instance.id)
        self.instances.pop(instance.id, None)

    def template(self, group: Any) -> dict[str, Any]:
        """
        Describe the capacity, labels, and taints of a node-group template.

        Args:
            group (Any): Node group configuration or identifier.

        Returns:
            dict[str, Any]: The capacity, labels, and taints of a node-group template.
        """
        return {'labels': {'kubernetes.io/os': 'linux'},
                'capacity': {'cpu': '2', 'memory': '4Gi', 'pods': '110'},
                'allocatable': {'cpu': '2', 'memory': '4Gi', 'pods': '110'}, 'taints': []}

    def vacancies(self, group: Any) -> dict[str, int]:
        """
        Report available template-sized VM slots by host.

        Args:
            group (Any): Node group configuration or identifier.

        Returns:
            dict[str, int]: Available template-sized VM slots by host.
        """
        return {'host-1': 4}


@pytest.fixture
def config(tmp_path: Path) -> Any:
    """
    Build a node group without touching SSH files or network resources.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        Any: Controller configuration test double with an isolated journal and one node group.
    """
    host = Host('host-1', '127.0.0.1', 'ssh', 22, 'qemu')
    group = AutoscalingGroup(
        image='/images/template.qcow2', name='worker-template', imageMigration='centralized',
        cloudInit=CloudInit(inline=(Path(__file__).resolve().parents[1] / 'data/cloud-init/worker.yaml').read_text(encoding='utf-8')),
        hosts=[host.name], replacement=HostReplacementStrategy('rollingUpdate', 1, 1),
        networking=Network('dynamic', [], '10.0.0.1', '10.0.0.0/24'),
        scaling=ScaleStrategy(0, 4, 1, 60, 'linear', Resources(80, 80, 80)),
    )
    return SimpleNamespace(controller=SimpleNamespace(
        autoscale=Autoscale([host], {'workers': group}),
        kubernetes=Kubernetes(8085, 'cluster-autoscaler', providerPort=0, stateFile=str(tmp_path / 'state.db')),
    ))


@pytest.fixture
def backend(config: Any) -> Iterator[Provider]:
    """
    Close the provider journal after each test.

    Args:
        config (Any): Parsed controller configuration.

    Yields:
        Provider: Resource available for the duration of the context.
    """
    provider = Provider(config, FakeDriver(), config.controller.kubernetes.stateFile)
    yield provider
    provider.close()


def test_scale_up_provisions_and_delete_removes_exact_node(backend: Provider) -> None:
    """
    Verify scale up provisions and delete removes exact node.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 2)
    assert backend.target_size('workers') == 2
    assert all(node.phase == 'queued' for node in backend.nodes('workers'))
    assert backend.run_once()
    assert backend.run_once()
    first, second = backend.nodes('workers')
    assert backend.group_for_node(first.provider_id, '') == 'workers'
    backend.delete_nodes('workers', [(first.provider_id, first.name)])
    assert backend.target_size('workers') == 1
    assert backend.run_once()
    assert backend.nodes('workers') == [second]
    assert cast(FakeDriver, backend.driver).deleted == [first.id]


def test_provider_observations_reflect_committed_lifecycle_counts(backend: Provider) -> None:
    """
    Report accepted targets, running VMs, pending deletions, and operation failures.

    Args:
        backend (Provider): Isolated provider with deterministic hypervisor operations.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 2)
    pending = backend.observations()['groups']['workers']
    assert pending['targetReplicas'] == pending['pendingReplicas'] == 2
    assert pending['runningReplicas'] == 0
    assert backend.run_once()
    cast(FakeDriver, backend.driver).failure = 'test failure'
    assert not backend.run_once()
    failed = backend.observations()['groups']['workers']
    assert failed['runningReplicas'] == failed['failedReplicas'] == 1
    assert failed['pendingReplicas'] == 0
    node = next(node for node in backend.nodes('workers') if node.phase == 'running')
    backend.delete_nodes('workers', [(node.provider_id, node.name)])
    deleting = backend.observations()['groups']['workers']
    assert deleting['targetReplicas'] == deleting['deletingReplicas'] == 1
    assert 'test failure' not in str(deleting)


def test_pending_target_reduction_never_deletes_a_running_vm(backend: Provider) -> None:
    """
    Verify pending target reduction never deletes a running vm.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 3)
    backend.run_once()
    backend.decrease_target('workers', -2)
    assert backend.target_size('workers') == 1
    assert backend.nodes('workers')[0].phase == 'running'
    with pytest.raises(ValueError):
        backend.decrease_target('workers', -1)
    assert not cast(FakeDriver, backend.driver).deleted


@pytest.mark.parametrize('delta', [0, -1, 5])
def test_invalid_scale_up_is_atomic(backend: Provider, delta: int) -> None:
    """
    Verify invalid scale up is atomic.

    Args:
        backend (Provider): Backend or provider exercised by the test.
        delta (int): Requested change in the node group target size.

    Returns:
        None: No value is returned.
    """
    with pytest.raises(ValueError):
        backend.increase('workers', delta)
    assert backend.target_size('workers') == 0


def test_concurrent_requests_cannot_exceed_maximum(backend: Provider) -> None:
    """
    Verify concurrent requests cannot exceed maximum.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    def request() -> bool:
        """
        Attempt one concurrent reservation and report whether it succeeded.

        Returns:
            bool: Attempt one concurrent reservation and report whether it succeeded.
        """
        try:
            backend.increase('workers', 1)
            return True
        except ValueError:
            return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        accepted = list(pool.map(lambda _: request(), range(12)))
    assert sum(accepted) == 4
    assert backend.target_size('workers') == 4


def test_foreign_node_or_mixed_delete_request_is_rejected_atomically(backend: Provider) -> None:
    """
    Verify foreign node or mixed delete request is rejected atomically.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 1)
    node = backend.nodes('workers')[0]
    with pytest.raises(ValueError):
        backend.delete_nodes('workers', [(node.provider_id, node.name), ('other://node', '')])
    assert backend.nodes('workers')[0].phase == 'queued'
    assert backend.group_for_node('other://node', node.name) is None


def test_delete_is_idempotent_while_pending_and_respects_minimum(backend: Provider) -> None:
    """
    Verify delete is idempotent while pending and respects minimum.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.groups['workers'].scaling.minNodes = 1
    backend.increase('workers', 2)
    first, second = backend.nodes('workers')
    backend.delete_nodes('workers', [(first.provider_id, '')] * 2)
    backend.delete_nodes('workers', [(first.provider_id, '')])
    assert backend.target_size('workers') == 1
    with pytest.raises(ValueError):
        backend.delete_nodes('workers', [(second.provider_id, '')])


def test_failed_creation_is_visible_and_retry_uses_the_same_identity(backend: Provider) -> None:
    """
    Verify failed creation is visible and retry uses the same identity.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 1)
    node = backend.nodes('workers')[0]
    cast(FakeDriver, backend.driver).failure = 'disk is full'
    assert backend.run_once() is False
    failed = backend.nodes('workers')[0]
    assert failed.id == node.id
    assert failed.error == 'disk is full'
    assert failed.phase == 'create_failed'
    cast(FakeDriver, backend.driver).failure = None
    assert backend.run_once()
    assert backend.nodes('workers')[0].phase == 'running'
    assert backend.target_size('workers') == 1


def test_queued_work_and_vm_identity_survive_restart(config: Any) -> None:
    """
    Verify queued work and vm identity survive restart.

    Args:
        config (Any): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    driver = FakeDriver()
    first = Provider(config, driver, config.controller.kubernetes.stateFile)
    first.increase('workers', 2)
    first.run_once()
    identities = {node.id for node in first.nodes('workers')}
    first.close()
    second = Provider(config, driver, config.controller.kubernetes.stateFile)
    try:
        second.refresh()
        assert second.target_size('workers') == 2
        assert second.run_once()
        assert {node.id for node in second.nodes('workers')} == identities
        assert all(node.phase == 'running' for node in second.nodes('workers'))
    finally:
        second.close()


def test_static_addresses_are_reserved_atomically(config: Any) -> None:
    """
    Verify static addresses are reserved atomically.

    Args:
        config (Any): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    group = config.controller.autoscale.groups['workers']
    group.networking = Network('static', ['10.0.0.10'], '10.0.0.1', '10.0.0.0/24')
    provider = Provider(config, FakeDriver(), ':memory:')
    try:
        with pytest.raises(ValueError, match='exhausted'):
            provider.increase('workers', 2)
        assert provider.target_size('workers') == 0
        provider.increase('workers', 1)
        assert provider.nodes('workers')[0].address == '10.0.0.10'
    finally:
        provider.close()


def test_journal_rejects_a_different_cluster(config: Any) -> None:
    """
    Verify journal rejects a different cluster.

    Args:
        config (Any): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    provider = Provider(config, FakeDriver(), config.controller.kubernetes.stateFile)
    provider.increase('workers', 1)
    provider.close()
    config.controller.kubernetes.clusterName = 'another-cluster'
    with pytest.raises(ValueError, match='different clusterName'):
        Provider(config, FakeDriver(), config.controller.kubernetes.stateFile)


def test_failed_delete_keeps_cleanup_pending_without_double_decrement(backend: Provider) -> None:
    """
    Verify failed delete keeps cleanup pending without double decrement.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 1)
    backend.run_once()
    node = backend.nodes('workers')[0]
    backend.delete_nodes('workers', [(node.provider_id, '')])
    cast(FakeDriver, backend.driver).failure = 'host is unavailable'
    assert backend.run_once() is False
    assert backend.nodes('workers')[0].phase == 'delete_failed'
    assert backend.target_size('workers') == 0
    cast(FakeDriver, backend.driver).failure = None
    assert backend.run_once()
    assert backend.nodes('workers') == []


def test_cloud_init_media_contains_identity_and_networking(backend: Provider) -> None:
    """
    Verify cloud init media contains identity and networking.

    Args:
        backend (Provider): Backend or provider exercised by the test.

    Returns:
        None: No value is returned.
    """
    backend.increase('workers', 1)
    node = backend.nodes('workers')[0]
    image = pycdlib.PyCdlib()
    image.open_fp(BytesIO(seed(node, backend.groups['workers'], mac_address(node.id))))
    try:
        files = {}
        for name in ('user-data', 'meta-data', 'network-config'):
            output = BytesIO()
            image.get_file_from_iso_fp(output, rr_path=f'/{name}')
            files[name] = YAML(typ='safe').load(output.getvalue())
        assert files['user-data']['hostname'] == node.name
        assert files['meta-data']['instance-id'] == node.id
        interface = files['network-config']['ethernets']['primary']
        assert interface['match']['macaddress'] == mac_address(node.id)
        assert interface['dhcp4'] is True
    finally:
        image.close()


@pytest.mark.skipif(not os.environ.get('PREMISCALE_AUTOSCALER_BINARY'), reason='requires the built Go adapter')
def test_go_python_grpc_provisions_reports_and_deletes(config: Any) -> None:
    """
    Verify go python grpc provisions reports and deletes.

    Args:
        config (Any): Parsed controller configuration.

    Returns:
        None: No value is returned.
    """
    driver = FakeDriver()
    with serve(config, driver) as runtime:
        with grpc.insecure_channel(runtime.address) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
            client = rpc.CloudProviderStub(channel)
            groups = client.NodeGroups(pb.NodeGroupsRequest(), timeout=5)
            assert [(group.id, group.maxSize) for group in groups.nodeGroups] == [('workers', 4)]
            client.NodeGroupIncreaseSize(pb.NodeGroupIncreaseSizeRequest(id='workers', delta=1), timeout=5)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                nodes = client.NodeGroupNodes(pb.NodeGroupNodesRequest(id='workers'), timeout=5).instances
                if nodes and nodes[0].status.instanceState == pb.InstanceStatus.instanceRunning:
                    break
                time.sleep(0.02)
            assert nodes[0].status.instanceState == pb.InstanceStatus.instanceRunning
            owner = client.NodeGroupForNode(pb.NodeGroupForNodeRequest(node=pb.ExternalGrpcNode(providerID=nodes[0].id)), timeout=5)
            assert owner.nodeGroup.id == 'workers'
            template = client.NodeGroupTemplateNodeInfo(pb.NodeGroupTemplateNodeInfoRequest(id='workers'), timeout=5)
            assert template.nodeBytes
            with pytest.raises(grpc.RpcError) as error:
                client.NodeGroupIncreaseSize(pb.NodeGroupIncreaseSizeRequest(id='workers', delta=0), timeout=5)
            assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            client.NodeGroupDeleteNodes(pb.NodeGroupDeleteNodesRequest(id='workers', nodes=[pb.ExternalGrpcNode(providerID=nodes[0].id)]), timeout=5)
            assert client.NodeGroupTargetSize(pb.NodeGroupTargetSizeRequest(id='workers'), timeout=5).targetSize == 0
            deadline = time.monotonic() + 5
            while driver.instances and time.monotonic() < deadline:
                time.sleep(0.02)
            assert not driver.instances
            assert driver.deleted
    assert runtime.process.poll() is not None
