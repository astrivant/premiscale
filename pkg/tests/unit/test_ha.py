"""
Exercise election races, shared durable journals, and consolidated scaling composition.
"""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4
from unittest.mock import Mock

import psycopg
from psycopg import sql
import pytest
from requests import Response
from ruamel.yaml import YAML

from premiscale.connections.journal import Ownership, schema_name
from premiscale.daemon.processes import build, leader_processes
from premiscale.daemon.settings import Execution
from premiscale.operator.election import Lease
from premiscale.reconciliation.runtime import pipeline_processes
from premiscale.support.sql import load_sql
from premiscale_cluster_autoscaler.state import Instance, State
from premiscale_cluster_autoscaler.libvirt import LibvirtDriver
from tests.unit.test_broker_chart import ROOT, render
from tests.unit.test_worker_scaling import worker_config
from premiscale.status.store import StatusStore

if TYPE_CHECKING:
    from typing import Any
    from premiscale.config.v1alpha1 import Config


def test_election_excludes_competitors_and_expired_leaders(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Model API resource-version conflicts and local deadline expiry without wall-clock trust.

    Args:
        monkeypatch (pytest.MonkeyPatch): Environment and clock overrides.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setenv('CONTROLLER_POD_NAME', 'test-pod')
    monkeypatch.setenv('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc')
    clock = [100.0]
    monkeypatch.setattr('premiscale.operator.election.monotonic', lambda: clock[0])
    resource: dict[str, Any] = {}

    def request(method: str, _path: str, body: dict[str, Any] | None = None) -> Response:
        """
        Emulate one Kubernetes Lease with atomic resource-version updates.

        Args:
            method (str): Requested HTTP verb.
            _path (str): Namespaced Lease path.
            body (dict[str, Any] | None): Requested object version and owner.

        Returns:
            Response: Kubernetes-style success, absence, or conflict response.
        """
        response = Response()
        response.status_code = 200
        if method == 'GET':
            response.status_code = 200 if resource else 404
        elif body is not None:
            version = resource.get('metadata', {}).get('resourceVersion')
            if (method == 'POST' and resource) or body['metadata'].get('resourceVersion') != version:
                response.status_code = 409
            else:
                resource.clear()
                resource.update(deepcopy(body))
                resource['metadata']['resourceVersion'] = str(int(version or 0) + 1)
        response._content = json.dumps(resource).encode()
        return response

    first, second = Lease('test', 'leader'), Lease('test', 'leader')
    monkeypatch.setattr(first, 'request', request)
    monkeypatch.setattr(second, 'request', request)
    try:
        assert first.step()
        assert not second.step()
        clock[0] += first.renew_deadline + 1
        with pytest.raises(RuntimeError, match='deadline'):
            first.step()
        assert resource['spec']['holderIdentity'] == first.identity
        clock[0] += first.duration
        assert second.step()
        assert resource['spec']['holderIdentity'] == second.identity
        first.release()
        assert resource['spec']['holderIdentity'] == second.identity
        second.release()
        assert not resource['spec']['holderIdentity']
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize('cleanup_fails', [False, True])
def test_leadership_loss_stops_children_before_releasing_ownership(worker_config: Config, tmp_path: Path,
                                                                 monkeypatch: pytest.MonkeyPatch, cleanup_fails: bool) -> None:
    """
    Keep journal and Lease ownership until the single-writer process tree is stopped.

    Args:
        worker_config (Config): HA controller configuration.
        tmp_path (Path): Private process status directory.
        monkeypatch (pytest.MonkeyPatch): Supervisor and election overrides.
        cleanup_fails (bool): Simulate an unreaped child which must prevent early release.

    Returns:
        None: No value is returned.
    """
    from premiscale.operator import leadership

    events: list[str] = []
    lease = Mock(valid=True, retry_period=2)
    lease.step.return_value = True
    lease.advertise.side_effect = lambda enabled: events.append(f'advertise-{enabled}')
    lease.release.side_effect = lambda: events.append('release-lease')
    owner = Mock()
    owner.close.side_effect = lambda: events.append('release-journal')
    supervisor = Mock()

    def lose_leadership() -> None:
        """
        Revoke leadership after the supervised children have been started.

        Returns:
            None: No value is returned.
        """
        lease.valid = False

    def stop_children() -> None:
        """
        Record cleanup before checking that ownership can be relinquished.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If the simulated child cannot be reaped.
        """
        events.append('stop-children')
        if cleanup_fails:
            raise RuntimeError('Unreaped child')

    supervisor.poll.side_effect = lose_leadership
    supervisor.close.side_effect = stop_children
    monkeypatch.setattr(leadership, 'current_store', lambda: StatusStore(tmp_path))
    monkeypatch.setattr(leadership, 'Lease', Mock(return_value=lease))
    monkeypatch.setattr(leadership, 'Ownership', Mock(return_value=owner))
    monkeypatch.setattr(leadership, 'Supervisor', Mock(return_value=supervisor))
    monkeypatch.setattr(leadership, 'report', lambda _store: {'status': 'OK'})
    with pytest.raises(RuntimeError):
        leadership.run(worker_config, 'test', '', Execution(mode='ha', publishers=('postgresql',)))
    assert 'stop-children' in events
    if cleanup_fails:
        assert 'release-journal' not in events and 'release-lease' not in events
    else:
        assert events.index('stop-children') < events.index('release-journal') < events.index('release-lease')


def test_shared_journal_survives_failover_and_rolls_back_failed_transactions(tmp_path: Path, worker_config: Config) -> None:
    """
    Reopen committed operations from another connection and enforce exclusive ownership.

    Args:
        tmp_path (Path): Path which PostgreSQL mode must never use for local state.
        worker_config (Config): Configuration used to reopen the volume ownership journal.

    Returns:
        None: No value is returned.
    """
    dsn = os.getenv('PREMISCALE_TEST_POSTGRES_DSN')
    if not dsn:
        pytest.skip('Set PREMISCALE_TEST_POSTGRES_DSN for shared journal tests')
    cluster = f'test-{uuid4()}'
    path = str(tmp_path / 'unused.db')
    owner = Ownership(dsn, cluster, 'provider')
    try:
        with pytest.raises(RuntimeError, match='owns'):
            Ownership(dsn, cluster, 'provider')
        first = State(path, cluster, dsn)
        instance = Instance(str(uuid4()), 'worker', 'group', 'host')
        worker_config.controller.kubernetes.clusterName = cluster
        worker_config.controller.kubernetes.stateDsn = dsn
        worker_config.controller.kubernetes.stateFile = path
        volume = f'/pool/premiscale-{instance.id}-root.qcow2'
        LibvirtDriver(worker_config)._claim(instance, volume, existing=False)
        try:
            with first.connection.transaction():
                first.put(instance)
            with pytest.raises(RuntimeError):
                with first.connection.transaction():
                    first.remove(instance)
                    Mock(side_effect=RuntimeError('transaction interrupted'))()
        finally:
            first.close()
        owner.close()
        successor = Ownership(dsn, cluster, 'provider')
        try:
            second = State(path, cluster, dsn)
            try:
                assert second.instances() == [instance]
                LibvirtDriver(worker_config)._claim(instance, volume, existing=True)
            finally:
                second.close()
            successor.check()
            with psycopg.connect(dsn, autocommit=True) as observer:
                observer.execute(load_sql('postgresql/terminate_owner.sql'), (successor.connection.info.backend_pid,))
            with pytest.raises(psycopg.OperationalError):
                successor.check()
        finally:
            successor.close()
        assert not Path(path).exists()
    finally:
        owner.close()
        with psycopg.connect(dsn, autocommit=True) as cleanup:
            cleanup.execute(sql.SQL(load_sql('postgresql/drop_schema.sql')).format(schema=sql.Identifier(schema_name(cluster))))


@pytest.mark.parametrize('mode', ['ha', 'hha'])
def test_compositions_keep_single_writers_under_election(mode: str, worker_config: Config) -> None:
    """
    Keep replica work disjoint from the elected controller's process subtree.

    Args:
        mode (str): Consolidated or horizontally split HA composition.
        worker_config (Config): Kubernetes configuration with Kafka and a PostgreSQL journal.

    Returns:
        None: No value is returned.
    """
    execution = Execution(mode=mode, publishers=('postgresql',))
    top = {spec.name for spec in build(worker_config, 'test', '', execution)}
    assert top == ({'api', 'leadership', 'reconciliation'} if mode == 'ha' else {'api', 'leadership'})
    leader = {spec.name for spec in leader_processes(worker_config, 'test', '', execution)}
    assert {'operator', 'kubernetes', 'autoscaling', 'reconciliation'} <= leader
    shared = pipeline_processes(worker_config, execution, 'work')
    local = pipeline_processes(worker_config, execution, 'leader')
    assert {spec.name for spec in shared} == {'collectors', 'publishers'}
    assert next(spec for spec in shared if spec.name == 'publishers').args[1] == ('postgresql',)
    assert {spec.name for spec in local} == {'publishers', 'collection-scheduler'}
    assert next(spec for spec in local if spec.name == 'publishers').args[1] == ('_state', 'primary')


@pytest.mark.parametrize('mode', ['ha', 'hha'])
def test_keda_targets_match_composition_and_both_outbound_workloads(mode: str, tmp_path: Path) -> None:
    """
    Render consolidated dual-demand scaling or separate worker targets with elected routing.

    Args:
        mode (str): Requested deployment composition.
        tmp_path (Path): Temporary Helm values directory.

    Returns:
        None: No value is returned.
    """
    values = YAML(typ='safe').load(ROOT / '.config/keda/values.yaml')
    values['mode'] = mode
    values['strimzi-kafka-operator']['enabled'] = False
    values['configMap']['config'] = (ROOT / '.config/keda/controller.yaml').read_text()
    objects = render(tmp_path, values)
    scaled = [item for item in objects if item['kind'] == 'ScaledObject']
    controller = next(item for item in objects if item['kind'] == 'Deployment' and item['metadata']['name'] == 'premiscale')
    assert len(controller['spec']['template']['spec']['containers']) == 1
    assert len(scaled) == (1 if mode == 'ha' else 2)
    if mode == 'ha':
        assert 'replicas' not in controller['spec']
        assert scaled[0]['spec']['scaleTargetRef']['name'] == 'premiscale'
        assert len(scaled[0]['spec']['triggers']) == 4
        assert scaled[0]['spec']['minReplicaCount'] >= 1
    else:
        assert controller['spec']['replicas'] == 2
        assert all(item['spec']['scaleTargetRef']['name'] != 'premiscale' for item in scaled)
    service = next(item for item in objects if item['kind'] == 'Service' and item['metadata']['name'] == 'premiscale')
    metrics = next(item for item in objects if item['kind'] == 'Service' and item['metadata']['name'].endswith('-scaling'))
    assert 'premiscale.com/leader' in service['spec']['selector']
    assert 'premiscale.com/leader' not in metrics['spec']['selector']
    assert sum(item['kind'] == 'KafkaTopic' for item in objects) == 2
