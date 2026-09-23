"""
Exercise cached process state and truthful AutoscalingGroup status across processes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import multiprocessing as mp
import os
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING

from jsonschema import Draft7Validator
import pytest
from ruamel.yaml import YAML

from premiscale.api import create_app
from premiscale.status.groups import configuration_digest, group_status
from premiscale.status.health import report
from premiscale.status.store import StatusStore

if TYPE_CHECKING:
    from typing import Any


ROOT = Path(__file__).resolve().parents[3]


def _publish(directory: str, source: str) -> None:
    """
    Write repeated snapshots in a spawned process without shared Python objects.

    Args:
        directory (str): Shared private test directory.
        source (str): Filename owned by this writer.

    Returns:
        None: No value is returned.
    """
    store = StatusStore(directory)
    for sequence in range(30):
        store.write(source, {'sequence': sequence, 'contents': 'x' * 8192})


def test_spawned_writers_publish_complete_independent_snapshots(tmp_path: Path) -> None:
    """
    Preserve independent child observations without partial reads or lost updates.

    Args:
        tmp_path (Path): Shared snapshot directory.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    workers = [context.Process(target=_publish, args=(str(tmp_path), f'worker-{index}')) for index in range(3)]
    store = StatusStore(tmp_path, cache_seconds=0)
    try:
        for process in workers:
            process.start()
        deadline = monotonic() + 15
        while any(process.is_alive() for process in workers):
            assert monotonic() < deadline
            for index in range(3):
                if (tmp_path / f'worker-{index}.json').exists():
                    current = store.read(f'worker-{index}')
                    assert current is not None
                    assert current['data']['contents'] == 'x' * 8192
                    assert 0 <= current['data']['sequence'] < 30
            sleep(0.001)
        for process in workers:
            process.join(timeout=15)
            assert process.exitcode == 0
        for index in range(3):
            snapshot = store.read(f'worker-{index}')
            assert snapshot is not None
            assert snapshot['data'] == {'sequence': 29, 'contents': 'x' * 8192}
            assert (tmp_path / f'worker-{index}.json').stat().st_mode & 0o777 == 0o600
    finally:
        for process in workers:
            if process.is_alive():
                process.kill()
                process.join(timeout=2)
            process.close()


def test_cache_does_not_extend_freshness_and_rejects_corrupt_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Reject stale observations even while they remain in the read cache.

    Args:
        tmp_path (Path): Shared snapshot directory.
        monkeypatch (pytest.MonkeyPatch): Clock replacement fixture.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setattr('premiscale.status.store.time', lambda: 100)
    writer = StatusStore(tmp_path)
    cached = StatusStore(tmp_path, cache_seconds=60)
    writer.write('sample', {'value': 1})
    first = cached.read('sample')
    assert first is not None and first['data']['value'] == 1
    writer.write('sample', {'value': 2})
    second = cached.read('sample')
    fresh = StatusStore(tmp_path).read('sample')
    assert second is not None and second['data']['value'] == 1
    assert fresh is not None and fresh['data']['value'] == 2
    monkeypatch.setattr('premiscale.status.store.time', lambda: 116)
    assert cached.read('sample') is None
    (tmp_path / 'sample.json').write_text('{broken')
    assert StatusStore(tmp_path).read('sample') is None
    with pytest.raises(ValueError, match='source'):
        cached.read('../sample')


def test_readiness_requires_live_parent_ready_children_and_fresh_provider(tmp_path: Path) -> None:
    """
    Return unavailable for missing state, startup, stale providers, and shutdown.

    Args:
        tmp_path (Path): Shared snapshot directory.

    Returns:
        None: No value is returned.
    """
    store = StatusStore(tmp_path, cache_seconds=0)
    client = create_app(status_store=store).test_client()
    assert client.get('/ready').status_code == 503
    assert client.get('/healthz').status_code == 404
    store.write('supervisor', {'phase': 'running', 'services': {'kubernetes': {'pid': os.getpid(), 'required': True}}})
    store.write('process-kubernetes', {'phase': 'starting'})
    assert report(store, require_ready=False)['status'] == 'OK'
    assert client.get('/ready').status_code == 503
    store.write('process-kubernetes', {'phase': 'ready'})
    assert client.get('/ready').status_code == 503
    store.write('autoscaler', {'groups': {}})
    assert client.get('/ready').status_code == 200
    store.write('supervisor', {'phase': 'stopping', 'services': {}})
    assert client.get('/ready').status_code == 503


@pytest.fixture
def group_spec() -> dict[str, Any]:
    """
    Load a complete CR specification from the documented chart example.

    Returns:
        dict[str, Any]: One valid AutoscalingGroup specification.
    """
    values = YAML(typ='safe').load(ROOT / 'charts/premiscale-crds/examples/multiple-resources.yaml')
    return values['autoscalingGroups']['workers-primary']['spec']


def test_status_tracks_operations_configuration_and_staleness(group_spec: dict[str, Any], tmp_path: Path) -> None:
    """
    Preserve transition times and distinguish VM state from unapplied CR changes.

    Args:
        group_spec (dict[str, Any]): Valid group specification.
        tmp_path (Path): Shared snapshot directory.

    Returns:
        None: No value is returned.
    """
    store = StatusStore(tmp_path, cache_seconds=0)
    group_spec['scaling']['maxNodes'] = 10
    observation = {'configurationDigest': configuration_digest(group_spec), 'targetReplicas': 2,
                   'runningReplicas': 2, 'pendingReplicas': 0, 'deletingReplicas': 0, 'failedReplicas': 0}
    store.write('autoscaler', {'groups': {'workers-primary': observation}})
    now = datetime(2026, 9, 23, tzinfo=timezone.utc)
    first = group_status('workers-primary', 1, group_spec, {}, store, now)
    assert next(condition for condition in first['conditions'] if condition['type'] == 'Ready')['status'] == 'True'
    unchanged = group_status('workers-primary', 1, group_spec, first, store, now + timedelta(seconds=5))
    assert unchanged['conditions'] == first['conditions']
    observation.update(pendingReplicas=1, runningReplicas=1, failedReplicas=1)
    store.write('autoscaler', {'groups': {'workers-primary': observation}})
    failed = group_status('workers-primary', 1, group_spec, first, store, now + timedelta(seconds=10))
    assert next(condition for condition in failed['conditions'] if condition['type'] == 'Ready')['reason'] == 'OperationFailed'
    group_spec['maxPods'] = 5
    mismatch = group_status('workers-primary', 2, group_spec, failed, store, now)
    assert mismatch['observedGeneration'] == 2
    assert next(condition for condition in mismatch['conditions'] if condition['type'] == 'Configured')['reason'] == 'ConfigurationMismatch'
    schema = json.loads((ROOT / 'charts/premiscale-crds/schemas/AutoscalingGroup.json').read_text())
    Draft7Validator(schema['properties']['status']).validate(mismatch)
    (tmp_path / 'autoscaler.json').write_text('{broken')
    stale = group_status('workers-primary', 2, group_spec, mismatch, store, now)
    assert stale['targetReplicas'] is None
    assert all(condition['status'] == 'Unknown' for condition in stale['conditions'])
