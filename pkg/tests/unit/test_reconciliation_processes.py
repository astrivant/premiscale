"""
Verify real nested collection processes and cleanup after parent or worker failure.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from premiscale.daemon.processes import ProcessSpec
from premiscale.daemon.supervisor import Supervisor
from premiscale.reconciliation.collectors import runtime
from .test_daemon import _alive

if TYPE_CHECKING:
    from typing import Any


def _record_partition(config: Any, hosts: list[int], index: int) -> None:
    """
    Report nested worker ownership and wait for termination or an injected failure.

    Args:
        config (Any): Configuration carrying the test report directory.
        hosts (list[int]): Host identities assigned by real reconciliation partitioning.
        index (int): Collection worker index.

    Returns:
        None: No value is returned before termination.

    Raises:
        SystemExit: When the test requests a collector failure.
    """
    directory = Path(config.report)
    if config.stubborn and index == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    temporary = directory / f'{index}.tmp'
    temporary.write_text(json.dumps({'pid': os.getpid(), 'parent': os.getppid(),
                                     'group': os.getpgrp(), 'hosts': hosts}))
    temporary.replace(directory / f'{index}.json')
    try:
        while True:
            if index == 0 and (directory / 'fail').exists():
                raise SystemExit(7)
            sleep(0.01)
    finally:
        directory.joinpath(f'{index}.cleaned').touch()


def _reconciliation(config: Any) -> int:
    """
    Run actual reconciliation with deterministic CPU capacity and test collectors.

    Args:
        config (Any): Minimal Kubernetes-mode controller configuration.

    Returns:
        int: Reconciliation runtime status propagated to the daemon supervisor.
    """
    with patch.object(runtime, 'available_cores', return_value=3), patch.object(runtime.partitioned, 'run', _record_partition):
        from premiscale.reconciliation.supervision import run_pool

        return run_pool(runtime.processes(config), 'collectors')


def _reports(directory: Path) -> list[dict[str, Any]]:
    """
    Wait for every nested collector to publish its process identity.

    Args:
        directory (Path): Directory containing atomic worker reports.

    Returns:
        list[dict[str, Any]]: Reports ordered by worker index.

    Raises:
        TimeoutError: If collectors fail to report before the startup deadline.
    """
    deadline = monotonic() + 15
    while len(list(directory.glob('*.json'))) < 3:
        if monotonic() >= deadline:
            raise TimeoutError('Collection subprocesses failed to start')
        sleep(0.01)
    return [json.loads(directory.joinpath(f'{index}.json').read_text()) for index in range(3)]


@pytest.mark.parametrize('failure', ['none', 'collector', 'reconciliation', 'stubborn'])
def test_nested_collection_lifecycle(tmp_path: Path, failure: str) -> None:
    """
    Reap a per-core collection tree after graceful shutdown or an unexpected exit.

    Args:
        tmp_path (Path): Directory for worker reports and cleanup markers.
        failure (str): Process to fail, none for graceful shutdown, or stubborn to ignore termination.

    Returns:
        None: No value is returned.
    """
    config = SimpleNamespace(report=str(tmp_path), stubborn=failure == 'stubborn', controller=SimpleNamespace(
        mode='kubernetes', autoscale=SimpleNamespace(hosts=list(range(8))),
    ))
    supervisor = Supervisor([ProcessSpec('reconciliation', _reconciliation, (config,))], mp.get_context('spawn'))
    reports = []
    try:
        supervisor.start(Event())
        parent = supervisor.processes[0][1].pid
        reports = _reports(tmp_path)
        assert len({report['pid'] for report in reports}) == 3
        assert all(report['parent'] == parent and report['group'] == parent for report in reports)
        assert sorted(host for report in reports for host in report['hosts']) == list(range(8))
        if failure == 'collector':
            tmp_path.joinpath('fail').touch()
        elif failure == 'reconciliation':
            assert parent is not None
            os.kill(parent, signal.SIGKILL)
        if failure in {'collector', 'reconciliation'}:
            deadline = monotonic() + 15
            status = None
            while status is None and monotonic() < deadline:
                status = supervisor.poll()
            assert status == (7 if failure == 'collector' else 1)
    finally:
        supervisor.close()
    if failure != 'reconciliation':
        assert len(list(tmp_path.glob('*.cleaned'))) == (2 if failure == 'stubborn' else 3)
    deadline = monotonic() + 5
    while any(_alive(report['pid']) for report in reports) and monotonic() < deadline:
        sleep(0.01)
    assert not any(_alive(report['pid']) for report in reports)
    assert not supervisor.processes
