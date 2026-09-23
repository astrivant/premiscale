"""
Exercise real worker failures, signals, bounded shutdown, and HTTP listener cleanup.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
from threading import current_thread, Event, main_thread
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import urlopen

import pytest

from premiscale.config.v1alpha1 import Config
from premiscale.daemon import runtime
from premiscale.daemon.processes import api, ProcessSpec
from premiscale.daemon.shutdown import Shutdown
from premiscale.daemon.supervisor import Supervisor

if TYPE_CHECKING:
    from typing import Any


def _wait_forever(ready: Any, cleaned: Path) -> None:
    """
    Wait for termination and record worker cleanup.

    Args:
        ready (Any): Readiness event or marker used to coordinate test processes.
        cleaned (Path): Path where the worker records successful cleanup.

    Returns:
        None: No value is returned.
    """
    try:
        ready.set()
        while True:
            signal.pause()
    finally:
        Path(cleaned).write_text('stopped')


def _exit_after_ready(ready: Any, code: int) -> None:
    """
    Exit with the requested status once the peer is ready.

    Args:
        ready (Any): Readiness event or marker used to coordinate test processes.
        code (int): Exit status or libvirt error code to simulate.

    Returns:
        None: No value is returned.

    Raises:
        RuntimeError: Peer failed to start.
        SystemExit: With the exit status requested by the test.
    """
    if not ready.wait(10):
        raise RuntimeError('Peer failed to start')
    raise SystemExit(code)


def _stubborn(ready: Any) -> None:
    """
    Ignore termination to exercise forced worker cleanup.

    Args:
        ready (Any): Readiness event or marker used to coordinate test processes.

    Returns:
        None: No value is returned.
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ready.set()
    while True:
        signal.pause()


def _orphan_child(ready: Any, pid_file: Path) -> None:
    """
    Leave a stubborn descendant behind when the worker exits.

    Args:
        ready (Any): Readiness event or marker used to coordinate test processes.
        pid_file (Path): Path where the descendant records its process ID.

    Returns:
        None: No value is returned.

    Raises:
        RuntimeError: Descendant failed to start.
        SystemExit: With status nine after the descendant publishes its PID.
    """
    program = Path(__file__).resolve().parents[1] / 'data/processes/stubborn_child.py'
    subprocess.Popen([sys.executable, str(program), str(pid_file)])
    deadline = time.monotonic() + 10
    while not Path(pid_file).exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Descendant failed to start')
        time.sleep(0.01)
    ready.set()
    raise SystemExit(9)


def _api_config(port: int=0) -> Config:
    """
    Build a minimal HTTP API configuration for process tests.

    Args:
        port (int): TCP port; zero requests an available local port.

    Returns:
        Config: Minimal configuration test double for the HTTP API.
    """
    return cast(Config, SimpleNamespace(controller=SimpleNamespace(
        mode='standalone', healthcheck=SimpleNamespace(host='127.0.0.1', port=port, stateDirectory='/tmp/premiscale-test-status'),
    )))


def _reported_api(config: Any, report: Path, cleaned: Path) -> None:
    """
    Serve the API while reporting child ownership and listener cleanup.

    Args:
        config (Any): Parsed controller configuration.
        report (Path): Path where the API child reports its address and process identity.
        cleaned (Path): Path where the worker records successful cleanup.

    Returns:
        None: No value is returned.
    """
    make_server = api.make_server

    def bind(*args: Any, **kwargs: Any) -> Any:
        """
        Bind a real HTTP listener and publish the owning process identity.

        Args:
            *args (Any): Positional arguments forwarded to the wrapped callable.
            **kwargs (Any): Keyword arguments forwarded to the wrapped callable.

        Returns:
            Any: Bind a real HTTP listener and publish the owning process identity.
        """
        server = make_server(*args, **kwargs)
        temporary = Path(report).with_suffix('.tmp')
        temporary.write_text(json.dumps({
            'address': server.server_address,
            'pid': os.getpid(),
            'parent': os.getppid(),
            'main_thread': current_thread() is main_thread(),
        }))
        temporary.replace(report)
        return server

    try:
        with patch.object(api, 'make_server', bind):
            api.run(config)
    finally:
        Path(cleaned).write_text('stopped')


def _wait_for_api(report: Path) -> dict[str, Any]:
    """
    Wait for the API child to publish its listener address.

    Args:
        report (Path): Path where the API child reports its address and process identity.

    Returns:
        dict[str, Any]: Wait for the API child to publish its listener address.
    """
    deadline = time.monotonic() + 10
    while not report.exists():
        if time.monotonic() >= deadline:
            pytest.fail('API failed to bind')
        time.sleep(0.01)
    return json.loads(report.read_text())


def _assert_listener_released(address: Any) -> None:
    """
    Verify that the former API address can be rebound.

    Args:
        address (Any): Host address or bound listener address.

    Returns:
        None: No value is returned.
    """
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(tuple(address))


def _signal_controller(ready: Any, cleaned: Path, report: Path, api_cleaned: Path) -> None:
    """
    Run a parent supervisor with an API child and a peer worker.

    Args:
        ready (Any): Readiness event or marker used to coordinate test processes.
        cleaned (Path): Path where the worker records successful cleanup.
        report (Path): Path where the API child reports its address and process identity.
        api_cleaned (Path): Path where the API child records successful cleanup.

    Returns:
        None: No value is returned.

    Raises:
        SystemExit: With the parent controller's final exit status.
    """
    config = _api_config()
    specs = [
        ProcessSpec('api', _reported_api, (config, report, api_cleaned)),
        ProcessSpec('worker', _wait_forever, (ready, cleaned)),
    ]
    with patch.object(runtime, 'build', return_value=specs):
        result = runtime.start(config, 'test', '')
    raise SystemExit(result)


def _api_after_peer(config: Any, ready: Any) -> None:
    """
    Start the API after the peer reaches its cleanup-protected loop.

    Args:
        config (Any): Parsed controller configuration.
        ready (Any): Readiness event or marker used to coordinate test processes.

    Returns:
        None: No value is returned.

    Raises:
        RuntimeError: Peer failed to start.
    """
    if not ready.wait(10):
        raise RuntimeError('Peer failed to start')
    api.run(config)


def _alive(pid: int) -> bool:
    """
    Check whether a process exists and is not awaiting reaping as a zombie.

    Args:
        pid (int): Process ID to check.

    Returns:
        bool: Whether a process exists and is not awaiting reaping as a zombie.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A killed orphan can await reaping by the container's init process.
    stat = Path(f'/proc/{pid}/stat')
    if stat.exists():
        try:
            return stat.read_text().rsplit(')', 1)[1].split()[0] != 'Z'
        except FileNotFoundError:
            return False
    return True


@pytest.mark.parametrize('exitcode', [0, 7])
def test_required_worker_exit_stops_peer_and_runs_cleanup(tmp_path: Path, exitcode: int) -> None:
    """
    Verify required worker exit stops peer and runs cleanup.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        exitcode (int): Worker exit status under test.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    cleaned = tmp_path / 'cleaned'
    supervisor = Supervisor([
        ProcessSpec('peer', _wait_forever, (ready, cleaned)),
        ProcessSpec('exiting', _exit_after_ready, (ready, exitcode)),
    ], context, shutdown_timeout=2)
    try:
        supervisor.start(Event())
        assert supervisor.wait(Event()) == (exitcode or 1)
    finally:
        supervisor.close()
    assert cleaned.read_text() == 'stopped'
    assert supervisor.exitcodes == {'peer': 0, 'exiting': exitcode}
    supervisor.close()


def test_optional_worker_can_decline_and_retire_its_process_handle() -> None:
    """
    Verify optional worker can decline and retire its process handle.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    ready.set()
    supervisor = Supervisor([
        ProcessSpec('optional', _exit_after_ready, (ready, 0), required=False),
    ], context)
    try:
        supervisor.start(Event())
        assert supervisor.wait(Event()) == 0
        assert not supervisor.processes
        assert supervisor.exitcodes == {'optional': 0}
    finally:
        supervisor.close()


def test_stubborn_worker_is_killed_within_shared_deadline() -> None:
    """
    Verify stubborn worker is killed within shared deadline.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    supervisor = Supervisor([ProcessSpec('stubborn', _stubborn, (ready,))], context,
                            shutdown_timeout=0.1, kill_timeout=2)
    try:
        supervisor.start(Event())
        assert ready.wait(10)
    finally:
        started = time.monotonic()
        supervisor.close()
    assert time.monotonic() - started < 4
    assert supervisor.exitcodes['stubborn'] == -signal.SIGKILL


def test_signal_permission_failure_on_a_live_worker_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify signal permission failure on a live worker is reported.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    supervisor = Supervisor([ProcessSpec('stubborn', _stubborn, (ready,))], context,
                            shutdown_timeout=0.1, kill_timeout=2)
    try:
        supervisor.start(Event())
        assert ready.wait(10)

        def denied(*_args: Any) -> None:
            """
            Simulate a denied process-group signal.

            Args:
                *_args (Any): Unused positional arguments supplied by the caller.

            Returns:
                None: No value is returned.

            Raises:
                PermissionError: signal denied.
            """
            raise PermissionError('signal denied')

        with monkeypatch.context() as patch:
            patch.setattr(os, 'killpg', denied)
            with pytest.raises(PermissionError, match='signal denied'):
                supervisor._signal(supervisor.processes[0][1], signal.SIGTERM)
    finally:
        supervisor.close()


def test_crashed_worker_does_not_leave_its_descendant_running(tmp_path: Path) -> None:
    """
    Verify crashed worker does not leave its descendant running.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    pid_file = tmp_path / 'descendant.pid'
    supervisor = Supervisor([ProcessSpec('crashing', _orphan_child, (ready, pid_file))], context,
                            shutdown_timeout=0.1, kill_timeout=2)
    try:
        supervisor.start(Event())
        assert ready.wait(10)
        assert supervisor.wait(Event()) == 9
    finally:
        supervisor.close()
    descendant = int(pid_file.read_text())
    deadline = time.monotonic() + 3
    while _alive(descendant) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not _alive(descendant)


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM])
def test_parent_signal_stops_and_reaps_worker(tmp_path: Path, signum: int) -> None:
    """
    Verify parent signal stops and reaps worker.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        signum (int): POSIX signal number to deliver.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    cleaned = tmp_path / 'signal-cleaned'
    report = tmp_path / 'api.json'
    api_cleaned = tmp_path / 'api-cleaned'
    controller = context.Process(target=_signal_controller, args=(ready, cleaned, report, api_cleaned))
    controller.start()
    try:
        assert ready.wait(10)
        api_details = _wait_for_api(report)
        assert controller.pid is not None
        assert api_details['parent'] == controller.pid
        with urlopen(f"http://127.0.0.1:{api_details['address'][1]}/healthz", timeout=2) as response:
            assert response.status == 200
        os.kill(controller.pid, signum)
        controller.join(timeout=10)
        assert controller.exitcode == 0
        assert cleaned.read_text() == 'stopped'
        assert api_cleaned.read_text() == 'stopped'
        assert not _alive(api_details['pid'])
        _assert_listener_released(api_details['address'])
    finally:
        if controller.is_alive():
            controller.kill()
            controller.join(timeout=2)
        controller.close()


def test_signal_handlers_are_restored() -> None:
    """
    Verify signal handlers are restored.

    Returns:
        None: No value is returned.
    """
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    stopped = Shutdown()
    with runtime.signals(stopped):
        signal.raise_signal(signal.SIGTERM)
        assert stopped.is_set()
        signal.raise_signal(signal.SIGINT)
    assert {signum: signal.getsignal(signum) for signum in previous} == previous


def test_api_serves_in_child_main_thread_then_releases_its_listener(tmp_path: Path) -> None:
    """
    Verify api serves in child main thread then releases its listener.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        None: No value is returned.
    """
    report = tmp_path / 'api.json'
    cleaned = tmp_path / 'api-cleaned'
    supervisor = Supervisor([
        ProcessSpec('api', _reported_api, (_api_config(), report, cleaned)),
    ], mp.get_context('spawn'), shutdown_timeout=2)
    try:
        supervisor.start(Event())
        details = _wait_for_api(report)
        assert details['pid'] != os.getpid()
        assert details['parent'] == os.getpid()
        assert details['main_thread']
        for path in ('/metrics',):
            with urlopen(f"http://127.0.0.1:{details['address'][1]}{path}", timeout=2) as response:
                assert response.status == 200
    finally:
        supervisor.close()
    assert cleaned.read_text() == 'stopped'
    assert supervisor.exitcodes == {'api': 0}
    _assert_listener_released(details['address'])
    supervisor.close()


def test_api_serve_failure_propagates_and_closes_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify api serve failure propagates and closes listener.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    server = api.make_server('127.0.0.1', 0, api.create_app(), threaded=True)
    address = server.server_address

    def fail() -> None:
        """
        Simulate a listener failure.

        Returns:
            None: No value is returned.

        Raises:
            OSError: listener failed.
        """
        raise OSError('listener failed')

    monkeypatch.setattr(api, 'make_server', lambda *_args, **_kwargs: server)
    monkeypatch.setattr(api, 'setproctitle', lambda _title: None)
    monkeypatch.setattr(server, 'serve_forever', fail)
    try:
        with pytest.raises(OSError, match='listener failed'):
            api.run(_api_config())
        assert server.fileno() == -1
        _assert_listener_released(address)
    finally:
        server.server_close()


def test_partial_startup_failure_cleans_started_worker_and_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify partial startup failure cleans started worker and api.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    cleaned = tmp_path / 'partial-cleaned'
    report = tmp_path / 'api.json'
    api_cleaned = tmp_path / 'api-cleaned'
    specs = [
        ProcessSpec('api', _reported_api, (_api_config(), report, api_cleaned)),
        ProcessSpec('started', _wait_forever, (ready, cleaned)),
        ProcessSpec('fails', _stubborn, (ready,)),
    ]
    monkeypatch.setattr(runtime, 'build', lambda *_args: specs)
    monkeypatch.setattr(runtime, 'setproctitle', lambda _title: None)
    original_process = context.Process
    created: list[Any] = []

    def process_factory(*args: Any, **kwargs: Any) -> Any:
        """
        Start two workers and fail the next process construction.

        Args:
            *args (Any): Positional arguments forwarded to the wrapped callable.
            **kwargs (Any): Keyword arguments forwarded to the wrapped callable.

        Returns:
            Any: Start two workers and fail the next process construction.

        Raises:
            OSError: process startup failed.
        """
        if len(created) == 2:
            assert ready.wait(10)
            _wait_for_api(report)
            raise OSError('process startup failed')
        process = original_process(*args, **kwargs)
        created.append(process)
        return process

    monkeypatch.setattr(context, 'Process', process_factory)
    assert runtime.start(_api_config(), 'test', '') == 1
    assert cleaned.read_text() == 'stopped'
    assert api_cleaned.read_text() == 'stopped'
    details = _wait_for_api(report)
    assert not _alive(details['pid'])
    _assert_listener_released(details['address'])


def test_failed_api_bind_stops_controller_and_cleans_worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify failed api bind stops controller and cleans worker.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    context = mp.get_context('spawn')
    ready = context.Event()
    cleaned = tmp_path / 'bind-failed-cleaned'
    monkeypatch.setattr(runtime, 'setproctitle', lambda _title: None)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        config = _api_config(listener.getsockname()[1])
        specs = [
            ProcessSpec('api', _api_after_peer, (config, ready)),
            ProcessSpec('worker', _wait_forever, (ready, cleaned)),
        ]
        monkeypatch.setattr(runtime, 'build', lambda *_args: specs)
        assert runtime.start(config, 'test', '') == 1
    assert cleaned.read_text() == 'stopped'
