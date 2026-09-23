"""
Monitor long-running processes and bound shutdown of their entire process groups.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import logging
from multiprocessing.connection import wait
import os
import signal
import sys
from time import monotonic

from setproctitle import setproctitle

from premiscale.status.store import StatusStore

if TYPE_CHECKING:
    from types import FrameType
    from multiprocessing.context import SpawnContext
    from multiprocessing.process import BaseProcess
    from threading import Event
    from .processes import ProcessSpec
    from .shutdown import Shutdown


log = logging.getLogger(__name__)


def _terminate(_signum: int, _frame: FrameType | None) -> None:
    """
    Unwind the worker on SIGTERM while ignoring repeated termination requests.

    Args:
        _signum (int): Signal number supplied by the operating system.
        _frame (FrameType | None): Interrupted Python stack frame, when available.

    Returns:
        None: No value is returned.

    Raises:
        SystemExit: With status zero when SIGTERM requests worker shutdown.
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise SystemExit(0)


def _run(spec: ProcessSpec, log_level: int, isolate_process_groups: bool = True,
         status_directory: str | None = None) -> None:
    """
    Isolate descendants and let SIGTERM unwind Python resource cleanup.

    Args:
        spec (ProcessSpec): Service entry point and its startup arguments.
        log_level (int): Logging level inherited from the parent process.
        isolate_process_groups (bool): Whether to give this service its own process group.
        status_directory (str | None): Shared run directory, or None when status reporting is disabled.

    Returns:
        None: No value is returned.

    Raises:
        SystemExit: With the entry point's status when it returns an explicit exit code.
    """
    if isolate_process_groups:
        os.setsid()
    setproctitle(f'premiscale-{spec.name}')
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, _terminate)
    logging.basicConfig(level=log_level, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    status_directory = status_directory or os.environ.get('PREMISCALE_RUNTIME_DIRECTORY')
    store = StatusStore(status_directory) if status_directory else None
    if store is not None:
        os.environ['PREMISCALE_RUNTIME_DIRECTORY'] = str(store.directory)
        os.environ['PREMISCALE_PROCESS_NAME'] = spec.name
        store.write(f'process-{spec.name}', {'phase': 'starting'})
    try:
        result = spec.target(*spec.args)
        if result is not None:
            raise SystemExit(result)
    finally:
        if store is not None:
            store.write(f'process-{spec.name}', {'phase': 'stopped'})


class Supervisor:
    """
    Own explicit processes instead of cancelling futures for infinite tasks.
    """

    def __init__(self, specs: list[ProcessSpec], context: SpawnContext,
                 shutdown_timeout: float = 10, kill_timeout: float = 5,
                 isolate_process_groups: bool = True, status_store: StatusStore | None = None) -> None:
        """
        Initialize Supervisor with the supplied settings.

        Args:
            specs (list[ProcessSpec]): Services to launch and supervise.
            context (SpawnContext): Execution context for the operation.
            shutdown_timeout (float): Shared grace period for worker cleanup, in seconds.
            kill_timeout (float): Shared deadline for reaping killed workers, in seconds.
            isolate_process_groups (bool): False keeps nested workers in their parent's process group.
            status_store (StatusStore | None): Shared status writer used only by the top-level supervisor.
        """
        self.specs = specs
        self.context = context
        self.shutdown_timeout = shutdown_timeout
        self.kill_timeout = kill_timeout
        self.isolate_process_groups = isolate_process_groups
        self.processes: list[tuple[ProcessSpec, BaseProcess]] = []
        self.exitcodes: dict[str, int | None] = {}
        self.closed = False
        self.status_store = status_store
        self._last_report = 0.0

    def report(self, phase: str = 'running') -> None:
        """
        Publish supervised process identities without reaping their reserved PIDs.

        Args:
            phase (str): Parent lifecycle phase exposed to health checks.

        Returns:
            None: No value is returned.
        """
        if self.status_store is not None:
            self.status_store.write('supervisor', {
                'phase': phase,
                'services': {spec.name: {'pid': process.pid, 'required': spec.required}
                             for spec, process in self.processes},
            })
            self._last_report = monotonic()

    def start(self, stopped: Event | Shutdown) -> None:
        """
        Launch importable entry points and retain partially started processes for cleanup.

        Args:
            stopped (Event | Shutdown): Flag indicating that controller shutdown has been requested.

        Returns:
            None: No value is returned.
        """
        for spec in self.specs:
            if stopped.is_set():
                break
            process = self.context.Process(
                name=spec.name, target=_run,
                args=(spec, logging.getLogger().getEffectiveLevel(), self.isolate_process_groups,
                      str(self.status_store.directory) if self.status_store is not None else None),
            )
            self.processes.append((spec, process))
            process.start()
        self.report()

    def wait(self, stopped: Event | Shutdown) -> int:
        """
        Fail on any required service exit, including an unexpected successful return.

        Args:
            stopped (Event | Shutdown): Flag indicating that controller shutdown has been requested.

        Returns:
            int: Zero for requested shutdown or optional-service completion; otherwise a failure status.
        """
        while not stopped.is_set():
            status = self.poll()
            if stopped.is_set():
                return 0
            if status is not None:
                return status
        return 0

    def poll(self, timeout: float = 0.1) -> int | None:
        """
        Monitor one interval so a supervisor can also schedule reconciliation passes.

        Args:
            timeout (float): Maximum time to wait for a child exit, in seconds.

        Returns:
            int | None: Failure status, zero when no services remain, or None while running.

        Raises:
            RuntimeError: If an exited worker cannot be reaped before the kill deadline.
        """
        if not self.processes:
            return 0
        if monotonic() - self._last_report >= 1:
            self.report()
        ready = wait([process.sentinel for _, process in self.processes], timeout=timeout)
        for spec, process in list(self.processes):
            if process.sentinel not in ready:
                continue
            # Clean the group before reaping its leader so its PID cannot be reused.
            self._signal(process, signal.SIGTERM)
            self._signal(process, signal.SIGKILL)
            if not self._reap(spec, process, self.kill_timeout):
                raise RuntimeError(f'Worker process did not exit: {spec.name}')
            code = self.exitcodes[spec.name]
            if code != 0 or spec.required:
                self.report('failed')
                log.error('Service %s exited unexpectedly with code %s', spec.name, code)
                return code if code is not None and code > 0 else 1
            log.info('Optional service %s is disabled', spec.name)
        return None

    def _signal(self, process: BaseProcess, signum: int) -> None:
        """
        Signal the owned child or its isolated group without reaping its PID first.

        Args:
            process (BaseProcess): Child process owned by the supervisor.
            signum (int): POSIX signal number to deliver.

        Returns:
            None: No value is returned.

        Raises:
            PermissionError: If the underlying operation fails after cleanup.
        """
        if process.pid is None:
            return
        try:
            if self.isolate_process_groups:
                os.killpg(process.pid, signum)
            else:
                os.kill(process.pid, signum)
        except PermissionError:
            # Darwin's killpg filters out zombies and returns EPERM for an otherwise
            # empty group. File-descriptor teardown can lag that transition briefly;
            # wait for the exit sentinel without reaping the leader's reserved PID.
            if sys.platform != 'darwin' or not wait([process.sentinel], timeout=0.1):
                raise
        except ProcessLookupError:
            # Startup may have failed before the entry point created its session.
            # Avoid Process.is_alive()/exitcode here: they can reap the leader.
            try:
                os.kill(process.pid, signum)
            except ProcessLookupError:
                pass

    def _reap(self, spec: ProcessSpec, process: BaseProcess, timeout: float) -> bool:
        """
        Join a worker and release its process handle after it exits.

        Args:
            spec (ProcessSpec): Service entry point and its startup arguments.
            process (BaseProcess): Child process owned by the supervisor.
            timeout (float): Maximum time to wait, in seconds.

        Returns:
            bool: True after the process exits and its handle is closed; False if the deadline expires.
        """
        if process.pid is not None:
            process.join(timeout=timeout)
            code = process.exitcode
            if code is None:
                return False
            self.exitcodes[spec.name] = code
        process.close()
        self.processes.remove((spec, process))
        return True

    def close(self) -> None:
        """
        Allow cleanup, kill remaining workers or descendants, then reap and close handles.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If any worker remains unreaped after forced shutdown.
        """
        if self.closed:
            return
        self.report('stopping')
        for spec, process in list(self.processes):
            if process.pid is None:
                self._reap(spec, process, 0)
                continue
            self._signal(process, signal.SIGTERM)
        deadline = monotonic() + self.shutdown_timeout
        while self.processes and (remaining := deadline - monotonic()) > 0:
            ready = wait([process.sentinel for _, process in self.processes], timeout=remaining)
            for spec, process in list(self.processes):
                if process.sentinel in ready:
                    # Keep the zombie leader's PID reserved until its descendants are stopped.
                    self._signal(process, signal.SIGKILL)
                    self._reap(spec, process, max(0, deadline - monotonic()))
        for spec, process in self.processes:
            log.warning('Service %s did not stop; forcing termination', spec.name)
            self._signal(process, signal.SIGKILL)
        deadline = monotonic() + self.kill_timeout
        failures = []
        for spec, process in list(self.processes):
            if not self._reap(spec, process, max(0, deadline - monotonic())):
                failures.append(spec.name)
        self.closed = True
        if failures:
            raise RuntimeError(f'Worker processes did not exit: {failures}')
