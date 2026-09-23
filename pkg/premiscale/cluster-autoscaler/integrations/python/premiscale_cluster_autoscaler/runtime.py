"""
Supervise the native provider frontend and Python lifecycle backend together.
"""

from __future__ import annotations

from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
import fcntl
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
from threading import Event
from typing import TYPE_CHECKING

from setproctitle import setproctitle

from premiscale.connections.journal import Ownership
from premiscale.status.store import current_store, ready
from .libvirt import LibvirtDriver
from .provider import Provider
from .server import create_server
from .toolchain import ensure_binary

if TYPE_CHECKING:
    from typing import Iterator
    from premiscale.config.v1alpha1 import Config
    from .provider import Driver


@dataclass
class Runtime:
    """
    The public adapter's address and supervised child process.

    Attributes:
        address (str): Host or service address.
        process (subprocess.Popen): Supervised native Go frontend process.
        provider (Provider): Python lifecycle provider.
        ownership (Ownership | None): Optional shared PostgreSQL ownership session.
    """

    address: str
    process: subprocess.Popen
    provider: Provider
    ownership: Ownership | None = None


@contextmanager
def serve(config: Config, driver: Driver | None = None) -> Iterator[Runtime]:
    """
    Start both services and clean them up together on exit or startup failure.

    Args:
        config (Config): Parsed controller configuration.
        driver (Driver | None): Hypervisor implementation used for discovery and VM operations.

    Yields:
        Runtime: Resource available for the duration of the context.

    Raises:
        RuntimeError: If another process owns the journal, or the Go frontend fails or times out during startup.
    """
    binary = ensure_binary()
    settings = config.controller.kubernetes
    state_path = Path(settings.stateFile).expanduser()
    with ExitStack() as resources, tempfile.TemporaryDirectory(prefix='premiscale-grpc-') as temporary:
        dsn = getattr(settings, 'stateDsn', '')
        owner = None
        if dsn:
            owner = Ownership(dsn, settings.clusterName, 'provider')
            resources.callback(owner.close)
        else:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            lock = resources.enter_context(Path(str(state_path) + '.lock').open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError('Another provider already owns this operation journal') from error
        provider = Provider(config, driver if driver is not None else LibvirtDriver(config), str(state_path))
        backend = None
        process = None
        try:
            provider.start()
            target = f'unix:{temporary}/backend.sock'
            backend = create_server(provider, target)
            backend.start()
            host = f'[{settings.providerHost}]' if ':' in settings.providerHost else settings.providerHost
            command = [str(binary), '--listen', f'{host}:{settings.providerPort}', '--backend', target]
            for option, value in [('tls-cert', settings.providerCert), ('tls-key', settings.providerKey), ('tls-ca', settings.clientCA)]:
                if value:
                    command.extend([f'--{option}', os.path.expandvars(os.path.expanduser(value))])
            process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            assert process.stdout is not None
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(30):
                    raise RuntimeError('Timed out waiting for the Go provider to listen')
                address = process.stdout.readline().strip()
            if not address or process.poll() is not None:
                raise RuntimeError('Go provider exited during startup')
            yield Runtime(address, process, provider, owner)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                if process.stdout is not None:
                    process.stdout.close()
            if backend is not None:
                backend.stop(5).wait()
            provider.close()


class KubernetesAutoscaler:
    """
    Run the external cloud provider as one controller subprocess.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize KubernetesAutoscaler with the supplied settings.

        Args:
            config (Config): Parsed controller configuration.
        """
        self.config = config

    def __call__(self) -> None:
        """
        Run the KubernetesAutoscaler service until shutdown.

        Returns:
            None: No value is returned.

        Raises:
            RuntimeError: If the supervised Go frontend exits before shutdown is requested.
        """
        setproctitle('cluster-autoscaler-provider')
        stopped = Event()
        previous = {}
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda _signal, _frame: stopped.set())
        try:
            with serve(self.config) as runtime:
                store = current_store()
                ready()
                while not stopped.wait(0.5):
                    if runtime.ownership is not None:
                        runtime.ownership.check()
                    if runtime.process.poll() is not None:
                        raise RuntimeError(f'Go autoscaler provider exited with code {runtime.process.returncode}')
                    if runtime.provider.worker is None or not runtime.provider.worker.is_alive():
                        raise RuntimeError('The VM lifecycle worker exited unexpectedly')
                    if store is not None:
                        store.write('autoscaler', runtime.provider.observations())
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
