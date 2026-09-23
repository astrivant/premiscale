"""
Build the packaged Go adapter into a source-addressed cache.
"""

import fcntl
import hashlib
from importlib import resources
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile


def ensure_binary() -> Path:
    """
    Use an explicitly supplied binary or compile the pinned packaged source.

    Returns:
        Path: Executable path supplied by the environment or built from the packaged Go sources.

    Raises:
        ValueError: PREMISCALE_AUTOSCALER_BINARY must name an executable file.
        RuntimeError: Install Go 1.25+ or set PREMISCALE_AUTOSCALER_BINARY to a prebuilt adapter.
    """
    supplied = os.environ.get('PREMISCALE_AUTOSCALER_BINARY')
    if supplied:
        path = Path(supplied).expanduser().resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError('PREMISCALE_AUTOSCALER_BINARY must name an executable file')
        return path
    source = resources.files('premiscale').joinpath('cluster-autoscaler')
    with resources.as_file(source) as directory:
        digest = hashlib.sha256(f'{platform.system()}/{platform.machine()}'.encode())
        for path in sorted(directory.rglob('*')):
            if 'integrations' in path.relative_to(directory).parts or path.name.endswith('_test.go'):
                continue
            if path.is_file() and (path.suffix == '.go' or path.name in {'go.mod', 'go.sum'}):
                digest.update(str(path.relative_to(directory)).encode())
                digest.update(path.read_bytes())
        cache = Path(os.environ.get('XDG_CACHE_HOME', '~/.cache')).expanduser() / 'premiscale/autoscaler'
        destination = cache / digest.hexdigest() / 'premiscale-autoscaler'
        if destination.is_file():
            return destination
        compiler = shutil.which('go')
        if compiler is None:
            raise RuntimeError('Install Go 1.25+ or set PREMISCALE_AUTOSCALER_BINARY to a prebuilt adapter')
        destination.parent.mkdir(parents=True, exist_ok=True)
        with (destination.parent / 'build.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if not destination.is_file():
                with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
                    output = Path(temporary) / destination.name
                    subprocess.run(
                        [compiler, 'build', '-mod=readonly', '-trimpath', '-o', str(output), './cmd/premiscale-autoscaler'],
                        cwd=directory, check=True, timeout=300,
                        env={**os.environ, 'GOWORK': 'off', 'GOTOOLCHAIN': 'auto', 'CGO_ENABLED': '0',
                             'GOOS': {'Darwin': 'darwin', 'Linux': 'linux'}[platform.system()],
                             'GOARCH': {'arm64': 'arm64', 'aarch64': 'arm64', 'x86_64': 'amd64'}[platform.machine()],
                             'GOMODCACHE': str(cache / 'go/modules'), 'GOCACHE': str(cache / 'go/build')},
                    )
                    output.replace(destination)
        return destination
