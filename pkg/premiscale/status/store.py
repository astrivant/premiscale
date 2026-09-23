"""
Publish atomic snapshots and cache reads without a management process.
"""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import re
import tempfile
from threading import RLock
from time import monotonic, time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


class StatusStore:
    """
    Share snapshots whose individual files each have one owning writer process.
    """

    def __init__(self, directory: str | Path, cache_seconds: float = 1) -> None:
        """
        Create an independent reader and writer for a controller run directory.

        Args:
            directory (str | Path): Private directory shared by this controller's children.
            cache_seconds (float): Maximum interval between reads of a snapshot file.
        """
        self.directory = Path(directory)
        self.cache_seconds = cache_seconds
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._lock = RLock()

    def _path(self, source: str) -> Path:
        """
        Restrict source identities to filenames within the shared directory.

        Args:
            source (str): Stable identity of the snapshot's owning writer.

        Returns:
            Path: Snapshot file within the run directory.

        Raises:
            ValueError: If the source contains path separators or unsupported characters.
        """
        if re.fullmatch(r'[a-zA-Z0-9_-]+', source) is None:
            raise ValueError('Invalid status source')
        return self.directory / f'{source}.json'

    def write(self, source: str, data: dict[str, Any]) -> None:
        """
        Replace one complete snapshot without exposing partially written JSON.

        Args:
            source (str): Identity owned exclusively by the calling writer.
            data (dict[str, Any]): JSON-compatible observation without credentials.

        Returns:
            None: No value is returned.
        """
        path = self._path(source)
        content = json.dumps({'updatedAt': time(), 'pid': os.getpid(), 'data': data}, allow_nan=False)
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=self.directory,
                                         prefix=f'.{source}-', delete=False) as temporary:
            pending = Path(temporary.name)
            try:
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
                os.replace(pending, path)
            finally:
                pending.unlink(missing_ok=True)
        with self._lock:
            self._cache.pop(source, None)

    def read(self, source: str, max_age: float | None = 15) -> dict[str, Any] | None:
        """
        Read a cached snapshot while enforcing freshness on every access.

        Args:
            source (str): Identity of the snapshot to read.
            max_age (float | None): Maximum observation age, or None for lifecycle markers.

        Returns:
            dict[str, Any] | None: Independent snapshot, or None for missing, stale, or corrupt data.
        """
        path = self._path(source)
        with self._lock:
            cached = self._cache.get(source)
            try:
                if cached is None or monotonic() - cached[0] >= self.cache_seconds:
                    value = json.loads(path.read_text(encoding='utf-8'))
                    if not isinstance(value, dict) or not isinstance(value.get('data'), dict):
                        return None
                    self._cache[source] = (monotonic(), value)
                else:
                    value = cached[1]
                age = time() - float(value['updatedAt'])
                if not -5 <= age or (max_age is not None and age > max_age):
                    return None
                return deepcopy(value)
            except (OSError, ValueError, TypeError, KeyError):
                self._cache.pop(source, None)
                return None


def current_store() -> StatusStore | None:
    """
    Locate the run directory inherited from the supervising parent.

    Returns:
        StatusStore | None: Process-local store, or None outside a supervised daemon.
    """
    directory = os.environ.get('PREMISCALE_RUNTIME_DIRECTORY')
    return StatusStore(directory) if directory else None


def ready() -> None:
    """
    Mark the current service ready after its essential resources are initialized.

    Returns:
        None: No value is returned.
    """
    store = current_store()
    name = os.environ.get('PREMISCALE_PROCESS_NAME')
    if store is not None and name:
        store.write(f'process-{name}', {'phase': 'ready'})
