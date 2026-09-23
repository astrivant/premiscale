"""
Start the real worker daemon for process lifecycle integration tests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from premiscale.daemon.runtime import start

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config
    from premiscale.daemon.settings import Execution


def run(config: Config, execution: Execution) -> None:
    """
    Propagate the daemon result to the spawned process exit status.

    Args:
        config (Config): Isolated worker and connection settings.
        execution (Execution): Worker role under test.

    Returns:
        None: No value is returned.

    Raises:
        SystemExit: With the daemon shutdown or failure status.
    """
    raise SystemExit(start(config, 'test', '', execution))
