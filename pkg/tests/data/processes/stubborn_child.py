"""
Remain alive after the parent exits to exercise process-group shutdown.
"""

import os
from pathlib import Path
import signal
import sys
import time


def main() -> None:
    """
    Ignore termination, publish this process ID, and wait for forced cleanup.

    Returns:
        None: No value is returned.
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(sys.argv[1]).write_text(str(os.getpid()))
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
