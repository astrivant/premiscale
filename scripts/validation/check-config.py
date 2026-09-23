"""
Validate configuration examples against the packaged CRD-derived JSON schema.
"""

import argparse
from pathlib import Path

from premiscale.config.parse import validateConfig


def main() -> int:
    """
    Check every supplied configuration and report failure without starting services.

    Returns:
        int: Zero when every configuration validates, otherwise one.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    parser.add_argument('paths', nargs='*', type=Path, default=[
        root / 'pkg/premiscale/config/default.yaml',
        *sorted((root / 'pkg/tests/data/config').glob('*.yaml')),
        root / '.config/minikube/controller.yaml',
        root / '.config/keda/controller.yaml',
    ])
    options = parser.parse_args()
    failed = False
    for path in options.paths:
        if not path.is_file():
            print(f'Configuration does not exist: {path}')
            failed = True
        elif not validateConfig(str(path)):
            print(f'Invalid configuration: {path}')
            failed = True
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
