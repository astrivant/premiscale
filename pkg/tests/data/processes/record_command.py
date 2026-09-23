#!/usr/bin/env python3
"""
Record Minikube integration commands without contacting Docker or Kubernetes.
"""

import json
import os
from pathlib import Path
import sys


def main() -> int:
    """
    Record this invocation and simulate image inspection or a selected failure.

    Returns:
        int: Status 43 for an injected failure, otherwise zero.
    """
    command = [Path(sys.argv[0]).name, *sys.argv[1:]]
    with Path(os.environ['COMMAND_LOG']).open('a') as output:
        output.write(json.dumps(command) + '\n')
    if os.environ.get('FAIL_COMMAND') and os.environ['FAIL_COMMAND'] in ' '.join(command):
        return 43
    if command[0] == 'docker' and command[1:3] == ['image', 'inspect']:
        print('sha256:' + 'a' * 64)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
