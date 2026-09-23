"""
Count logical CPUs available to the controller, including container quotas.
"""

from math import ceil
import os
from pathlib import Path, PurePosixPath


def available_cores(cgroup_root: Path = Path('/sys/fs/cgroup'),
                    membership: Path = Path('/proc/self/cgroup')) -> int:
    """
    Cap affinity-aware CPU availability by visible cgroup v1 and v2 quotas.

    Fractional quotas receive one worker for the final partial CPU. Missing or
    unreadable cgroup files leave the operating system's CPU count unchanged.

    Args:
        cgroup_root (Path): Root of the visible cgroup filesystem.
        membership (Path): Proc file describing the current process's cgroups.

    Returns:
        int: At least one worker, bounded by CPU affinity and applicable quotas.
    """
    count = os.process_cpu_count() or 1
    roots = [(cgroup_root, PurePosixPath('/')),
             (cgroup_root / 'cpu', PurePosixPath('/')),
             (cgroup_root / 'cpu,cpuacct', PurePosixPath('/'))]
    try:
        for line in membership.read_text().splitlines():
            _, controllers, location = line.split(':', 2)
            path = PurePosixPath(location)
            if not path.is_absolute() or '..' in path.parts:
                continue
            if not controllers:
                roots.append((cgroup_root, path))
            elif 'cpu' in controllers.split(','):
                roots.extend((root, path) for root in (cgroup_root / 'cpu', cgroup_root / 'cpu,cpuacct'))
    except (OSError, ValueError):
        pass
    directories: set[Path] = set()
    for root, path in roots:
        directories.update(root / ancestor.relative_to('/') for ancestor in (path, *path.parents))
    for directory in directories:
        try:
            quota, period = directory.joinpath('cpu.max').read_text().split()
            if quota != 'max' and int(quota) > 0 and int(period) > 0:
                count = min(count, ceil(int(quota) / int(period)))
        except (OSError, ValueError):
            pass
        try:
            quota_value = int(directory.joinpath('cpu.cfs_quota_us').read_text())
            period_value = int(directory.joinpath('cpu.cfs_period_us').read_text())
            if quota_value > 0 and period_value > 0:
                count = min(count, ceil(quota_value / period_value))
        except (OSError, ValueError):
            pass
    return max(1, count)
