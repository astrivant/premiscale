"""
Check cluster targeting and failure handling without touching a developer's cluster.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import json
import os
from pathlib import Path
from shutil import copyfile
import subprocess
import sys

import pytest

if TYPE_CHECKING:
    from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'integrations/minikube/minikube.sh'


@pytest.fixture
def commands(tmp_path: Path) -> Callable[..., tuple[subprocess.CompletedProcess[str], list[list[str]]]]:
    """
    Record external commands and simulate targeted failures in an isolated PATH.

    Args:
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        Callable[..., tuple[subprocess.CompletedProcess[str], list[list[str]]]]: Callable returning the installer result and the external commands it invoked.
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    recorder = bin_dir / 'record'
    copyfile(ROOT / 'pkg/tests/data/processes/record_command.py', recorder)
    (bin_dir / 'python3').symlink_to(sys.executable)
    recorder.chmod(0o755)
    for executable in ('docker', 'helm', 'minikube', 'kubectl'):
        (bin_dir / executable).symlink_to(recorder)
    log = tmp_path / 'commands.jsonl'
    environment = {
        **os.environ,
        'PATH': f'{bin_dir}:/usr/bin:/bin',
        'COMMAND_LOG': str(log),
        'PREMISCALE_MINIKUBE_PROFILE': 'isolated-test',
        'PREMISCALE_MINIKUBE_NAMESPACE': 'test-namespace',
    }
    for key in ('PREMISCALE_MINIKUBE_CONFIG', 'PREMISCALE_MINIKUBE_VALUES', 'PREMISCALE_MINIKUBE_NODES', 'FAIL_COMMAND'):
        environment.pop(key, None)

    def run(*args: str, overrides: dict[str, str] | None=None) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
        """
        Run.

        Args:
            *args (str): Positional arguments forwarded to the wrapped callable.
            overrides (dict[str, str] | None): Environment overrides for the simulated installer command.

        Returns:
            tuple[subprocess.CompletedProcess[str], list[list[str]]]: Run.
        """
        log.write_text('')
        result = subprocess.run(['bash', str(SCRIPT), *args], cwd=tmp_path,
                                env={**environment, **(overrides or {})}, capture_output=True, text=True, check=False)
        return result, [json.loads(line) for line in log.read_text().splitlines()]

    return run


def test_start_scopes_all_commands_and_loads_the_built_image(commands: Any) -> None:
    """
    Verify start scopes all commands and loads the built image.

    Args:
        commands (Any): Installer runner that records external commands.

    Returns:
        None: No value is returned.
    """
    result, calls = commands('start')
    assert result.returncode == 0, result.stderr
    start = next(call for call in calls if call[0] == 'minikube' and 'start' in call)
    assert '--keep-context' in start
    assert start[start.index('--nodes') + 1] == '1'
    addons = start[start.index('--addons') + 1].split(',')
    assert addons == ['storage-provisioner', 'default-storageclass', 'metrics-server']
    for call in calls:
        if call[0] == 'minikube':
            assert call[1:3] == ['--profile', 'isolated-test']
        elif call[0] == 'kubectl':
            assert call[1:5] == ['--context', 'isolated-test', '--namespace', 'test-namespace']
    image_load = next(call for call in calls if call[0] == 'minikube' and 'load' in call)
    assert image_load[-1] == 'premiscale/premiscale:local-' + 'a' * 64
    upgrade = next(call for call in calls if call[0] == 'helm' and 'upgrade' in call)
    assert upgrade[upgrade.index('--kube-context') + 1] == 'isolated-test'
    assert upgrade[upgrade.index('--namespace') + 1] == 'test-namespace'
    assert 'premiscale.deployment.image.tag=local-' + 'a' * 64 in upgrade
    assert '--set-file' not in upgrade
    assert calls.index(image_load) < calls.index(upgrade)
    assert any('exec' in call and 'python' in call for call in calls)
    assert any('statefulset/premiscale-dragonfly' in call and 'status' in call for call in calls)


def test_build_failure_does_not_install_or_delete_anything(commands: Any) -> None:
    """
    Verify build failure does not install or delete anything.

    Args:
        commands (Any): Installer runner that records external commands.

    Returns:
        None: No value is returned.
    """
    result, calls = commands('start', overrides={'FAIL_COMMAND': 'docker buildx build'})
    assert result.returncode == 43
    assert not any('upgrade' in call or 'delete' in call or 'exec' in call for call in calls)


@pytest.mark.parametrize('command', ['stop', 'delete'])
def test_profile_cleanup_never_touches_other_profiles_or_docker_resources(commands: Any, command: str) -> None:
    """
    Verify profile cleanup never touches other profiles or docker resources.

    Args:
        commands (Any): Installer runner that records external commands.
        command (str): Installer lifecycle command under test.

    Returns:
        None: No value is returned.
    """
    result, calls = commands(command)
    assert result.returncode == 0
    assert calls == [['minikube', '--profile', 'isolated-test', command]]


def test_disable_keeps_namespace_and_storage(commands: Any) -> None:
    """
    Verify disable keeps namespace and storage.

    Args:
        commands (Any): Installer runner that records external commands.

    Returns:
        None: No value is returned.
    """
    result, calls = commands('disable')
    assert result.returncode == 0
    assert [call[0] for call in calls] == ['minikube', 'helm']
    assert calls[1][1:3] == ['uninstall', 'premiscale']
    assert '--ignore-not-found' in calls[1]
    assert calls[1][calls[1].index('--kube-context') + 1] == 'isolated-test'


def test_render_is_cluster_independent_and_accepts_paths_with_spaces(commands: Any, tmp_path: Path) -> None:
    """
    Verify render is cluster independent and accepts paths with spaces.

    Args:
        commands (Any): Installer runner that records external commands.
        tmp_path (Path): Isolated temporary directory supplied by pytest.

    Returns:
        None: No value is returned.
    """
    config = tmp_path / 'controller config.yaml'
    config.write_text('version: v1alpha1\n')
    result, calls = commands('render', overrides={'PREMISCALE_MINIKUBE_CONFIG': str(config)})
    assert result.returncode == 0
    assert {call[0] for call in calls} == {'helm'}
    rendered = next(call for call in calls if 'template' in call)
    assert f'premiscale.configMap.config={config}' in rendered
    assert '--include-crds' in rendered


@pytest.mark.parametrize('args', [[], ['unknown'], ['start', '--profile', 'other']])
def test_invalid_commands_do_not_touch_infrastructure(commands: Any, args: Any) -> None:
    """
    Verify invalid commands do not touch infrastructure.

    Args:
        commands (Any): Installer runner that records external commands.
        args (Any): Positional arguments forwarded to the wrapped callable.

    Returns:
        None: No value is returned.
    """
    result, calls = commands(*args)
    assert result.returncode == 2
    assert not calls


def test_invalid_profile_is_rejected_before_any_command(commands: Any) -> None:
    """
    Verify invalid profile is rejected before any command.

    Args:
        commands (Any): Installer runner that records external commands.

    Returns:
        None: No value is returned.
    """
    result, calls = commands('delete', overrides={'PREMISCALE_MINIKUBE_PROFILE': '--all'})
    assert result.returncode != 0
    assert not calls


def test_node_count_supports_distributed_controller_placement(commands: Any) -> None:
    """
    Pass an explicit node count to Minikube for the HA controller placement rules.

    Args:
        commands (Any): Installer runner that records external commands.

    Returns:
        None: No value is returned.
    """
    result, calls = commands('start', overrides={'PREMISCALE_MINIKUBE_NODES': '3'})
    assert result.returncode == 0, result.stderr
    start = next(call for call in calls if call[0] == 'minikube' and 'start' in call)
    assert start[start.index('--nodes') + 1] == '3'
