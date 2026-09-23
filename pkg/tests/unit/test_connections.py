"""
Verify SSH setup happens in connection code and remains safe across workers.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
import multiprocessing as mp
from stat import S_IMODE
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from premiscale.config.v1alpha1 import Host
from premiscale.connections.ssh import configure_ssh
from premiscale import hypervisor
from premiscale_cluster_autoscaler import libvirt as autoscaler_libvirt
from .test_cluster_autoscaler import config as config

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def test_host_construction_only_expands_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Construct configuration without opening files or exposing expanded credentials.

    Args:
        monkeypatch (pytest.MonkeyPatch): Fixture restoring test credential environment variables.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setenv('PREMISCALE_TEST_SSH_KEY', 'private-test-key')
    monkeypatch.setenv('PREMISCALE_TEST_SSH_USER', 'admin')
    with patch('builtins.open', side_effect=AssertionError('Configuration must not open SSH files')):
        host = Host('host-1', '192.0.2.1', 'ssh', 22, 'qemu',
                    sshKey='$PREMISCALE_TEST_SSH_KEY', user='$PREMISCALE_TEST_SSH_USER')
    assert host.user == 'admin'
    assert host.sshKey == 'private-test-key'
    assert 'private-test-key' not in repr(host)


def test_ssh_setup_is_repeatable_and_refreshes_private_keys(tmp_path: Path) -> None:
    """
    Create private SSH files once and refresh credentials without duplicating entries.

    Args:
        tmp_path (Path): Isolated parent directory for SSH files.

    Returns:
        None: No value is returned.
    """
    directory = tmp_path / '.ssh'
    host = Host('host-1', '192.0.2.1', 'ssh', 22, 'qemu', sshKey='first-key', timeout=13)
    configure_ssh(host, directory)
    original = (directory / 'config').read_text()
    configure_ssh(host, directory)
    assert (directory / 'config').read_text() == original
    assert 'ConnectTimeout 13' in original
    assert f'IdentityFile "{directory / host.name}"' in original
    assert S_IMODE(directory.stat().st_mode) == 0o700
    assert S_IMODE((directory / 'config').stat().st_mode) == 0o600
    host.sshKey = 'rotated-key\n'
    (directory / host.name).chmod(0o644)
    configure_ssh(host, directory)
    assert (directory / host.name).read_text() == 'rotated-key\n'
    assert S_IMODE((directory / host.name).stat().st_mode) == 0o600
    assert (directory / 'config').read_text() == original
    assert {path.name for path in directory.iterdir()} == {'config', 'host-1'}


def test_existing_ssh_entries_are_preserved_and_matched_exactly(tmp_path: Path) -> None:
    """
    Preserve custom host settings without confusing addresses that share a prefix.

    Args:
        tmp_path (Path): Isolated SSH directory.

    Returns:
        None: No value is returned.
    """
    original = 'Host 192.0.2.10\n\tConnectTimeout 99\n\tIdentityFile /custom/key'
    (tmp_path / 'config').write_text(original)
    configure_ssh(Host('host-1', '192.0.2.1', 'ssh', 22, 'qemu'), tmp_path)
    configure_ssh(Host('host-10', '192.0.2.10', 'ssh', 22, 'qemu'), tmp_path)
    contents = (tmp_path / 'config').read_text()
    assert contents.startswith(original + '\n')
    assert contents.splitlines().count('Host 192.0.2.10') == 1
    assert contents.splitlines().count('Host 192.0.2.1') == 1


def test_tls_does_not_prepare_ssh_files(tmp_path: Path) -> None:
    """
    Skip all SSH filesystem work for TLS connections.

    Args:
        tmp_path (Path): Isolated parent directory for the nonexistent SSH directory.

    Returns:
        None: No value is returned.
    """
    directory = tmp_path / '.ssh'
    configure_ssh(Host('host-1', '192.0.2.1', 'tls', 16514, 'qemu'), directory)
    assert not directory.exists()


@pytest.mark.parametrize('name', ['../key', 'config', 'known_hosts', 'bad\nname'])
def test_ssh_key_names_cannot_overwrite_transport_files(name: str, tmp_path: Path) -> None:
    """
    Reject key paths that escape their directory or replace SSH control files.

    Args:
        name (str): Unsafe host name used as a private-key filename.
        tmp_path (Path): Isolated parent directory for SSH files.

    Returns:
        None: No value is returned.
    """
    directory = tmp_path / '.ssh'
    with pytest.raises(ValueError, match='Invalid SSH host'):
        configure_ssh(Host(name, '192.0.2.1', 'ssh', 22, 'qemu', sshKey='test-key'), directory)
    assert not directory.exists()


def _prepare_hosts(hosts: list[Host], directory: Path) -> None:
    """
    Simulate a collector process preparing connections through several threads.

    Args:
        hosts (list[Host]): Hosts each worker will repeatedly configure.
        directory (Path): SSH directory shared by the test workers.

    Returns:
        None: No value is returned.
    """
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(partial(configure_ssh, directory=directory), hosts * 3))


def test_processes_and_threads_share_ssh_configuration(tmp_path: Path) -> None:
    """
    Serialize concurrent writers without duplicate entries or partially written keys.

    Args:
        tmp_path (Path): Parent directory for shared SSH files.

    Returns:
        None: No value is returned.
    """
    directory = tmp_path / '.ssh'
    hosts = [Host(f'host-{index}', f'192.0.2.{index}', 'ssh', 22, 'qemu', sshKey=f'key-{index}')
             for index in range(1, 5)]
    with ProcessPoolExecutor(max_workers=3, mp_context=mp.get_context('spawn')) as executor:
        futures = [executor.submit(_prepare_hosts, hosts, directory) for _ in range(3)]
        for future in futures:
            future.result(timeout=15)
    contents = (directory / 'config').read_text()
    for host in hosts:
        assert host.sshKey is not None
        assert contents.splitlines().count(f'Host {host.address}') == 1
        assert (directory / host.name).read_text() == host.sshKey + '\n'
    assert len(contents.splitlines()) == len(hosts) * 4


def test_metrics_and_autoscaler_prepare_transport_before_connecting(config: Any, tmp_path: Path,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Use the same transport helper for metrics collection and VM operations.

    Args:
        config (Any): Controller test configuration with a host and node group.
        tmp_path (Path): Parent directory for isolated SSH files.
        monkeypatch (pytest.MonkeyPatch): Fixture restoring transport and libvirt functions.

    Returns:
        None: No value is returned.
    """
    directory = tmp_path / '.ssh'
    prepare = Mock(side_effect=partial(configure_ssh, directory=directory))
    monkeypatch.setattr(hypervisor, 'configure_ssh', prepare)
    monkeypatch.setattr(autoscaler_libvirt, 'configure_ssh', prepare)
    driver = autoscaler_libvirt.LibvirtDriver(config)
    host = config.controller.autoscale.hosts[0]
    assert not directory.exists()
    hypervisor.build_hypervisor_connection(host, readonly=True)
    assert (directory / 'config').is_file()
    connection = Mock()

    def connect(_uri: str) -> Mock:
        """
        Check transport preparation before returning a fake libvirt connection.

        Args:
            _uri (str): Libvirt URI supplied by the driver.

        Returns:
            Mock: Open connection test double.
        """
        assert prepare.call_count == 2
        return connection

    monkeypatch.setattr(autoscaler_libvirt.libvirt, 'open', connect)
    with driver.connect(host.name) as opened:
        assert opened is connection
    connection.close.assert_called_once_with()
