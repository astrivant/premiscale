"""
Prepare SSH configuration and credentials when opening hypervisor connections.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import fcntl
from importlib.resources import files
import logging
import os
from pathlib import Path
import re
from string import Template
from tempfile import NamedTemporaryFile
from threading import Lock


if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Host


log = logging.getLogger(__name__)
_lock = Lock()
_host_config = Template(files('premiscale.support').joinpath('ssh/host.conf').read_text(encoding='utf-8'))


def _write_key(path: Path, contents: str) -> None:
    """
    Replace a private key atomically without exposing partial or permissive files.

    Args:
        path (Path): Destination private-key file.
        contents (str): Expanded private-key contents.

    Returns:
        None: No value is returned.
    """
    contents = contents.rstrip('\n') + '\n'
    if path.is_file() and not path.is_symlink() and path.read_text(encoding='utf-8') == contents:
        path.chmod(0o600)
        return
    with NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            temporary.write(contents)
            temporary.flush()
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)


def configure_ssh(host: Host, directory: Path | None = None) -> None:
    """
    Install host SSH settings and optional credentials on the local filesystem before connecting.

    TLS hosts require no SSH files. Existing exact host entries are preserved;
    keys can still be refreshed. Thread and file locks serialize worker updates.

    Args:
        host (Host): Expanded host connection settings and optional private key.
        directory (Path | None): SSH directory; defaults to the current user's ~/.ssh.

    Returns:
        None: No value is returned.

    Raises:
        ValueError: If a host name or address cannot safely identify an SSH entry or key file.
    """
    if host.protocol.lower() != 'ssh':
        return
    if (not re.fullmatch(r'[A-Za-z0-9_.-]+', host.name)
            or host.name in {'.', '..', 'config', 'authorized_keys', 'known_hosts'}
            or not host.address or any(character.isspace() for character in host.address)):
        raise ValueError('Invalid SSH host name or address')
    directory = directory if directory is not None else Path.home() / '.ssh'
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    key_path = directory / host.name
    with _lock, os.fdopen(os.open(directory / 'config', os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600),
                          mode='a+', encoding='utf-8') as config:
        fcntl.flock(config.fileno(), fcntl.LOCK_EX)
        if host.sshKey is not None:
            _write_key(key_path, host.sshKey)
        config.seek(0)
        contents = config.read()
        if re.search(rf'(?im)^[ \t]*Host[ \t]+{re.escape(host.address)}[ \t]*(?:#.*)?$', contents):
            return
        if contents and not contents.endswith('\n'):
            config.write('\n')
        config.write(_host_config.substitute(address=host.address, timeout=host.timeout, key_path=key_path))
    log.debug('Configured SSH connection to %s with a timeout of %s seconds', host.address, host.timeout)
