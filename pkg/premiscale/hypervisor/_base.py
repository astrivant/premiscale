"""
Provide methods to interact with the Libvirt API.
"""


from __future__ import annotations

import libvirt as lv
import logging

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from libvirt import libvirtError
from functools import wraps


if TYPE_CHECKING:
    from ipaddress import IPv4Address
    from premiscale.schemas.qemu import RawDomainStats, HostStats
    from typing import Any, Dict, Callable


log = logging.getLogger(__name__)


def retry_libvirt_connection(retries: int = 3) -> Callable:
    """
    Decorator to retry a connection to the Libvirt hypervisor if it fails.

    Args:
        retries (int): Number of times to retry the connection. Defaults to 3.

    Returns:
        Callable: The decorated function.
    """
    def decorator(func: Callable) -> Callable:
        """
        Wrap the supplied operation with retry handling.

        Args:
            func (Callable): Hypervisor operation to retry after reconnecting.

        Returns:
            Callable: Hypervisor operation wrapper that retries after reconnecting.
        """
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Run the wrapped operation with the configured retry policy.

            Args:
                *args (Any): Positional arguments forwarded to the wrapped callable.
                **kwargs (Any): Keyword arguments forwarded to the wrapped callable.

            Returns:
                Any: Wrapped operation result, or None when reconnection attempts are exhausted.
            """
            nonlocal retries

            self_ = args[0]
            assert isinstance(self_, Libvirt)
            tries = 0

            while tries < retries:
                try:
                    if self_.is_connected():
                        return func(*args, **kwargs)
                    else:
                        log.warning(f'Connection to host at "{self_.connection_string}" is not open, attempting to reconnect')
                        self_.open()
                        tries += 1
                        continue

                except libvirtError as e:
                    log.error(f'Failed to connect to host at "{self_.connection_string}" on try {tries + 1} / {retries}: {e}')
                    tries += 1

            log.error(f'Failed to connect to host at "{self_.connection_string}" after {retries} tries')

            return None
        return wrapper
    return decorator


class Libvirt(ABC):
    """
    Connect to hosts and provide an interface for interacting with VMs on them.
    """
    def __init__(self,
                 name: str,
                 address: IPv4Address,
                 port: int,
                 protocol: str,
                 hypervisor: str,
                 timeout: int = 30,
                 user: str | None = None,
                 readonly: bool = False,
                 resources: Dict | None = None) -> None:
        """
        Initialize Libvirt with the supplied settings.

        Args:
            name (str): Name of the host.
            address (IPv4Address): IP address of the host to connect to.
            port (int): Port to connect to the host on.
            protocol (str): Type of authentication to use. Defaults to 'ssh'. Can be either 'ssh' or 'tls'.
            hypervisor (str): Type of hypervisor to connect to.
            timeout (int): Timeout for the connection. Defaults to 30 seconds.
            user (str | None): Username to authenticate with (if using SSH).
            readonly (bool): Whether to open the connection in read-only mode. Defaults to False.
            resources (Dict | None): Resources available on the host. Defaults to None.
        """
        self.name = name
        self.address = address
        self._address_str = str(address)
        self.port = port
        self.protocol = protocol
        self.timeout = timeout
        self.hypervisor = hypervisor

        # Set the user to 'root' if not provided by the end user.
        if user is None:
            self.user = 'root'
        else:
            self.user = user

        self.readonly = readonly
        self.resources = resources
        self._connection: lv.virConnect | None = None
        if protocol.lower() == 'ssh':
            # SSH
            self.connection_string = f'{hypervisor}+ssh://{user}@{address}:{port}/system'
        else:
            # TLS
            self.connection_string = f'{hypervisor}+tls://{address}:{port}/system'

    def __enter__(self) -> Libvirt | None:
        """
        Acquire the resources managed by this context.

        Returns:
            Libvirt | None: The initialized resource managed by this context.
        """
        return self.open()

    def __exit__(self, *args: Any) -> None:
        """
        Release resources when the context finishes.

        Args:
            *args (Any): Positional arguments forwarded to the wrapped callable.

        Returns:
            None: No value is returned.
        """
        self.close()

    def open(self) -> Libvirt | None:
        """
        Open a connection to the Libvirt hypervisor.

        Returns:
            Libvirt | None: This connection wrapper on success, or None when libvirt rejects the connection.
        """
        try:
            log.debug(f'Attempting to connect to host at {self.connection_string}')

            if self.readonly:
                self._connection = lv.openReadOnly(self.connection_string)
            else:
                self._connection = lv.open(self.connection_string)
                log.info(f'Connected to host at {self.connection_string}')
        except libvirtError as e:
            log.error(f'Failed to connect to host at {self.connection_string}: {e}')
            return None

        return self

    def close(self) -> None:
        """
        Close the connection with the Libvirt hypervisor.

        Returns:
            None: No value is returned.
        """
        if self._connection:
            self._connection.close()
            log.debug(f'Closed connection to host at {self.connection_string}')
        else:
            log.error(f'No host connection to close, probably due to an error on connection open')

    def is_connected(self) -> bool:
        """
        Check if the connection to the Libvirt hypervisor is open.

        Returns:
            bool: True if the connection is open.
        """
        return self._connection is not None

    @abstractmethod
    def collect_host_stats(self) -> HostStats | None:
        """
        Get a report of schedulable resource utilization on the host.

        Returns:
            HostStats | None: Host resources and domain states, or None when no connection is available.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def request_domain_stats(self, cluster: str, groups: frozenset[str]) -> tuple[RawDomainStats, ...]:
        """
        Request raw counters for verified managed VMs without reducing or publishing them.

        Args:
            cluster (str): Owning cluster identity.
            groups (frozenset[str]): Groups assigned to this host.

        Returns:
            tuple[RawDomainStats, ...]: Acquired samples; an empty tuple is a successful empty inventory.

        Raises:
            NotImplementedError: If the hypervisor does not implement acquisition.
        """
        raise NotImplementedError
