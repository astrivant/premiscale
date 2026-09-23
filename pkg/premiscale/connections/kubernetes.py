"""
Share bounded, authenticated Kubernetes API access between operator services.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from attrs import define, field
import requests

if TYPE_CHECKING:
    from typing import Any


@define
class KubernetesClient:
    """
    Read projected service-account credentials for every API request.

    Attributes:
        session (requests.Session): Process-local HTTP session without ambient proxy credentials.
        credentials (Path): Directory containing the projected token and server CA.
        base (str): In-cluster API origin determined from Kubernetes service settings.
    """

    session: requests.Session = field(factory=requests.Session, repr=False)
    credentials: Path = field(factory=lambda: Path(os.getenv(
        'PREMISCALE_SERVICE_ACCOUNT_DIRECTORY', '/var/run/secrets/kubernetes.io/serviceaccount')))
    base: str = field(init=False)

    def __attrs_post_init__(self) -> None:
        """
        Resolve the API server address without opening a network connection.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If Kubernetes service discovery is unavailable.
        """
        host = os.getenv('KUBERNETES_SERVICE_HOST', '')
        if not host:
            raise ValueError('Kubernetes API service settings are unavailable')
        host = f'[{host}]' if ':' in host else host
        self.base = f'https://{host}:{os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")}'
        self.session.trust_env = False

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> requests.Response:
        """
        Authenticate one request using current credentials and bounded transport waits.

        Args:
            method (str): HTTP method.
            path (str): Absolute API path within the configured cluster.
            body (dict[str, Any] | None): Optional JSON request body.

        Returns:
            requests.Response: Response whose status the caller must check.
        """
        return self.session.request(method, self.base + path, json=body, timeout=(2, 2),
                                    verify=str(self.credentials / 'ca.crt'),
                                    headers={'Authorization': f'Bearer {(self.credentials / "token").read_text().strip()}',
                                             'Content-Type': 'application/merge-patch+json' if method == 'PATCH' else 'application/json'})

    def close(self) -> None:
        """
        Release this process's HTTP connection pool.

        Returns:
            None: No value is returned.
        """
        self.session.close()
