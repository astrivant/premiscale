"""
Coordinate whole-controller leadership through Kubernetes Leases and bounded renewals.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from time import monotonic
from typing import TYPE_CHECKING
from urllib.parse import quote
from uuid import uuid4

from premiscale.connections.kubernetes import KubernetesClient

if TYPE_CHECKING:
    from typing import Any
    import requests


class Lease:
    """
    Use optimistic resource versions and local observation time to elect one candidate.

    Attributes:
        duration (int): Seconds a candidate observes an unchanged foreign Lease before takeover.
        renew_deadline (int): Local renewal deadline before leader services must stop.
        retry_period (int): Seconds between election attempts.
    """

    duration: int = 45
    renew_deadline: int = 10
    retry_period: int = 2

    def __init__(self, namespace: str, name: str) -> None:
        """
        Configure an in-cluster client without reusing a prior process identity.

        Args:
            namespace (str): Namespace containing the controller pods and Lease.
            name (str): Release-scoped Lease name.

        Raises:
            ValueError: If the pod identity or Kubernetes API address is unavailable.
        """
        self.namespace = namespace
        self.name = name
        self.pod = os.getenv('CONTROLLER_POD_NAME', '')
        if not self.pod:
            raise ValueError('Leader election requires pod identity')
        self.client = KubernetesClient()
        self.path = f'/apis/coordination.k8s.io/v1/namespaces/{quote(namespace, safe="")}/leases'
        self.identity = f'{self.pod}:{uuid4()}'
        self.holding = False
        self.confirmed = 0.0
        self.observed: Any = None
        self.observed_at = monotonic()

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> requests.Response:
        """
        Read projected credentials on every request and verify the API server certificate.

        Args:
            method (str): HTTP method.
            path (str): Namespace-scoped Kubernetes API path.
            body (dict[str, Any] | None): Optional structured request body.

        Returns:
            requests.Response: Bounded HTTP response for the caller to validate.
        """
        return self.client.request(method, path, body)

    @property
    def valid(self) -> bool:
        """
        Apply a conservative local renewal deadline before the remote Lease can expire.

        Returns:
            bool: Whether this candidate may still run leader services.
        """
        return self.holding and monotonic() - self.confirmed < self.renew_deadline

    def step(self) -> bool:
        """
        Acquire or renew through a compare-and-swap without trusting remote wall clocks.

        Returns:
            bool: True after a confirmed acquisition or renewal.

        Raises:
            RuntimeError: If an existing leader has already missed its local deadline.
        """
        started = monotonic()
        if self.holding and not self.valid:
            raise RuntimeError('Leadership renewal deadline expired')
        response = self.request('GET', f'{self.path}/{quote(self.name, safe="")}')
        creating = response.status_code == 404
        resource: dict[str, Any]
        if creating:
            resource = {'apiVersion': 'coordination.k8s.io/v1', 'kind': 'Lease',
                        'metadata': {'name': self.name, 'namespace': self.namespace}, 'spec': {}}
        else:
            response.raise_for_status()
            resource = response.json()
        spec = resource.get('spec', {})
        holder = spec.get('holderIdentity', '')
        record = (holder, spec.get('renewTime'), spec.get('leaseTransitions'), spec.get('leaseDurationSeconds'))
        if record != self.observed:
            self.observed, self.observed_at = record, monotonic()
        if holder and holder != self.identity and monotonic() - self.observed_at < spec.get('leaseDurationSeconds', self.duration):
            self.holding = False
            return False
        now = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        replacing = holder != self.identity
        resource['spec'] = {**spec, 'holderIdentity': self.identity, 'leaseDurationSeconds': self.duration,
                            'renewTime': now, 'acquireTime': now if replacing else spec.get('acquireTime', now),
                            'leaseTransitions': int(spec.get('leaseTransitions', 0)) + int(replacing)}
        response = self.request('POST' if creating else 'PUT', self.path if creating else f'{self.path}/{quote(self.name, safe="")}', resource)
        if response.status_code == 409:
            # An unconfirmed renewal may be retried only while the old deadline remains valid.
            return False
        response.raise_for_status()
        self.holding, self.confirmed = True, started
        return self.valid

    def advertise(self, enabled: bool) -> None:
        """
        Route provider traffic to this pod only after its leader services are ready.

        Args:
            enabled (bool): Add the release's leader label, or clear a stale one.

        Returns:
            None: No value is returned.
        """
        response = self.request('PATCH', f'/api/v1/namespaces/{quote(self.namespace, safe="")}/pods/{quote(self.pod, safe="")}',
                                {'metadata': {'labels': {'premiscale.com/leader': self.name if enabled else None}}})
        response.raise_for_status()

    def release(self) -> None:
        """
        Clear only this process's Lease after all of its leader services stop.

        Returns:
            None: No value is returned.
        """
        if not self.holding:
            return
        response = self.request('GET', f'{self.path}/{quote(self.name, safe="")}')
        response.raise_for_status()
        resource = response.json()
        if resource.get('spec', {}).get('holderIdentity') == self.identity:
            resource['spec']['holderIdentity'] = ''
            response = self.request('PUT', f'{self.path}/{quote(self.name, safe="")}', resource)
            if response.status_code != 409:
                response.raise_for_status()
        self.holding = False

    def close(self) -> None:
        """
        Close the HTTP session without implicitly relinquishing a live leader's Lease.

        Returns:
            None: No value is returned.
        """
        self.client.close()
