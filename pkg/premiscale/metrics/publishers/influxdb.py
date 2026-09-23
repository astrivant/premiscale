"""
Publish reduced metrics through the InfluxDB 2.x HTTP write API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import requests

from premiscale.support.influx import encode_lines
from ._base import Publisher

if TYPE_CHECKING:
    from premiscale.config.databases import InfluxDBMetrics
    from premiscale.schemas.metrics import MetricBatch


class InfluxDBPublisher(Publisher):
    """
    Own one HTTP session and acknowledge only synchronous accepted writes.
    """

    def __init__(self, config: InfluxDBMetrics) -> None:
        """
        Retain settings without opening a session in the constructing process.

        Args:
            config (InfluxDBMetrics): Resolved URL, bucket, organization, and Secret-backed token.
        """
        self.config = config
        self.session: requests.Session | None = None

    def publish(self, batch: MetricBatch, message_id: str) -> None:
        """
        Write original measurement identities and timestamps for idempotent retries.

        Args:
            batch (MetricBatch): Reduced data independent of InfluxDB formatting.
            message_id (str): Transport identifier; point identity makes retries idempotent.

        Returns:
            None: No value is returned after the server accepts the synchronous write.

        Raises:
            requests.HTTPError: If the destination rejects the write.
        """
        if not batch.metrics:
            return
        body = encode_lines(batch)
        if self.session is None:
            self.session = requests.Session()
            self.session.trust_env = False
        with self.session.post(self.config.url.rstrip('/') + '/api/v2/write',
                               params={'org': self.config.organization, 'bucket': self.config.bucket, 'precision': 'ns'},
                               headers={'Authorization': f'Token {self.config.token}', 'Content-Type': 'text/plain; charset=utf-8'},
                               data=body, timeout=(self.config.timeoutSeconds, self.config.timeoutSeconds),
                               verify=self.config.caFile or True, allow_redirects=False) as response:
            if response.status_code != 204:
                raise requests.HTTPError('InfluxDB did not accept the complete write', response=response)

    def close(self) -> None:
        """
        Release the HTTP session after a failure or process shutdown.

        Returns:
            None: No value is returned.
        """
        if self.session is not None:
            self.session.close()
            self.session = None
