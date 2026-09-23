"""
Define the lifecycle and idempotent write contract for metric publishers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from abc import ABC, abstractmethod


if TYPE_CHECKING:
    from premiscale.schemas.metrics import MetricBatch


class Publisher(ABC):
    """
    Own one destination connection and translate metrics only at its write boundary.
    """

    @abstractmethod
    def publish(self, batch: MetricBatch, message_id: str) -> None:
        """
        Persist a batch, safely accepting repeated delivery of the same message.

        Args:
            batch (MetricBatch): Reduced measurements to write.
            message_id (str): Stable transport UUID used to deduplicate retries.

        Returns:
            None: No value is returned; failures must raise before acknowledgement.

        Raises:
            NotImplementedError: If the adapter does not implement publication.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Release the destination connection after failure or shutdown.

        Returns:
            None: No value is returned.

        Raises:
            NotImplementedError: If the adapter does not implement cleanup.
        """
        raise NotImplementedError
