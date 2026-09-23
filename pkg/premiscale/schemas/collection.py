"""
Describe a completed host collection pass for throughput feedback.
"""

from attrs import frozen


@frozen
class CollectionReport:
    """
    Record attempts, successful collections, and elapsed wall time.

    Attributes:
        attempted (int): Number of hosts visited during this pass.
        succeeded (int): Hosts collected and enqueued successfully, including empty hosts.
        elapsed (float): Wall-clock duration of the pass in seconds.
    """

    attempted: int
    succeeded: int
    elapsed: float

    @property
    def throughput(self) -> float:
        """
        Measure successful host collections per second of active collection.

        Returns:
            float: Successful hosts per second, or zero for an empty pass.
        """
        return self.succeeded / max(self.elapsed, 1e-9)
