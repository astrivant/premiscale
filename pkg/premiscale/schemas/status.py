"""
Typed observations exchanged between the provider and Kubernetes status publisher.
"""

from attrs import frozen


@frozen
class GroupObservation:
    """
    Describe provider lifecycle counts without asserting Kubernetes Node readiness.

    Attributes:
        configurationDigest (str): Digest of the configuration actually loaded by the provider.
        targetReplicas (int): Accepted instances excluding requested deletions.
        runningReplicas (int): VMs whose provider operations completed successfully.
        pendingReplicas (int): Queued or in-progress VM creations.
        deletingReplicas (int): Requested deletions, including failed deletion attempts.
        failedReplicas (int): Instances whose most recent lifecycle operation failed.
    """

    configurationDigest: str
    targetReplicas: int
    runningReplicas: int
    pendingReplicas: int
    deletingReplicas: int
    failedReplicas: int
