"""
Describe observed VM state separately from metrics and requested lifecycle actions.
"""

from datetime import datetime

from attrs import frozen

from .qemu import ManagedDomain


@frozen
class DomainObservation:
    """
    Carry the latest observed state and capacity needed by reconciliation.

    Missing counters remain unknown rather than being recorded as zero. These
    observations never authorize changes to the provider's requested VM lifecycle.

    Attributes:
        domain (ManagedDomain): Verified VM ownership and location.
        time (datetime): Original acquisition timestamp, used to reject stale delivery.
        state (int | None): Libvirt lifecycle state, including stopped domains.
        reason (int | None): Libvirt reason for that state.
        vcpus (int | None): Present virtual CPU count.
        memory_bytes (int | None): Configured maximum guest memory in bytes.
        storage_bytes (int | None): Total capacity of all known top-level block devices.
    """

    domain: ManagedDomain
    time: datetime
    state: int | None = None
    reason: int | None = None
    vcpus: int | None = None
    memory_bytes: int | None = None
    storage_bytes: int | None = None
