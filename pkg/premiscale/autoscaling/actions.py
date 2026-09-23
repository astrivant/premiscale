"""
Define actions the agent can take against infrastructure.
"""


from __future__ import annotations

# attrs needs ClassVar at runtime to exclude constants from instance fields.
from typing import ClassVar, TYPE_CHECKING
from attrs import define
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from premiscale.hypervisor._base import Libvirt
    from typing import Any


@define
class Verb:
    """
    Classify different C[R]UD operations the controller can take on infrastructure, e.g.

    creating, deleting, or migrating VMs.

    Attributes:
        NULL (ClassVar[int]): No-op action code.
        CREATE (ClassVar[int]): VM creation action code.
        CLONE (ClassVar[int]): VM cloning action code.
        MIGRATE (ClassVar[int]): VM migration action code.
        REPLACE (ClassVar[int]): VM replacement action code.
        DELETE (ClassVar[int]): VM deletion action code.
    """
    # Do nothing.
    NULL: ClassVar[int] = 0

    # Creates
    CREATE: ClassVar[int] = 1
    CLONE: ClassVar[int] = 3

    # Updates
    MIGRATE: ClassVar[int] = 2
    REPLACE: ClassVar[int] = 4

    # Deletes
    DELETE: ClassVar[int] = 10


class Action(ABC):
    """
    Encapsulate the various actions that the autoscaler can take.

    These get queued up and acted upon in the autoscaling subprocess as threads. One action is processed at a time in each thread, and each thread corresponds to an ASG.
    """
    def __init__(self, action: int) -> None:
        """
        Initialize Action with the supplied settings.

        Args:
            action (int): Infrastructure action to represent or encode.
        """
        self.action = action

        # The normalized number of virtual machines to act on.
        self.modifier = 0

    @abstractmethod
    def audit_trail_msg(self) -> dict:
        """
        Return a dictionary (JSON) object containing audit data about the action taken.

        Returns:
            dict: generates an audit trail message based on what occurred with the Action's execution.

        Raises:
            NotImplementedError: if the method is not implemented.
        """
        raise NotImplementedError

    def kind(self) -> int:
        """
        Return the type of action.

        Returns:
            int: the type of action.
        """
        return self.action

    def __enter__(self) -> Action:
        """
        Acquire the resources managed by this context.

        Returns:
            Action: The initialized resource managed by this context.
        """
        return self

    def __exit__(self, *args: Any) -> None:
        """
        Release resources when the context finishes.

        Args:
            *args (Any): Positional arguments forwarded to the wrapped callable.

        Returns:
            None: No value is returned.
        """
        pass

    @abstractmethod
    def execute(self) -> None:
        """
        Execute the action.

        Returns:
            None: No value is returned.

        Raises:
            NotImplementedError: if the method is not implemented.
        """
        raise NotImplementedError


class Null(Action):
    """
    Action encapsulating logic to do nothing on a VM on a particular host.
    """
    def __init__(self) -> None:
        """
        Initialize Null with the supplied settings.
        """
        self.modifier = 0
        super().__init__(action=Verb.NULL)

    def audit_trail_msg(self) -> dict:
        """
        Describe the action for the platform audit trail.

        Returns:
            dict: The action for the platform audit trail.
        """
        return {
            'action': 'null',
            'modifier': self.modifier
        }

    def execute(self) -> None:
        """
        Complete a no-op action without changing infrastructure.

        Returns:
            None: No value is returned.
        """


class Create(Action):
    """
    Action encapsulating logic to create a VM on a particular host.
    """
    def __init__(self) -> None:
        """
        Initialize Create with the supplied settings.
        """
        super().__init__(action=Verb.CREATE)

    def audit_trail_msg(self) -> dict:
        """
        Return a dictionary (JSON) object containing audit data about the action taken.

        Returns:
            dict: an audit trail message based on what occurred with the Action's execution.
        """
        return {
            'action': self.action,
            'modifier': self.modifier
        }


class Migrate(Action):
    """
    Action encapsulating logic to migrate a VM from one host to another.
    """
    def __init__(self, vm_name: str, host: str) -> None:
        """
        Initialize Migrate with the supplied settings.

        Args:
            vm_name (str): Name of the virtual machine.
            host (str): Configured hypervisor host name.
        """
        super().__init__(action=Verb.MIGRATE)


class Clone(Action):
    """
    Action encapsulating logic to clone a VM on a particular host.
    """
    def __init__(self, vm_name: str, host: str) -> None:
        """
        Initialize Clone with the supplied settings.

        Args:
            vm_name (str): Name of the virtual machine.
            host (str): Configured hypervisor host name.
        """
        super().__init__(action=Verb.CLONE)


class Replace(Action):
    """
    Action encapsulating logic to replace a VM on a particular host.
    """
    def __init__(self, vm_name: str, host: str) -> None:
        """
        Initialize Replace with the supplied settings.

        Args:
            vm_name (str): Name of the virtual machine.
            host (str): Configured hypervisor host name.
        """
        super().__init__(action=Verb.REPLACE)


class Delete(Action):
    """
    Action encapsulating logic to delete a VM on a particular host.
    """
    def __init__(self, vm_name: str, host: str) -> None:
        """
        Initialize Delete with the supplied settings.

        Args:
            vm_name (str): Name of the virtual machine.
            host (str): Configured hypervisor host name.
        """
        super().__init__(action=Verb.DELETE)
