"""
Process queues of Actions for all autoscaling groups. This is the main entry point for the autoscaling subprocess.
"""

from __future__ import annotations

import logging

from typing import TYPE_CHECKING
from queue import Empty
from setproctitle import setproctitle
from premiscale.messaging import InvalidMessage


if TYPE_CHECKING:
    from premiscale.autoscaling.actions import Action
    from premiscale.messaging import RedisQueue
    from premiscale.config.v1alpha1 import Config


log = logging.getLogger(__name__)


class Autoscaler:
    """
    Handle actions.

    E.g., if a new VM needs to be created or deleted on some host, handle that action, and all relevant side-effects (e.g. updating MySQL state).

    One of these classes gets instantiated for every autoscaling group defined in
    the config.
    """
    def __init__(self, config: Config) -> None:
        """
        Initialize Autoscaler with the supplied settings.

        Args:
            config (Config): Parsed controller configuration.
        """
        self.config = config
        self.queue: RedisQueue[Action]

    def __call__(self, asg_queue: RedisQueue[Action]) -> None:
        """
        Run the Autoscaler service until shutdown.

        Args:
            asg_queue (RedisQueue[Action]): Queue that receives or supplies infrastructure actions.

        Returns:
            None: No value is returned.
        """
        setproctitle('autoscaling')
        self.queue = asg_queue
        log.debug('Starting autoscaling subprocess')
        self._autoscale()

    def _autoscale(self) -> None:
        """
        Continuously process actions from the queue.

        Returns:
            None: No value is returned.
        """
        while True:
            try:
                with self.queue.delivery() as message:
                    self._handle_action(message.payload)
            except Empty:
                continue
            except InvalidMessage:
                log.exception('Invalid action moved to the dead-letter stream')

    def _handle_action(self, action: Action) -> None:
        """
        Handle an action through to completion.

        Args:
            action (Action): The action to handle.

        Returns:
            None: No value is returned.
        """
        log.debug(f'Handling action: {action}')
        action.execute()
        log.debug(f'Finished handling action: {action}')
