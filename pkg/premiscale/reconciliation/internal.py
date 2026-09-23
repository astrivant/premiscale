"""
Reconcile metrics and state databases and place Actions on the autoscaling queue for the Autoscaling subprocess.
"""


from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from premiscale.autoscaling.actions import (
    Verb,
    Null,
    Create,
    Migrate,
    Clone,
    Replace,
    Delete
)

if TYPE_CHECKING:
    from premiscale.metrics.state._base import State
    from premiscale.metrics.timeseries._base import TimeSeries
    from premiscale.config.v1alpha1 import Config
    from premiscale.messaging import RedisQueue
    from premiscale.autoscaling.actions import Action


log = logging.getLogger(__name__)


class Reconcile:
    """
    Internal reconciliation queries metrics and state databases and places Actions on the autoscaling queue for the Autoscaling subprocess to handle.
    """
    def __init__(self, config: Config, asg_queue: RedisQueue[Action], platform_queue: RedisQueue[str]) -> None:
        """
        Prepare the existing standalone decision engine without starting a loop.

        Args:
            config (Config): The parsed, user-provided configuration.
            asg_queue (RedisQueue[Action]): Queue receiving infrastructure actions.
            platform_queue (RedisQueue[str]): Queue carrying platform messages.
        """
        self._config = config
        self.asg_queue = asg_queue
        self.platform_queue = platform_queue
        self.state_database = config.controller.databases.state.adapter()
        self.timeseries_database = config.controller.databases.timeseries.adapter()

    def reconcile_once(self) -> None:
        """
        Read one metrics snapshot for the standalone decision engine.

        Scheduling and collection supervision belong to the reconciliation runtime.
        Standalone action planning remains unimplemented.

        Returns:
            None: No value is returned after reading the current snapshot.
        """
        with self.timeseries_database as timeseries, self.state_database:
            timeseries.get_all()

    # Actions to place on the autoscaling queue.

    def _create(self) -> None:
        """
        Add a Create-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """

    def _delete(self) -> None:
        """
        Add a Delete-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """

    def _null(self) -> None:
        """
        Add a Null-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """

    def _migrate(self) -> None:
        """
        Add a Migrate-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """

    def _clone(self) -> None:
        """
        Add a Clone-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """

    def _replace(self) -> None:
        """
        Add a Replace-event to an autoscaling queue.

        Returns:
            None: No value is returned.
        """
