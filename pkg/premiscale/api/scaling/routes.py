"""
Report scale signals without exposing host identities or connection credentials.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Blueprint
from confluent_kafka import KafkaException
from redis.exceptions import RedisError


if TYPE_CHECKING:
    from typing import Any
    from premiscale.reconciliation.demand import Demand


def create_blueprint(demand: Demand) -> Blueprint:
    """
    Build scaling routes that fail closed when the broker cannot be queried.

    Args:
        demand (Demand): Aggregate observations for explicitly delegated workloads.

    Returns:
        Blueprint: Scaling endpoints beneath the application's scaling prefix.
    """
    blueprint = Blueprint('scaling', __name__)

    @blueprint.get('/<path:workload>')
    def snapshot(workload: str) -> tuple[dict[str, Any], int]:
        """
        Return numeric scaling signals or an explicit unavailable response.

        Args:
            workload (str): Collection or named publisher workload path.

        Returns:
            tuple[dict[str, Any], int]: JSON payload and HTTP status.
        """
        try:
            return demand.snapshot(workload), 200
        except KeyError:
            return {'error': 'Unknown workload'}, 404
        except (RedisError, KafkaException):
            return {'error': 'Scaling metrics unavailable'}, 503

    return blueprint
