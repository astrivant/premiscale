"""
Expose Prometheus metrics through the controller's Flask application.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from flask import Blueprint, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


def create_blueprint(registry: CollectorRegistry) -> Blueprint:
    """
    Build routes for the supplied Prometheus registry.

    Args:
        registry (CollectorRegistry): Registry to collect when handling a scrape request.

    Returns:
        Blueprint: Prometheus metrics endpoints.
    """
    blueprint = Blueprint('metrics', __name__)

    @blueprint.get('', strict_slashes=False)
    def scrape() -> Response:
        """
        Return current samples in Prometheus text format.

        Returns:
            Response: Current samples in Prometheus text format.
        """
        return Response(generate_latest(registry), content_type=CONTENT_TYPE_LATEST)

    return blueprint
