"""
Serve controller health and readiness checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from flask import Blueprint, jsonify
from premiscale.status.health import report

if TYPE_CHECKING:
    from flask import Response
    from premiscale.status.store import StatusStore


def create_blueprint(store: StatusStore | None = None, liveness: bool = False) -> Blueprint:
    """
    Build readiness routes and the standalone-mode liveness fallback.

    Args:
        store (StatusStore | None): Cached reader for shared process observations.
        liveness (bool): Expose liveness here when no Kubernetes operator is running.

    Returns:
        Blueprint: Health and readiness endpoints.
    """
    blueprint = Blueprint('healthcheck', __name__)

    def healthcheck() -> Response:
        """
        Report that the controller API is running.

        Returns:
            Response: JSON response indicating that the HTTP API is running.
        """
        health = report(store, require_ready=False)
        response = jsonify(health)
        response.status_code = 200 if health['status'] == 'OK' else 503
        return response

    if liveness:
        blueprint.add_url_rule('/healthz', view_func=healthcheck, methods=['GET'])

    @blueprint.get('/ready')
    def ready() -> Response:
        """
        Report completed initialization and fresh shared provider observations.

        Returns:
            Response: JSON response reporting the current readiness contract.
        """
        health = report(store)
        response = jsonify(health)
        response.status_code = 200 if health['status'] == 'OK' else 503
        return response

    return blueprint
