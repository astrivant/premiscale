"""
Build the controller API from endpoint-specific blueprints.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flask import Flask
from flask_cors import CORS
from prometheus_client import REGISTRY

from premiscale.api import healthcheck, metrics, scaling

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any
    from prometheus_client import CollectorRegistry
    from premiscale.status.store import StatusStore
    from premiscale.reconciliation.demand import Demand


__all__ = ['create_app']


def create_app(
    config: Mapping[str, Any] | None = None,
    *,
    registry: CollectorRegistry = REGISTRY,
    status_store: StatusStore | None = None,
    liveness: bool = False,
    demand: Demand | None = None,
) -> Flask:
    """
    Build a Flask app and register each top-level API path.

    Args:
        config (Mapping[str, Any] | None): Optional Flask configuration overrides.
        registry (CollectorRegistry): Prometheus registry exposed by the metrics endpoint.
        status_store (StatusStore | None): Shared runtime observations for readiness checks.
        liveness (bool): Include a liveness route for standalone modes without Kopf.
        demand (Demand | None): Optional aggregate scaling observations for worker deployments.

    Returns:
        Flask: The configured controller API application.
    """
    app = Flask(__name__, static_folder=None)
    if config is not None:
        app.config.from_mapping(config)

    CORS(app)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    app.register_blueprint(healthcheck.create_blueprint(status_store, liveness))
    app.register_blueprint(metrics.create_blueprint(registry), url_prefix='/metrics')
    if demand is not None:
        app.register_blueprint(scaling.create_blueprint(demand), url_prefix='/scaling')
    return app
