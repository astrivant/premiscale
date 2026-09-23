"""
Exercise the unified controller API through Flask's request lifecycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import os

import pytest
from prometheus_client import CollectorRegistry, CONTENT_TYPE_LATEST, Gauge

from premiscale.api import create_app
from premiscale.status.store import StatusStore

if TYPE_CHECKING:
    from pathlib import Path
    from flask import Flask


@pytest.fixture(name='app')
def api_app(tmp_path: Path) -> Flask:
    """
    Build an isolated API application for each test.

    Args:
        tmp_path (Path): Isolated shared status directory.

    Returns:
        Flask: An isolated API application for each test.
    """
    store = StatusStore(tmp_path, cache_seconds=0)
    store.write('supervisor', {'phase': 'running', 'services': {'api': {'pid': os.getpid(), 'required': True}}})
    store.write('process-api', {'phase': 'ready'})
    return create_app({'TESTING': True}, registry=CollectorRegistry(), status_store=store, liveness=True)


@pytest.mark.parametrize('path', ['/healthz', '/ready'])
def test_health_endpoints_return_json(app: Flask, path: str) -> None:
    """
    Keep probe paths available with valid JSON responses.

    Args:
        app (Flask): Isolated Flask application fixture.
        path (str): Filesystem path to the requested resource.

    Returns:
        None: No value is returned.
    """
    response = app.test_client().get(path)

    assert response.status_code == 200
    assert response.mimetype == 'application/json'
    assert response.get_json()['status'] == 'OK'


@pytest.mark.parametrize('path', ['/metrics', '/metrics/'])
def test_metrics_exposes_current_registry_samples(path: str) -> None:
    """
    Scrape current values through either spelling of the metrics path.

    Args:
        path (str): Filesystem path to the requested resource.

    Returns:
        None: No value is returned.
    """
    registry = CollectorRegistry()
    gauge = Gauge('premiscale_test_nodes', 'Test node count', registry=registry)
    client = create_app({'TESTING': True}, registry=registry).test_client()

    gauge.set(3)
    response = client.get(path)

    assert response.status_code == 200
    assert response.content_type == CONTENT_TYPE_LATEST
    assert '# TYPE premiscale_test_nodes gauge\n' in response.text
    assert 'premiscale_test_nodes 3.0\n' in response.text

    gauge.set(7)
    assert 'premiscale_test_nodes 7.0\n' in client.get(path).text


def test_app_instances_have_independent_configuration_and_metrics() -> None:
    """
    Building a second app must not alter the first app's state.

    Returns:
        None: No value is returned.
    """
    first_registry = CollectorRegistry()
    second_registry = CollectorRegistry()
    Gauge('premiscale_test_nodes', 'Test node count', registry=first_registry).set(1)
    Gauge('premiscale_test_nodes', 'Test node count', registry=second_registry).set(2)
    first = create_app({'TESTING': True}, registry=first_registry)
    second = create_app(registry=second_registry)

    assert first.testing is True
    assert second.testing is False
    assert 'premiscale_test_nodes 1.0\n' in first.test_client().get('/metrics').text
    assert 'premiscale_test_nodes 2.0\n' in second.test_client().get('/metrics').text
    for instance in (first, second):
        assert instance.test_client().get('/healthz').status_code == 404
        assert instance.test_client().get('/ready').status_code == 503


@pytest.mark.parametrize('path', ['/healthz', '/ready', '/metrics'])
def test_cors_is_applied_to_all_endpoint_modules(app: Flask, path: str) -> None:
    """
    Apply the shared CORS policy to every registered blueprint.

    Args:
        app (Flask): Isolated Flask application fixture.
        path (str): Filesystem path to the requested resource.

    Returns:
        None: No value is returned.
    """
    response = app.test_client().get(path, headers={'Origin': 'https://example.com'})

    assert response.headers['Access-Control-Allow-Origin'] == 'https://example.com'


@pytest.mark.parametrize('path', ['/healthz', '/ready', '/metrics'])
def test_endpoints_reject_post_requests(app: Flask, path: str) -> None:
    """
    Keep probe and scrape endpoints read-only.

    Args:
        app (Flask): Isolated Flask application fixture.
        path (str): Filesystem path to the requested resource.

    Returns:
        None: No value is returned.
    """
    assert app.test_client().post(path).status_code == 405


def test_unknown_path_returns_not_found(app: Flask) -> None:
    """
    Leave unregistered paths unavailable.

    Args:
        app (Flask): Isolated Flask application fixture.

    Returns:
        None: No value is returned.
    """
    assert app.test_client().get('/unknown').status_code == 404
