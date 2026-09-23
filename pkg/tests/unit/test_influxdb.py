"""
Check optional InfluxDB publication without coupling collectors to its HTTP format.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
import requests
from ruamel.yaml import YAML

from premiscale.config import parse
from premiscale.config.databases import InfluxDBMetrics
from premiscale.metrics.publishers.influxdb import InfluxDBPublisher
from premiscale.schemas.metrics import Metric, MetricBatch
from premiscale.support.influx import encode_lines
from .test_config_parse import config_file

if TYPE_CHECKING:
    from typing import Any


@pytest.fixture
def influx_batch() -> MetricBatch:
    """
    Combine exact integer counters with identifiers requiring line-protocol escaping.

    Returns:
        MetricBatch: Deterministically timestamped measurement.
    """
    return MetricBatch((Metric('cpu total', datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
                               {'name': 'worker,= 1'}, {'cpu_time_ns': 2 ** 55 + 1, 'usage': 12.5}),))


def test_line_protocol_preserves_exact_values_and_escapes_identifiers(influx_batch: MetricBatch) -> None:
    """
    Keep cumulative integers and timestamp precision without JSON or float conversion.

    Args:
        influx_batch (MetricBatch): Escaped measurement fixture.

    Returns:
        None: No value is returned.
    """
    expected = (Path(__file__).parents[1] / 'data/metrics/influx-line.txt').read_bytes().rstrip(b'\n')
    assert encode_lines(influx_batch) == expected


def test_http_errors_remain_retryable_and_successful_retries_have_identical_points(
        influx_batch: MetricBatch, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Raise before acknowledgement and reuse the same point identity after a rejected write.

    Args:
        influx_batch (MetricBatch): Escaped measurement fixture.
        monkeypatch (pytest.MonkeyPatch): HTTP transport replacement.

    Returns:
        None: No value is returned.
    """
    responses = []
    for code in (503, 204, 204):
        response = requests.Response()
        response.status_code = code
        response._content = b''
        _ = response.content
        responses.append(response)
    session = Mock(post=Mock(side_effect=responses))
    factory = Mock(return_value=session)
    monkeypatch.setattr('premiscale.metrics.publishers.influxdb.requests.Session', factory)
    settings = InfluxDBMetrics(url='https://influx.example/prefix', organization='team', bucket='metrics', token='secret', timeoutSeconds=3)
    publisher = InfluxDBPublisher(settings)
    factory.assert_not_called()
    with pytest.raises(requests.HTTPError):
        publisher.publish(influx_batch, 'delivery')
    publisher.close()
    publisher.publish(influx_batch, 'delivery')
    publisher.publish(influx_batch, 'delivery')
    assert all(call.kwargs['data'] == encode_lines(influx_batch) for call in session.post.call_args_list)
    request = session.post.call_args
    assert request.args == ('https://influx.example/prefix/api/v2/write',)
    assert request.kwargs['params'] == {'org': 'team', 'bucket': 'metrics', 'precision': 'ns'}
    assert request.kwargs['headers']['Authorization'] == 'Token secret'
    assert request.kwargs['timeout'] == (3, 3)
    assert request.kwargs['verify'] is True and request.kwargs['allow_redirects'] is False
    assert 'secret' not in repr(settings)
    publisher.close()


@pytest.mark.parametrize('value', [True, float('nan'), 2 ** 63])
def test_invalid_numeric_fields_cannot_reach_influxdb(value: Any) -> None:
    """
    Reject values that would corrupt counters or fail InfluxDB integer bounds.

    Args:
        value (Any): Unsupported numerical value.

    Returns:
        None: No value is returned.
    """
    with pytest.raises(ValueError):
        encode_lines(MetricBatch((Metric('cpu', datetime.now(timezone.utc), {}, {'cpu': value}),)))


def test_helm_schema_and_attrs_select_the_influxdb_publisher(config_file: tuple[Path, dict[str, Any]],
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Parse Secret-backed settings without connecting or placing tokens in representations.

    Args:
        config_file (tuple[Path, dict[str, Any]]): Valid runtime configuration.
        monkeypatch (pytest.MonkeyPatch): Secret environment fixture.

    Returns:
        None: No value is returned.
    """
    path, data = config_file
    monkeypatch.setenv('PREMISCALE_INFLUXDB_TOKEN', 'private-token')
    data['controller']['databases']['publishers'] = {'grafana': {
        'type': 'influxdb', 'url': 'https://influx.example', 'organization': 'team',
        'bucket': 'premiscale', 'token': '$PREMISCALE_INFLUXDB_TOKEN',
    }}
    YAML(typ='safe').dump(data, path)
    settings = parse.configParse(str(path)).controller.databases.publishers['grafana']
    assert isinstance(settings, InfluxDBMetrics)
    assert settings.token == 'private-token'
    assert 'private-token' not in repr(settings)
    assert settings.publisher().session is None


def test_influxdb_can_use_a_separate_keda_publisher_deployment(tmp_path: Path) -> None:
    """
    Render an InfluxDB subscriber through the same HHA publication and scaling path.

    Args:
        tmp_path (Path): Temporary chart override directory.

    Returns:
        None: No value is returned.
    """
    from .test_broker_chart import ROOT, render

    values = YAML(typ='safe').load(ROOT / '.config/keda/values.yaml')
    overlay = YAML(typ='safe').load(ROOT / '.config/influxdb/values.yaml')
    values['config']['controller']['databases']['publishers'].update(overlay['config']['controller']['databases']['publishers'])
    values['controller']['extraEnv'].extend(overlay['controller']['extraEnv'])
    values['autoscaling']['publishers'].update(overlay['autoscaling']['publishers'])
    rendered = render(tmp_path, values)
    publisher = next(item for item in rendered if item['kind'] == 'Deployment'
                     and item['metadata']['name'].endswith('-publisher-grafana'))
    scaled = next(item for item in rendered if item['kind'] == 'ScaledObject'
                  and item['spec']['scaleTargetRef']['name'] == publisher['metadata']['name'])
    assert scaled['spec']['minReplicaCount'] == 2
    assert publisher['metadata']['annotations']['configmap.reloader.stakater.com/reload'] == 'premiscale'
