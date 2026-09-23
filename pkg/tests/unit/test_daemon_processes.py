"""
Check mode selection and worker setup without live infrastructure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import asyncio
from contextlib import contextmanager, suppress
from queue import Empty
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from premiscale.autoscaling.group import Autoscaler
from premiscale.config.databases import Databases, SQLiteState, LocalMetrics
from premiscale.daemon.processes import api, build, platform
from premiscale.platform import Platform

if TYPE_CHECKING:
    from typing import Any, Iterator


@pytest.mark.parametrize('mode, expected, timeseries', [
    ('standalone', ['api', 'platform', 'autoscaling', 'reconciliation'], True),
    ('standalone-external-metrics', ['api', 'platform', 'autoscaling', 'reconciliation'], None),
    ('kubernetes', ['api', 'platform', 'autoscaling', 'reconciliation', 'kubernetes', 'operator'], True),
    ('kubernetes-external-metrics', ['api', 'platform', 'autoscaling', 'kubernetes', 'operator'], None),
])
def test_modes_select_their_services_without_instantiating_clients(mode: str, expected: list[str], timeseries: bool | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify modes select their services without instantiating clients.

    Args:
        mode (str): Controller mode under test.
        expected (list[str]): Expected services selected for the controller mode.
        timeseries (bool | None): Expected time-series collection setting.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    register = Mock(side_effect=AssertionError('Registration must happen in the child'))
    monkeypatch.setattr(Platform, 'register', register)
    app = Mock(side_effect=AssertionError('The API must be constructed in the child'))
    monkeypatch.setattr(api, 'create_app', app)
    config: Any = SimpleNamespace(controller=SimpleNamespace(mode=mode, databases=Databases(5, 30, 2, SQLiteState(), LocalMetrics())))
    specs = build(config, 'version', 'token')
    assert [spec.name for spec in specs] == expected
    assert all(spec.required == (spec.name != 'platform') for spec in specs)
    assert specs[0].target is api.run
    assert specs[0].args == (config,)
    if timeseries is not None:
        assert next(spec for spec in specs if spec.name == 'reconciliation').args == (config,)
    register.assert_not_called()
    app.assert_not_called()


def test_unknown_mode_is_rejected() -> None:
    """
    Verify unknown mode is rejected.

    Returns:
        None: No value is returned.
    """
    config: Any = SimpleNamespace(controller=SimpleNamespace(mode='unknown'))
    with pytest.raises(ValueError, match='Unknown controller mode'):
        build(config, 'version', 'token')


def test_platform_registration_and_client_are_created_in_entry_point(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify platform registration and client are created in entry point.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    settings = SimpleNamespace(domain='example.test', certificates=SimpleNamespace(path='ca.pem'))
    config: Any = SimpleNamespace(controller=SimpleNamespace(platform=settings, broker=Mock()))
    register = Mock(return_value=None)
    monkeypatch.setattr(Platform, 'register', register)
    messages = Mock()
    broker = Mock()
    monkeypatch.setattr(platform, 'platform_queue', broker)
    platform.run(config, 'version', 'token')
    broker.assert_not_called()
    register.assert_called_once_with(version='version', token='token', host='example.test', cacert='ca.pem')
    client = Mock()
    register.return_value = client
    @contextmanager
    def connect(_config: Any) -> Iterator[Any]:
        """
        Yield the fake platform message queue.

        Args:
            _config (Any): Config.

        Yields:
            Any: Resource available for the duration of the context.
        """
        yield messages

    monkeypatch.setattr(platform, 'platform_queue', connect)
    with pytest.raises(RuntimeError, match='exited unexpectedly'):
        platform.run(config, 'version', 'token')
    client.assert_called_once_with(messages)


def test_autoscaler_acknowledges_only_completed_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify autoscaler acknowledges only completed actions.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setattr('premiscale.autoscaling.group.setproctitle', lambda _title: None)
    action = Mock()
    acknowledged: list[Mock] = []

    @contextmanager
    def delivery() -> Iterator[SimpleNamespace]:
        """
        Yield a queued test payload and record successful acknowledgement.

        Yields:
            SimpleNamespace: Resource available for the duration of the context.

        Raises:
            SystemExit: If the requested operation cannot be completed.
        """
        if acknowledged:
            raise SystemExit(0)
        yield SimpleNamespace(payload=action)
        acknowledged.append(action)

    actions = Mock(delivery=delivery)
    with pytest.raises(SystemExit):
        Autoscaler(Mock())(actions)
    action.execute.assert_called_once_with()
    assert acknowledged == [action]
    acknowledged.clear()
    action.execute.side_effect = RuntimeError('action failed')
    with pytest.raises(RuntimeError, match='action failed'):
        Autoscaler(Mock())(actions)
    assert not acknowledged


@pytest.mark.parametrize('fail', [False, True])
def test_platform_acknowledges_only_successful_sends(fail: bool) -> None:
    """
    Verify platform acknowledges only successful sends.

    Args:
        fail (bool): Whether to simulate an operation failure.

    Returns:
        None: No value is returned.
    """
    async def exercise() -> None:
        """
        Exercise the asynchronous platform sender and its acknowledgements.

        Returns:
            None: No value is returned.
        """
        client = Platform({}, 'test')
        pending = ['first', 'second']

        @contextmanager
        def delivery(*, block: Any) -> Iterator[SimpleNamespace]:
            """
            Yield a queued test payload and record successful acknowledgement.

            Args:
                block (Any): Whether to wait for a newly published message.

            Yields:
                SimpleNamespace: Resource available for the duration of the context.

            Raises:
                Empty: If the requested operation cannot be completed.
            """
            assert not block
            if not pending:
                raise Empty
            yield SimpleNamespace(payload=pending[0])
            pending.pop(0)

        client._queue = Mock(delivery=delivery)
        send = AsyncMock(side_effect=RuntimeError('send failed') if fail else None)
        client._websocket = Mock(send=send)
        task = asyncio.create_task(client._sync_platform_queue())
        try:
            await asyncio.sleep(0)
            if fail:
                with pytest.raises(RuntimeError, match='send failed'):
                    await task
                assert pending == ['first', 'second']
            else:
                assert [call.args for call in send.await_args_list] == [('first',), ('second',)]
                assert not pending
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError, RuntimeError):
                await task

    asyncio.run(exercise())
