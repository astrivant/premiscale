"""
Verify broker delivery, leases, and process shutdown against a disposable Redis-compatible server.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from contextlib import ExitStack
import json
import multiprocessing as mp
import os
from pathlib import Path
from queue import Empty
import signal
from threading import Event
from time import monotonic, sleep
from uuid import uuid4

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from premiscale.autoscaling.actions import Action, Null
from premiscale.config.v1alpha1 import Broker
from premiscale.daemon.processes import ProcessSpec
from premiscale.daemon.supervisor import Supervisor
from premiscale.messaging import InvalidMessage, LostLease, RedisQueue
from premiscale.messaging.channels import action_queue, decode_action, encode_action, platform_queue

if TYPE_CHECKING:
    from typing import Any, Iterator


@pytest.fixture
def broker() -> Iterator[Broker]:
    """
    Isolate every test, including parallel workers, in a disposable key namespace.

    Yields:
        Broker: Resource available for the duration of the context.

    Raises:
        RedisConnectionError: If the underlying operation fails after cleanup.
        RedisTimeoutError: If the underlying operation fails after cleanup.
    """
    url = os.getenv('PREMISCALE_TEST_REDIS_URL')
    if not url:
        pytest.skip('Set PREMISCALE_TEST_REDIS_URL to run broker integration tests')
    config = Broker(url=url, namespace=f'test-{uuid4()}', leaseSeconds=3, blockMilliseconds=100)
    with Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1) as client:
        deadline = monotonic() + 20
        while True:
            try:
                client.ping()
                break
            except (RedisConnectionError, RedisTimeoutError):
                if monotonic() >= deadline:
                    raise
                sleep(0.1)
        try:
            yield config
        finally:
            keys = list(client.scan_iter(match=f'premiscale:{{{config.namespace}:*'))
            if keys:
                client.delete(*keys)


def test_json_delivery_and_worker_distribution(broker: Broker) -> None:
    """
    Verify json delivery and worker distribution.

    Args:
        broker (Broker): Isolated queue configuration for the test.

    Returns:
        None: No value is returned.
    """
    with ExitStack() as resources:
        producer = resources.enter_context(platform_queue(broker))
        first = resources.enter_context(platform_queue(broker))
        second = resources.enter_context(platform_queue(broker))
        message_id = str(uuid4())
        producer.put('first', message_id=message_id)
        producer.put('second')
        with first.delivery() as one, second.delivery() as two:
            assert one.payload == 'first'
            assert one.message_id == message_id
            assert two.payload == 'second'
            assert one.id != two.id
            assert producer.client.xpending(first.key, first.group)['pending'] == 2
        assert producer.client.xlen(first.key) == 0
        assert producer.client.xpending(first.key, first.group)['pending'] == 0
        with pytest.raises(Empty), second.delivery(block=False):
            pytest.fail('Acknowledged messages were delivered again')


def test_publish_before_consumer_start_and_recover_after_connection_closes(broker: Broker) -> None:
    """
    Verify publish before consumer start and recover after connection closes.

    Args:
        broker (Broker): Isolated queue configuration for the test.

    Returns:
        None: No value is returned.

    Raises:
        RuntimeError: handler failed before acknowledgement.
    """
    with ExitStack() as resources:
        producer = platform_queue(broker)
        resources.callback(producer.close)
        producer.put('recover me')
        with platform_queue(broker) as old:
            with pytest.raises(RuntimeError), old.delivery() as original:
                raise RuntimeError('handler failed before acknowledgement')
        with platform_queue(broker) as replacement:
            # Advance this delivery's idle age without a slow wall-clock timeout.
            replacement.client.xclaim(old.key, old.group, old.consumer, 0, [original.id], idle=4000)
            with replacement.delivery(block=False) as recovered:
                assert recovered == original
            assert replacement.client.xlen(old.key) == 0


def test_active_handler_renews_lease(broker: Broker) -> None:
    """
    Verify active handler renews lease.

    Args:
        broker (Broker): Isolated queue configuration for the test.

    Returns:
        None: No value is returned.
    """
    with platform_queue(broker) as active, platform_queue(broker) as competitor:
        active.put('long operation')
        with active.delivery():
            sleep(3.4)
            with pytest.raises(Empty), competitor.delivery(block=False):
                pytest.fail('An active handler lost its delivery')
        assert active.client.xlen(active.key) == 0


def test_old_owner_cannot_acknowledge_reassigned_work(broker: Broker) -> None:
    """
    Verify old owner cannot acknowledge reassigned work.

    Args:
        broker (Broker): Isolated queue configuration for the test.

    Returns:
        None: No value is returned.
    """
    with platform_queue(broker) as old, platform_queue(broker) as replacement:
        old.put('claimed')
        with pytest.raises(LostLease), old.delivery() as original:
            replacement.client.xclaim(old.key, old.group, old.consumer, 0, [original.id], idle=4000)
            new_delivery = replacement._receive(block=False)
            assert new_delivery == original
            with pytest.raises(LostLease):
                old._touch(original)
        assert replacement.client.xpending(old.key, old.group)['pending'] == 1
        replacement._ack(new_delivery)


@pytest.mark.parametrize('body', [
    'not json',
    json.dumps({'version': True, 'id': '00000000-0000-0000-0000-000000000001', 'data': 'text'}),
    json.dumps({'version': 1, 'id': 123, 'data': 'text'}),
    json.dumps({'version': 1, 'id': '00000000-0000-0000-0000-000000000001', 'data': {'unexpected': 'object'}}),
])
def test_invalid_payload_is_quarantined_and_does_not_block_next_message(broker: Broker, body: str) -> None:
    """
    Verify invalid payload is quarantined and does not block next message.

    Args:
        broker (Broker): Isolated queue configuration for the test.
        body (str): Serialized message envelope under test.

    Returns:
        None: No value is returned.
    """
    with platform_queue(broker) as queue:
        queue.client.xadd(queue.key, {'message': body})
        queue.put('valid')
        with pytest.raises(InvalidMessage), queue.delivery(block=False):
            pytest.fail('Invalid payload was delivered')
        with queue.delivery(block=False) as delivery:
            assert delivery.payload == 'valid'
        assert queue.client.xpending(queue.key, queue.group)['pending'] == 0
        rejected = queue.client.xrange(queue.dead_letters)
        assert rejected is not None
        assert len(rejected) == 1
        rejected_fields = rejected[0][1]
        assert rejected_fields is not None
        assert rejected_fields['message'] == body


def _receive_until_shutdown(config: Any, ready: Any) -> None:
    """
    Hold a delivery until the test terminates this consumer.

    Args:
        config (Any): Parsed controller configuration.
        ready (Any): Readiness event or marker used to coordinate test processes.

    Returns:
        None: No value is returned.
    """
    with platform_queue(config) as queue, queue.delivery():
        Path(ready).touch()
        while True:
            signal.pause()


@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGKILL])
def test_process_exit_preserves_inflight_work(broker: Broker, tmp_path: Path, signum: int) -> None:
    """
    Verify process exit preserves inflight work.

    Args:
        broker (Broker): Isolated queue configuration for the test.
        tmp_path (Path): Isolated temporary directory supplied by pytest.
        signum (int): POSIX signal number to deliver.

    Returns:
        None: No value is returned.
    """
    ready = tmp_path / 'ready'
    with platform_queue(broker) as producer:
        stream_id = producer.put('unfinished')
        supervisor = Supervisor(
            [ProcessSpec('consumer', _receive_until_shutdown, (broker, str(ready)))],
            mp.get_context('spawn'), shutdown_timeout=2, kill_timeout=2,
        )
        try:
            supervisor.start(Event())
            deadline = monotonic() + 10
            while not ready.exists():
                if monotonic() >= deadline:
                    pytest.fail('Consumer did not start')
                sleep(0.02)
            process = supervisor.processes[0][1]
            assert process.pid is not None
            os.kill(process.pid, signum)
        finally:
            supervisor.close()
        pending = producer.client.xpending_range(producer.key, producer.group, '-', '+', 1)
        assert len(pending) == 1
        consumer = pending[0]['consumer']
        assert isinstance(consumer, (str, bytes))
        producer.client.xclaim(producer.key, producer.group, consumer, 0, [stream_id], idle=4000)
        with producer.delivery(block=False) as recovered:
            assert recovered.payload == 'unfinished'


def test_queue_namespace_and_channels_are_isolated(broker: Broker) -> None:
    """
    Verify queue namespace and channels are isolated.

    Args:
        broker (Broker): Isolated queue configuration for the test.

    Returns:
        None: No value is returned.
    """
    other = Broker(url=broker.url, namespace=f'{broker.namespace}:other')
    with platform_queue(broker) as platform, action_queue(broker) as actions, platform_queue(other) as isolated:
        platform.put('one')
        actions.put(Null())
        with pytest.raises(Empty), isolated.delivery(block=False):
            pytest.fail('A different controller received the message')
        with actions.delivery() as action:
            assert isinstance(action.payload, Null)
        with platform.delivery() as message:
            assert message.payload == 'one'


@pytest.mark.parametrize('options', [
    {'url': 'http://localhost'}, {'namespace': ''}, {'namespace': '{other}'},
    {'leaseSeconds': 0}, {'socketTimeout': 1}, {'blockMilliseconds': 0}, {'connectTimeout': 0},
])
def test_invalid_broker_configuration(options: dict[str, Any]) -> None:
    """
    Verify invalid broker configuration.

    Args:
        options (dict[str, Any]): Configuration overrides expected to fail validation.

    Returns:
        None: No value is returned.
    """
    with pytest.raises(ValueError):
        Broker(**options)


def test_environment_defaults_and_safe_client_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify environment defaults and safe client construction.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        None: No value is returned.
    """
    monkeypatch.setenv('PREMISCALE_REDIS_URL', 'rediss://user:secret@dragonfly:6379/0')
    monkeypatch.setenv('PREMISCALE_QUEUE_NAMESPACE', 'test:controller')
    config = Broker(caFile='/mounted/ca.pem')
    assert config.url == os.environ['PREMISCALE_REDIS_URL']
    assert config.namespace == 'test:controller'
    assert 'secret' not in repr(config)
    queue = platform_queue(config)
    assert queue.client.connection_pool.connection_kwargs['ssl_ca_certs'] == '/mounted/ca.pem'
    queue.close()
    with pytest.raises(ValueError, match='rediss'):
        platform_queue(Broker(url='redis://localhost', caFile='/mounted/ca.pem'))
    with pytest.raises(ValueError, match='Unknown'):
        RedisQueue(config, 'unknown')


def test_action_codec_rejects_unsupported_commands() -> None:
    """
    Verify action codec rejects unsupported commands.

    Returns:
        None: No value is returned.
    """
    assert isinstance(decode_action(encode_action(Null())), Null)
    with pytest.raises(ValueError):
        decode_action({'action': 'create', 'module': 'arbitrary.python'})
    with pytest.raises(ValueError):
        encode_action(cast(Action, object()))
