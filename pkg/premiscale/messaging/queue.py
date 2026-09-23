"""
Consume versioned JSON messages with acknowledgements and recoverable delivery leases.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
import re
from queue import Empty
from threading import Event, Thread
from typing import Generic, TYPE_CHECKING, TypeVar, cast
from uuid import uuid4, UUID

from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError
from redis.retry import Retry

from premiscale.support.lua import load_lua

if TYPE_CHECKING:
    from typing import Any, Callable, Iterator
    from premiscale.config.v1alpha1 import Broker


T = TypeVar('T')

class InvalidMessage(ValueError):
    """
    A malformed message has been moved to the queue's dead-letter stream.
    """


class LostLease(RuntimeError):
    """
    Another consumer owns this delivery, so it must not be acknowledged here.
    """


@dataclass(frozen=True)
class Delivery(Generic[T]):
    """
    Carry the stable publisher ID, stream delivery ID, and decoded application payload.

    Attributes:
        id (str): Stable instance or delivery identifier.
        message_id (str): Stable UUID assigned by the publisher.
        payload (T): Decoded application message.
    """

    id: str
    message_id: str
    payload: T


class RedisQueue(Generic[T]):
    """
    Own one worker's Redis connection pool and consumer identity.

    Each stream has one consumer group. Pending messages are retained until acknowledged;
    delivery is at least once, so action handlers must be idempotent.
    """

    def __init__(self, config: Broker, channel: str, *,
                 encode: Callable[[T], Any] = lambda value: value,
                 decode: Callable[[Any], T] = lambda value: value) -> None:
        """
        Initialize RedisQueue with the supplied settings.

        Args:
            config (Broker): Broker connection, namespace, timeout, and lease settings.
            channel (str): Stream channel: autoscaling, platform, collection, or metrics:<subscriber>.
            encode (Callable[[T], Any]): Convert an application payload to JSON-compatible data.
            decode (Callable[[Any], T]): Validate and reconstruct an application payload from decoded JSON.

        Raises:
            ValueError: If the channel is unknown or a CA file is configured without TLS.
        """
        metric_channel = re.fullmatch(r'metrics:(_state|[a-zA-Z0-9][a-zA-Z0-9_-]{0,62})', channel)
        if channel not in {'autoscaling', 'platform', 'collection'} and metric_channel is None:
            raise ValueError('Unknown controller queue channel')
        self.config = config
        self.key = f'premiscale:{{{config.namespace}:{channel}}}:queue'
        self.dead_letters = f'premiscale:{{{config.namespace}:{channel}}}:dead-letters'
        if metric_channel is not None:
            # State and metric subscribers share a Redis Cluster slot for atomic fanout.
            prefix = f'premiscale:{{{config.namespace}:metrics}}:{metric_channel[1]}'
            self.key = f'{prefix}:queue'
            self.dead_letters = f'{prefix}:dead-letters'
        self.group = 'workers'
        self.consumer = f'{os.getenv("CONTROLLER_POD_NAME", "worker")}:{os.getpid()}:{uuid4()}'
        options: dict[str, Any] = {
            'decode_responses': True, 'protocol': 2, 'legacy_responses': False,
            'socket_connect_timeout': config.connectTimeout,
            'socket_timeout': config.socketTimeout,
            'retry': Retry(NoBackoff(), 0),
        }
        if config.caFile:
            if not config.url.startswith('rediss://'):
                raise ValueError('Broker caFile requires a rediss:// connection')
            options['ssl_ca_certs'] = config.caFile
        self.client = Redis.from_url(config.url, **options)
        self.encode = encode
        self.decode = decode
        self.cursor = '0-0'
        self.ready = False

    def initialize(self) -> None:
        """
        Connect and create the consumer group without losing previously queued messages.

        Returns:
            None: No value is returned.

        Raises:
            ResponseError: If Redis rejects group creation for a reason other than an existing group.
        """
        if self.ready:
            return
        self.client.ping()
        try:
            self.client.xgroup_create(self.key, self.group, id='0-0', mkstream=True)
        except ResponseError as error:
            if not str(error).startswith('BUSYGROUP'):
                raise
        self.ready = True

    def put(self, payload: T, *, message_id: str | None = None) -> str:
        """
        Publish JSON without loading executable Python objects from the broker.

        Args:
            payload (T): Application data carried by the message.
            message_id (str | None): Publisher UUID; a new UUID is generated when omitted.

        Returns:
            str: Redis stream entry ID assigned to the published message.
        """
        identity = str(UUID(message_id)) if message_id is not None else str(uuid4())
        body = json.dumps({'version': 1, 'id': identity, 'data': self.encode(payload)},
                          separators=(',', ':'), allow_nan=False)
        return cast(str, self.client.xadd(self.key, {'message': body}))

    def _receive(self, block: bool) -> Delivery[T]:
        """
        Claim an expired or new delivery and validate its message envelope.

        Args:
            block (bool): Whether to wait for a newly published message.

        Returns:
            Delivery[T]: Claimed delivery containing its stream ID, publisher UUID, and decoded payload.

        Raises:
            Empty: If no expired or new message is available.
            ValueError: Converted to InvalidMessage when the envelope version or publisher ID is invalid.
            LostLease: Invalid delivery is no longer owned by this consumer.
            InvalidMessage: Message moved to the dead-letter stream.
        """
        self.initialize()
        claimed = self.client.xautoclaim(self.key, self.group, self.consumer,
                                         self.config.leaseSeconds * 1000, self.cursor, count=1)
        self.cursor = claimed[0]
        entries = claimed[1]
        if not entries:
            streams = self.client.xreadgroup(
                self.group, self.consumer, {self.key: '>'}, count=1,
                block=self.config.blockMilliseconds if block else None,
            )
            # redis-py 8's unified responses use a mapping for either wire protocol.
            entries = cast('dict[str, Any]', streams).get(self.key, [])
        if not entries:
            raise Empty
        identity, fields = entries[0]
        body = fields.get('message', '')
        try:
            envelope = json.loads(body)
            if not isinstance(envelope, dict) or envelope.get('version') != 1 or isinstance(envelope.get('version'), bool):
                raise ValueError('Unknown message envelope')
            if not isinstance(envelope.get('id'), str):
                raise ValueError('Message ID must be a UUID string')
            message_id = str(UUID(envelope['id']))
            payload = self.decode(envelope['data'])
        except (ValueError, TypeError, KeyError) as error:
            rejected = self.client.eval(load_lua('messaging/reject.lua'), 2, self.key, self.dead_letters, self.group,
                                        self.consumer, identity, body, 'Invalid message envelope or payload')
            if rejected != 1:
                raise LostLease('Invalid delivery is no longer owned by this consumer') from error
            raise InvalidMessage('Message moved to the dead-letter stream') from error
        return Delivery(identity, message_id, payload)

    def _touch(self, delivery: Delivery[T]) -> None:
        """
        Renew a delivery lease only while this consumer owns it.

        Args:
            delivery (Delivery[T]): Claimed message whose lease belongs to this consumer.

        Returns:
            None: No value is returned.

        Raises:
            LostLease: Delivery is no longer owned by this consumer.
        """
        if not self.client.eval(load_lua('messaging/touch.lua'), 1, self.key, self.group, self.consumer, delivery.id):
            raise LostLease('Delivery is no longer owned by this consumer')

    def _ack(self, delivery: Delivery[T]) -> None:
        """
        Acknowledge and remove a delivery only while this consumer owns it.

        Args:
            delivery (Delivery[T]): Claimed message whose lease belongs to this consumer.

        Returns:
            None: No value is returned.

        Raises:
            LostLease: Delivery is no longer owned by this consumer.
        """
        if self.client.eval(load_lua('messaging/ack.lua'), 1, self.key, self.group, self.consumer, delivery.id) != 1:
            raise LostLease('Delivery is no longer owned by this consumer')

    @contextmanager
    def delivery(self, *, block: bool = True) -> Iterator[Delivery[T]]:
        """
        Renew ownership during processing and acknowledge only after successful completion.

        Args:
            block (bool): Whether to wait for a newly published message.

        Yields:
            Delivery[T]: Resource available for the duration of the context.

        Raises:
            LostLease: Delivery lease could not be renewed.
        """
        delivery = self._receive(block)
        stopped = Event()
        failures: list[Exception] = []

        def renew() -> None:
            """
            Keep the active delivery lease alive until processing completes.

            Returns:
                None: No value is returned.
            """
            try:
                while not stopped.wait(self.config.leaseSeconds / 3):
                    self._touch(delivery)
            except Exception as error:
                failures.append(error)

        heartbeat = Thread(target=renew, name='queue-delivery-lease', daemon=True)
        heartbeat.start()
        try:
            yield delivery
            if failures:
                raise LostLease('Delivery lease could not be renewed') from failures[0]
            self._ack(delivery)
        finally:
            stopped.set()
            heartbeat.join(timeout=self.config.socketTimeout + 1)

    def close(self) -> None:
        """
        Disconnect locally, preserving queued and pending messages for other pods.

        Returns:
            None: No value is returned.
        """
        self.client.close()
        self.client.connection_pool.disconnect()

    def __enter__(self) -> RedisQueue[T]:
        """
        Acquire the resources managed by this context.

        Returns:
            RedisQueue[T]: The initialized resource managed by this context.

        Raises:
            BaseException: If the underlying operation fails after cleanup.
        """
        try:
            self.initialize()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_args: Any) -> None:
        """
        Release resources when the context finishes.

        Args:
            *_args (Any): Unused positional arguments supplied by the caller.

        Returns:
            None: No value is returned.
        """
        self.close()
