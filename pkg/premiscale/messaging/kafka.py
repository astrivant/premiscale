"""
Transport metrics through Kafka with independent subscriber offsets and explicit commits.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
from queue import Empty
from threading import Event
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from premiscale.metrics.codec import decode_batch, encode_batch
from .queue import Delivery, InvalidMessage

if TYPE_CHECKING:
    from typing import Any, Iterator
    from premiscale.config.v1alpha1 import Kafka
    from premiscale.schemas.metrics import MetricBatch


def _decode(body: bytes) -> tuple[str, MetricBatch]:
    """
    Validate an envelope before exposing its identity and metric batch.

    Args:
        body (bytes): Versioned Kafka record payload.

    Returns:
        tuple[str, MetricBatch]: Stable UUID and reconstructed measurements.

    Raises:
        ValueError: If the envelope version is unsupported.
    """
    envelope = json.loads(body)
    if envelope['version'] != 1:
        raise ValueError('Unknown metrics envelope version')
    return str(UUID(envelope['id'])), decode_batch(envelope['data'])


class KafkaFanout:
    """
    Publish once to a retained topic consumed independently by every database group.
    """

    def __init__(self, config: Kafka) -> None:
        """
        Construct a process-local idempotent producer.

        Args:
            config (Kafka): Broker, topic, and authentication settings.
        """
        self.config = config
        self.producer = Producer({**config.options, 'bootstrap.servers': config.bootstrapServers,
                                  'enable.idempotence': True, 'acks': 'all', 'delivery.timeout.ms': 30000})

    def send(self, topic: str, identity: str, body: str) -> None:
        """
        Wait for broker acknowledgement before allowing a collection request to complete.

        Args:
            topic (str): Destination metrics or dead-letter topic.
            identity (str): Stable record key.
            body (str): Versioned JSON envelope.

        Returns:
            None: No value is returned.

        Raises:
            KafkaException: If Kafka rejects or times out the publication.
        """
        done = Event()
        errors = []

        def delivered(error: Any, _message: Any) -> None:
            """
            Capture the acknowledgement for this record across concurrent producers.

            Args:
                error (Any): Kafka delivery error, or None.
                _message (Any): Broker acknowledgement metadata.

            Returns:
                None: No value is returned.
            """
            if error is not None:
                errors.append(error)
            done.set()

        self.producer.produce(topic, key=identity, value=body, on_delivery=delivered)
        while not done.is_set():
            self.producer.poll(0.1)
        if errors:
            raise KafkaException(errors[0])

    def publish(self, batch: MetricBatch) -> str:
        """
        Publish a stable UUID which all database groups use for idempotent writes.

        Args:
            batch (MetricBatch): Reduced measurements independent of database representation.

        Returns:
            str: Stable message identity stored with the Kafka record.
        """
        identity = str(uuid4())
        body = json.dumps({'version': 1, 'id': identity, 'data': encode_batch(batch)}, allow_nan=False)
        self.send(self.config.topic, identity, body)
        return identity

    def close(self) -> None:
        """
        Give already submitted records a bounded opportunity to finish.

        Returns:
            None: No value is returned.
        """
        self.producer.flush(3)


class KafkaQueue:
    """
    Consume one database's group and commit offsets only after successful writes.
    """

    def __init__(self, config: Kafka, subscriber: str) -> None:
        """
        Prepare lazy consumer ownership; each process owns at most one database connection.

        Args:
            config (Kafka): Broker, topic, and authentication settings.
            subscriber (str): Stable database destination name.
        """
        self.config = config
        self.subscriber = subscriber
        self.consumer: Any = None
        self.ready = False

    def initialize(self) -> None:
        """
        Join the subscriber group without enabling automatic offset commits or storage.

        Returns:
            None: No value is returned.
        """
        if self.consumer is None:
            self.consumer = Consumer({**self.config.options, 'bootstrap.servers': self.config.bootstrapServers,
                                      'group.id': f'{self.config.groupPrefix}.{self.subscriber}',
                                      'enable.auto.commit': False, 'enable.auto.offset.store': False,
                                      'auto.offset.reset': 'earliest', 'allow.auto.create.topics': False})
            self.consumer.subscribe([self.config.topic])
        self.ready = True

    def _commit(self, message: Any) -> None:
        """
        Check every synchronous commit result before acknowledging a database write.

        Args:
            message (Any): Kafka message whose next offset is to be committed.

        Returns:
            None: No value is returned.

        Raises:
            KafkaException: If the broker reports a partition-specific commit failure.
        """
        for partition in self.consumer.commit(message=message, asynchronous=False):
            if partition.error is not None:
                raise KafkaException(partition.error)

    @contextmanager
    def delivery(self, *, block: bool = True) -> Iterator[Delivery[MetricBatch]]:
        """
        Retain failed database deliveries and dead-letter malformed records before committing.

        Args:
            block (bool): Wait briefly for assignment and records when True.

        Yields:
            Delivery[MetricBatch]: Decoded batch with a stable deduplication identifier.

        Raises:
            Empty: If no record is currently available.
            KafkaException: If polling, dead-letter publication, or offset commit fails.
            InvalidMessage: If an invalid record was durably dead-lettered.
            BaseException: If processing fails after releasing the consumer assignment.
        """
        self.initialize()
        message = self.consumer.poll(1 if block else 0)
        if message is None:
            raise Empty
        if message.error():
            raise KafkaException(message.error())
        try:
            try:
                identity, batch = _decode(message.value())
            except (ValueError, TypeError, KeyError, AttributeError) as error:
                producer = KafkaFanout(self.config)
                try:
                    producer.send(f'{self.config.topic}.dead', str(uuid4()), json.dumps({
                        'subscriber': self.subscriber, 'topic': message.topic(), 'partition': message.partition(),
                        'offset': message.offset(), 'body': (message.value() or b'').decode('utf-8', errors='replace'),
                    }))
                finally:
                    producer.close()
                self._commit(message)
                raise InvalidMessage('Malformed Kafka metrics record was dead-lettered') from error
            yield Delivery(f'{message.partition()}:{message.offset()}', identity, batch)
            self._commit(message)
        except BaseException:
            # Rejoin from the last committed offset; never advance past a failed write.
            self.close()
            raise

    def close(self) -> None:
        """
        Leave the group without committing unacknowledged records.

        Returns:
            None: No value is returned.
        """
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None
        self.ready = False

    def __enter__(self) -> 'KafkaQueue':
        """
        Join this database's consumer group.

        Returns:
            KafkaQueue: Initialized consumer.
        """
        self.initialize()
        return self

    def __exit__(self, *_args: Any) -> None:
        """
        Release the consumer's partitions on context exit.

        Args:
            *_args (Any): Context exit information.

        Returns:
            None: No value is returned.
        """
        self.close()


class KafkaLag:
    """
    Observe subscriber lag without joining a group or moving its offsets.
    """

    def __init__(self, config: Kafka, subscriber: str) -> None:
        """
        Create an unassigned client with the subscriber's group identity.

        Args:
            config (Kafka): Broker and metrics topic settings.
            subscriber (str): Database subscriber whose unfinished records are counted.
        """
        self.topic = config.topic
        self.consumer = Consumer({**config.options, 'bootstrap.servers': config.bootstrapServers,
                                  'group.id': f'{config.groupPrefix}.{subscriber}',
                                  'enable.auto.commit': False, 'allow.auto.create.topics': False})

    def count(self) -> int:
        """
        Count retained records after committed offsets, including in-flight database writes.

        Returns:
            int: Total backlog across all topic partitions.

        Raises:
            KafkaException: If the topic cannot be read.
        """
        topic = self.consumer.list_topics(self.topic, timeout=3).topics[self.topic]
        if topic.error is not None:
            raise KafkaException(topic.error)
        partitions = [TopicPartition(self.topic, index) for index in topic.partitions]
        committed = self.consumer.committed(partitions, timeout=3)
        total = 0
        for partition in committed:
            low, high = self.consumer.get_watermark_offsets(partition, timeout=3)
            total += max(0, high - max(low, partition.offset))
        return total

    def close(self) -> None:
        """
        Close the observation client without changing consumer group membership.

        Returns:
            None: No value is returned.
        """
        self.consumer.close()
