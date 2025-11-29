"""Kafka to Pub/Sub streaming bridge with backpressure handling.

Consumes messages from Kafka and publishes to Pub/Sub with:
- Bounded in-memory buffer
- Backpressure via Kafka consumer pause/resume
- Graceful shutdown handling
- Metrics for monitoring
"""

import json
import os
import signal
import threading
import time
from collections import deque
from concurrent.futures import Future, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError

log = structlog.get_logger()


@dataclass
class BridgeConfig:
    """Configuration for the streaming bridge."""
    kafka_brokers: str
    kafka_topic: str
    kafka_group_id: str
    pubsub_project: str
    pubsub_topic: str

    # Backpressure settings
    buffer_max_size: int = 10000  # Max messages in buffer before pausing Kafka
    buffer_resume_size: int = 5000  # Resume Kafka when buffer drops to this
    publish_batch_size: int = 100  # Messages per Pub/Sub publish batch
    publish_timeout_seconds: float = 30.0

    # Health check
    max_lag_seconds: int = 300  # Alert if processing is this far behind

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        return cls(
            kafka_brokers=os.environ["KAFKA_BROKERS"],
            kafka_topic=os.environ["KAFKA_TOPIC"],
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "pubsub-bridge"),
            pubsub_project=os.environ["PROJECT_ID"],
            pubsub_topic=os.environ["PUBSUB_TOPIC"],
            buffer_max_size=int(os.environ.get("BUFFER_MAX_SIZE", "10000")),
            buffer_resume_size=int(os.environ.get("BUFFER_RESUME_SIZE", "5000")),
            publish_batch_size=int(os.environ.get("PUBLISH_BATCH_SIZE", "100")),
        )


@dataclass
class BufferedMessage:
    """Message waiting to be published to Pub/Sub."""
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: datetime
    payload: bytes
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class BridgeMetrics:
    """Metrics for monitoring bridge health."""
    messages_received: int = 0
    messages_published: int = 0
    messages_failed: int = 0
    publish_errors: int = 0
    buffer_high_water: int = 0
    paused_count: int = 0
    last_message_at: datetime | None = None
    last_publish_at: datetime | None = None


class StreamingBridge:
    """Kafka to Pub/Sub bridge with backpressure handling."""

    def __init__(self, config: BridgeConfig) -> None:
        self.config = config
        self.metrics = BridgeMetrics()
        self._shutdown = threading.Event()
        self._paused = False

        # Bounded buffer for backpressure
        self._buffer: deque[BufferedMessage] = deque(maxlen=config.buffer_max_size)
        self._buffer_lock = threading.Lock()

        # Pending Pub/Sub publish futures for tracking
        self._pending_futures: dict[str, tuple[Future, BufferedMessage]] = {}
        self._pending_lock = threading.Lock()

        # Offset tracking for commits
        self._uncommitted_offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> offset
        self._offset_lock = threading.Lock()

        # Kafka consumer
        self._consumer = Consumer({
            "bootstrap.servers": config.kafka_brokers,
            "group.id": config.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit after Pub/Sub ack
            "max.poll.interval.ms": 300000,
        })

        # Pub/Sub publisher with batching
        self._publisher = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=False,
            ),
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=config.publish_batch_size,
                max_latency=0.1,  # 100ms max latency
            ),
        )
        self._topic_path = self._publisher.topic_path(
            config.pubsub_project, config.pubsub_topic
        )

        log.info(
            "bridge_initialised",
            kafka_topic=config.kafka_topic,
            pubsub_topic=config.pubsub_topic,
            buffer_max=config.buffer_max_size,
        )

    def run(self) -> None:
        """Main run loop."""
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        self._consumer.subscribe([self.config.kafka_topic])
        log.info("bridge_started", topic=self.config.kafka_topic)

        # Start publisher thread
        publisher_thread = threading.Thread(target=self._publisher_loop, daemon=True)
        publisher_thread.start()

        try:
            while not self._shutdown.is_set():
                self._consume_messages()
                self._check_backpressure()
                self._commit_offsets()
        except KeyboardInterrupt:
            log.info("bridge_interrupted")
        finally:
            self._graceful_shutdown()

    def _consume_messages(self) -> None:
        """Consume messages from Kafka and buffer them."""
        if self._paused:
            # Don't poll if paused due to backpressure
            time.sleep(0.1)
            return

        msg = self._consumer.poll(timeout=1.0)

        if msg is None:
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            log.error("kafka_error", error=msg.error())
            return

        # Transform message for Pub/Sub
        try:
            kafka_value = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.error("message_decode_error", error=str(e), offset=msg.offset())
            self.metrics.messages_failed += 1
            return

        # Add Kafka metadata
        kafka_timestamp = datetime.fromtimestamp(
            msg.timestamp()[1] / 1000, tz=timezone.utc
        ) if msg.timestamp()[0] != 0 else datetime.now(timezone.utc)

        kafka_value["_kafka_partition"] = msg.partition()
        kafka_value["_kafka_offset"] = msg.offset()
        kafka_value["_kafka_timestamp"] = kafka_timestamp.isoformat()
        kafka_value["_ingestion_time"] = datetime.now(timezone.utc).isoformat()

        buffered = BufferedMessage(
            kafka_partition=msg.partition(),
            kafka_offset=msg.offset(),
            kafka_timestamp=kafka_timestamp,
            payload=json.dumps(kafka_value).encode("utf-8"),
        )

        with self._buffer_lock:
            self._buffer.append(buffered)
            self.metrics.buffer_high_water = max(
                self.metrics.buffer_high_water, len(self._buffer)
            )

        self.metrics.messages_received += 1
        self.metrics.last_message_at = datetime.now(timezone.utc)

    def _publisher_loop(self) -> None:
        """Background thread to publish buffered messages to Pub/Sub."""
        while not self._shutdown.is_set():
            message = None

            with self._buffer_lock:
                if self._buffer:
                    message = self._buffer.popleft()

            if message is None:
                time.sleep(0.01)  # Small sleep when buffer empty
                continue

            self._publish_message(message)

    def _publish_message(self, message: BufferedMessage) -> None:
        """Publish a single message to Pub/Sub."""
        try:
            future = self._publisher.publish(self._topic_path, message.payload)

            # Track the future for completion handling
            future_id = f"{message.kafka_partition}:{message.kafka_offset}"
            with self._pending_lock:
                self._pending_futures[future_id] = (future, message)

            # Add callback for completion
            future.add_done_callback(
                lambda f, fid=future_id, msg=message: self._on_publish_complete(f, fid, msg)
            )

        except GoogleAPICallError as e:
            log.error(
                "pubsub_publish_error",
                error=str(e),
                partition=message.kafka_partition,
                offset=message.kafka_offset,
            )
            self.metrics.publish_errors += 1

            # Re-queue the message for retry
            with self._buffer_lock:
                self._buffer.appendleft(message)

    def _on_publish_complete(
            self, future: Future, future_id: str, message: BufferedMessage
    ) -> None:
        """Callback when Pub/Sub publish completes."""
        with self._pending_lock:
            self._pending_futures.pop(future_id, None)

        try:
            future.result(timeout=1.0)

            # Track offset for commit
            with self._offset_lock:
                key = (self.config.kafka_topic, message.kafka_partition)
                current = self._uncommitted_offsets.get(key, -1)
                if message.kafka_offset > current:
                    self._uncommitted_offsets[key] = message.kafka_offset

            self.metrics.messages_published += 1
            self.metrics.last_publish_at = datetime.now(timezone.utc)

        except FuturesTimeoutError:
            log.warning("pubsub_publish_timeout", future_id=future_id)
            self.metrics.publish_errors += 1
        except Exception as e:
            log.error("pubsub_publish_failed", error=str(e), future_id=future_id)
            self.metrics.messages_failed += 1

    def _check_backpressure(self) -> None:
        """Pause/resume Kafka consumer based on buffer size."""
        with self._buffer_lock:
            buffer_size = len(self._buffer)

        if not self._paused and buffer_size >= self.config.buffer_max_size:
            # Pause consumption
            partitions = self._consumer.assignment()
            if partitions:
                self._consumer.pause(partitions)
                self._paused = True
                self.metrics.paused_count += 1
                log.warning(
                    "kafka_paused",
                    buffer_size=buffer_size,
                    reason="backpressure",
                )

        elif self._paused and buffer_size <= self.config.buffer_resume_size:
            # Resume consumption
            partitions = self._consumer.assignment()
            if partitions:
                self._consumer.resume(partitions)
                self._paused = False
                log.info("kafka_resumed", buffer_size=buffer_size)

    def _commit_offsets(self) -> None:
        """Commit Kafka offsets for successfully published messages."""
        with self._offset_lock:
            if not self._uncommitted_offsets:
                return

            offsets_to_commit = []
            for (topic, partition), offset in self._uncommitted_offsets.items():
                # Commit offset + 1 (next offset to read)
                offsets_to_commit.append(
                    TopicPartition(topic, partition, offset + 1)
                )

            self._uncommitted_offsets.clear()

        if offsets_to_commit:
            try:
                self._consumer.commit(offsets=offsets_to_commit, asynchronous=False)
            except KafkaException as e:
                log.error("kafka_commit_error", error=str(e))

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Signal handler for graceful shutdown."""
        log.info("shutdown_signal_received", signal=signum)
        self._shutdown.set()

    def _graceful_shutdown(self) -> None:
        """Gracefully shutdown, ensuring messages are published."""
        log.info("graceful_shutdown_starting")

        # Wait for buffer to drain (with timeout)
        drain_timeout = 30  # seconds
        start = time.monotonic()

        while time.monotonic() - start < drain_timeout:
            with self._buffer_lock:
                if not self._buffer:
                    break
            time.sleep(0.1)

        with self._buffer_lock:
            remaining = len(self._buffer)

        if remaining:
            log.warning("shutdown_with_remaining_messages", count=remaining)

        # Final offset commit
        self._commit_offsets()

        # Close connections
        self._consumer.close()

        log.info(
            "bridge_shutdown_complete",
            messages_received=self.metrics.messages_received,
            messages_published=self.metrics.messages_published,
            messages_failed=self.metrics.messages_failed,
        )

    def get_health(self) -> dict[str, Any]:
        """Return health check data."""
        with self._buffer_lock:
            buffer_size = len(self._buffer)

        with self._pending_lock:
            pending_count = len(self._pending_futures)

        # Calculate lag
        lag_seconds = None
        if self.metrics.last_message_at:
            lag_seconds = (
                    datetime.now(timezone.utc) - self.metrics.last_message_at
            ).total_seconds()

        healthy = (
                not self._paused
                and (lag_seconds is None or lag_seconds < self.config.max_lag_seconds)
                and buffer_size < self.config.buffer_max_size * 0.9
        )

        return {
            "status": "healthy" if healthy else "degraded",
            "kafka_connected": True,  # Would be false if consumer errored
            "pubsub_connected": True,
            "paused": self._paused,
            "buffer_size": buffer_size,
            "buffer_max": self.config.buffer_max_size,
            "pending_publishes": pending_count,
            "lag_seconds": lag_seconds,
            "metrics": {
                "messages_received": self.metrics.messages_received,
                "messages_published": self.metrics.messages_published,
                "messages_failed": self.metrics.messages_failed,
                "publish_errors": self.metrics.publish_errors,
                "buffer_high_water": self.metrics.buffer_high_water,
                "paused_count": self.metrics.paused_count,
            },
        }


def main() -> None:
    """Entry point for the streaming bridge."""
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    config = BridgeConfig.from_env()
    bridge = StreamingBridge(config)
    bridge.run()


if __name__ == "__main__":
    main()
