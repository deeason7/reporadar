"""Kafka adapters — the producer sink and the consumer source.

One module owns the Kafka transport in both directions, so the wire format and
this client's quirks live in one place.

``KafkaSink`` is the produce side. ``poll_stream`` hands each batch of fresh
events to an ``EventSink``; this one produces them onto Kafka in the wire format
(``ingest.wire``): value = the versioned envelope, key = the repository id, so
one repository's events share a partition and keep their order for every
consumer.

Sends are pipelined, confirmation is a per-batch barrier: every event is
handed to the producer first (letting the client coalesce them into few
requests), then all delivery futures are awaited before the call returns. A
failed confirmation raises — the service dies loudly rather than dropping
events silently — and a restart may then resend part of the batch:
at-least-once delivery, which is why consumers dedupe by event id and
completeness is measured by reconciliation rather than assumed.

``kafka_source`` is the read side: the batch stream ``consume_stream`` pulls
from. It polls with a timeout so a stop arriving between messages is honored
within that timeout rather than waiting for traffic to show up, and it owns the
offset commit — :func:`_consumed_batches` explains why the commit sits exactly
where it does.

Each client's lifetime belongs to the caller: use :func:`kafka_sink` or
:func:`kafka_source` to get a started client scoped to one run.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.consumer import ConsumedMessage, DeadLetter, MessageSource
from reporadar.ingest.wire import encode_dead_letter, encode_key, encode_value

CLIENT_ID = "reporadar-producer"  # how this process introduces itself in broker logs/metrics
DLQ_CLIENT_ID = "reporadar-dlq-producer"
CONSUMER_CLIENT_ID = "reporadar-consumer"

# The consumer group is this pipeline's identity to the broker: its committed
# offsets are the record of what has been stored, and every replica sharing this
# name shares the partitions rather than duplicating the work.
CONSUMER_GROUP = "reporadar-store-writer"

# Long enough that an idle topic isn't a busy-loop, short enough that a stop is
# honored promptly — this is the worst-case delay between SIGTERM and shutdown.
POLL_TIMEOUT_MS = 1_000


class EventProducer(Protocol):
    """The slice of an async producer the sink needs (aiokafka-shaped):
    hand bytes over, get back a future that resolves on delivery."""

    async def send(
        self, topic: str, value: bytes | None = None, key: bytes | None = None
    ) -> Awaitable[object]: ...


class KafkaSink:
    """An ``EventSink`` producing each event as one message on ``topic``."""

    def __init__(self, producer: EventProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def __call__(self, events: Sequence[RawEvent]) -> None:
        captured_at = datetime.now(tz=UTC)  # one capture instant stamps the whole batch
        deliveries = [
            await self._producer.send(
                self._topic,
                value=encode_value(event, captured_at=captured_at),
                key=encode_key(event),
            )
            for event in events
        ]
        await asyncio.gather(*deliveries)


@asynccontextmanager
async def kafka_sink(settings: Settings) -> AsyncIterator[KafkaSink]:
    """A ready :class:`KafkaSink`: producer started on entry, stopped on exit.

    ``stop()`` drains anything still buffered, and runs crash or not — a dying
    service flushes what it already accepted instead of leaking a connection.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, client_id=CLIENT_ID
    )
    await producer.start()
    try:
        yield KafkaSink(producer, topic=settings.kafka_live_topic)
    finally:
        await producer.stop()


class KafkaDeadLetterSink:
    """A ``DeadLetterSink`` producing each dead letter as one message on ``topic``.

    The produce-side mirror of :class:`KafkaSink`: the same pipelined-send-then-
    barrier shape and the same at-least-once contract (a failed confirmation
    raises rather than losing the record). The message keeps the original repo-id
    key, so a repository's poison messages land on the partition its live events
    do — and the value is the dead-letter envelope (``ingest.wire``), carrying the
    original bytes plus the triage reason for an operator or a replay tool.
    """

    def __init__(self, producer: EventProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def __call__(self, dead_letters: Sequence[DeadLetter]) -> None:
        deliveries = [
            await self._producer.send(
                self._topic,
                value=encode_dead_letter(
                    reason=letter.reason,
                    detail=letter.detail,
                    value=letter.message.value,
                    key=letter.message.key,
                ),
                key=letter.message.key,
            )
            for letter in dead_letters
        ]
        await asyncio.gather(*deliveries)


@asynccontextmanager
async def kafka_dead_letter_sink(settings: Settings) -> AsyncIterator[KafkaDeadLetterSink]:
    """A ready :class:`KafkaDeadLetterSink`: producer started on entry, stopped on exit.

    A producer dedicated to the dead-letter topic — the consume path produces
    nowhere else — with the same lifecycle guarantee as :func:`kafka_sink`.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, client_id=DLQ_CLIENT_ID
    )
    await producer.start()
    try:
        yield KafkaDeadLetterSink(producer, topic=settings.kafka_dlq_topic)
    finally:
        await producer.stop()


class ConsumerRecord(Protocol):
    """The slice of a consumed record the source reads."""

    value: bytes | None
    key: bytes | None


class EventConsumer(Protocol):
    """The slice of an async consumer the source needs (aiokafka-shaped):
    ask for whatever has arrived within a timeout, and commit offsets on demand."""

    async def getmany(self, *, timeout_ms: int) -> Mapping[object, Sequence[ConsumerRecord]]: ...

    async def commit(self) -> None: ...


async def _consumed_batches(
    consumer: EventConsumer, *, timeout_ms: int
) -> AsyncGenerator[Sequence[ConsumedMessage], None]:
    """Yield message batches, committing the previous one on the way back in.

    The commit sits *after* the yield deliberately. This generator resumes only
    when the consume loop comes back for another batch, and the loop only does
    that once it has stored the batch it was handed — so "resumed" is precisely
    the signal "the last batch is durable". That places the commit after the
    store without the seam having to carry a commit call at all.

    Both consequences of that placement are the correct ones. If storing raises,
    the loop never comes back, the offsets stay put, and the batch is
    redelivered. On a clean stop the final batch is likewise left uncommitted
    and redelivered on restart. That is at-least-once, which the store's
    idempotent insert absorbs — whereas committing on the way *out* would
    quietly commit batches that never landed, which is at-most-once, which is
    data loss.

    An empty batch is yielded when the poll times out with nothing to show. That
    is not noise: handing control back is exactly what lets the loop check its
    stop event, and an empty batch counts as a batch for the same reason a
    rate-limited cycle counts for the poller — the run did the work of looking.

    A message with no value (a tombstone; nothing this pipeline writes) becomes
    empty bytes rather than being skipped: it fails to decode, so it lands in
    the dead-letter queue with a reason instead of vanishing.
    """
    while True:
        records = await consumer.getmany(timeout_ms=timeout_ms)
        batch = [
            ConsumedMessage(value=record.value or b"", key=record.key)
            for partition_records in records.values()
            for record in partition_records
        ]
        yield batch
        if batch:
            await consumer.commit()


@asynccontextmanager
async def kafka_source(
    settings: Settings, *, timeout_ms: int = POLL_TIMEOUT_MS
) -> AsyncIterator[MessageSource]:
    """A ready ``MessageSource``: consumer started on entry, stopped on exit.

    Auto-commit is off, and that is the whole point: offsets here record what
    has been *stored*, not what has been *delivered*, and only the batch
    generator knows the difference. A new group starts from the beginning of the
    topic — an ingestion pipeline that silently skipped the backlog it hadn't
    read yet would be lying about completeness.
    """
    consumer = AIOKafkaConsumer(
        settings.kafka_live_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=CONSUMER_GROUP,
        client_id=CONSUMER_CLIENT_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    batches = _consumed_batches(consumer, timeout_ms=timeout_ms)
    try:
        yield batches
    finally:
        # Closing throws GeneratorExit *at* the yield, so the commit that follows
        # it never runs — exit can't commit a batch the loop never finished.
        await batches.aclose()
        await consumer.stop()
