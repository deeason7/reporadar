"""Kafka event sink — fresh events onto the live-events topic.

``poll_stream`` hands each batch of fresh events to an ``EventSink``; this one
produces them onto Kafka in the wire format (``ingest.wire``): value = the
versioned envelope, key = the repository id, so one repository's events share
a partition and keep their order for every consumer.

Sends are pipelined, confirmation is a per-batch barrier: every event is
handed to the producer first (letting the client coalesce them into few
requests), then all delivery futures are awaited before the call returns. A
failed confirmation raises — the service dies loudly rather than dropping
events silently — and a restart may then resend part of the batch:
at-least-once delivery, which is why consumers dedupe by event id and
completeness is measured by reconciliation rather than assumed.

The producer's lifetime belongs to the caller: the sink only sends. Use
:func:`kafka_sink` to get a started producer scoped to one run.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol

from aiokafka import AIOKafkaProducer

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.wire import encode_key, encode_value

CLIENT_ID = "reporadar-producer"  # how this process introduces itself in broker logs/metrics


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
