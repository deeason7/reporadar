"""Validating stream consumer: message bytes → validated events in the store.

``KafkaSink`` puts wire-format messages on the stream; this is the read half.
``consume_stream`` pulls batches from an injected **source**, validates each
message with the one wire contract (:mod:`reporadar.ingest.wire`), dedupes by
event id, hands the fresh valid events to a **store**, and routes anything that
fails to decode to a **dead-letter sink** — never dropping a message silently,
never letting one bad message halt ingestion.

Every collaborator is injected, exactly as the poller's sink is: the loop never
learns that its source is Kafka, its store is a database, or its dead-letter sink
is another topic. It is pure read-side policy — decode, dedupe, route — and the
concrete adapters land as their own pieces behind stable seams.

Delivery is at-least-once by design: a redelivered message reappears, so the
bounded dedup window absorbs the common recent-redelivery case and the
store is expected to be idempotent by event id for the rest — the same
belt-and-suspenders the produce side uses (dedup window plus reconciliation), so
at-least-once *delivery* becomes effectively-once *effect*.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass

from pydantic import ValidationError

from reporadar.ingest.dedup import DEFAULT_SEEN_WINDOW, RecentIds
from reporadar.ingest.metrics import ConsumeCounters
from reporadar.ingest.wire import UnsupportedSchemaVersionError, WireEnvelope, decode_value

logger = logging.getLogger(__name__)

# Dead-letter triage reasons — one per decode_value failure, so an operator
# reads the reason, not the stack trace.
REASON_CORRUPT = "corrupt"  # not JSON at all — a truncated write or a foreign producer
REASON_UNSUPPORTED_VERSION = "unsupported_version"  # JSON, but not an envelope this reader speaks
REASON_INVALID_SHAPE = "invalid_shape"  # our version, wrong shape — a producer bug


@dataclass(frozen=True)
class ConsumedMessage:
    """One raw message pulled from the stream, before any validation.

    Only the bytes the decoder needs: the ``value`` (the wire envelope) and the
    ``key`` (the repo id, kept for dead-letter provenance). Partition and offset
    arrive with the concrete source, once offset-commit management does.
    """

    value: bytes
    key: bytes | None = None


@dataclass(frozen=True)
class DeadLetter:
    """A message that could not be turned into a validated event.

    Carries the original bytes, a triage ``reason`` (one of the ``REASON_*``
    constants), and the exception ``detail`` — so the record is self-contained
    for the DLQ, later replay, or an operator's eyeballs.
    """

    message: ConsumedMessage
    reason: str
    detail: str


MessageSource = AsyncIterator[Sequence[ConsumedMessage]]
"""A stream of message batches to pull from — an aiokafka consumer later, a list in tests."""

ValidatedStore = Callable[[Sequence[WireEnvelope]], Awaitable[None]]
"""Where validated, deduped events are durably written — idempotent by event id.

The whole envelope travels, not just the event inside it: the store persists both
clocks, and ``captured_at`` is per-message — one consumed batch can carry messages
produced at different instants — so it cannot be re-derived once dropped.
"""

DeadLetterSink = Callable[[Sequence[DeadLetter]], Awaitable[None]]
"""Where undecodable messages go instead of being dropped — a dead-letter topic later."""


def _reason(exc: Exception) -> str:
    """Map a ``decode_value`` failure to its operator-facing triage reason."""
    if isinstance(exc, UnsupportedSchemaVersionError):
        return REASON_UNSUPPORTED_VERSION
    if isinstance(exc, json.JSONDecodeError):
        return REASON_CORRUPT
    return REASON_INVALID_SHAPE  # pydantic ValidationError — the only case left


async def consume_stream(
    source: MessageSource,
    store: ValidatedStore,
    dead_letter: DeadLetterSink,
    *,
    seen_window: int = DEFAULT_SEEN_WINDOW,
    report_every: int = 60,
    stop: asyncio.Event | None = None,
) -> ConsumeCounters:
    """Consume message batches until the source ends or ``stop`` is set.

    Each message is decoded with :func:`~reporadar.ingest.wire.decode_value`: the
    ones that validate are deduped by event id and handed to ``store``; the ones
    that fail are routed to ``dead_letter`` with a triage reason, so a poison
    message is isolated, counted, and logged — never fatal, never silent.

    ``stop`` is checked *before* every pull, so a stop already set consumes
    nothing; a concrete source should therefore poll with a timeout, so a stop
    that arrives between messages is honored within that timeout rather than
    waiting for the next message to show up. Progress is logged every
    ``report_every`` batches (0 disables) and once more on exit. Returns the
    final counters so a caller can assert on or surface the run.
    """
    seen = RecentIds(maxlen=seen_window)
    counters = ConsumeCounters()
    batches = aiter(source)
    while True:
        if stop is not None and stop.is_set():
            break
        try:
            batch = await anext(batches)
        except StopAsyncIteration:
            break
        fresh: list[WireEnvelope] = []
        failures: list[DeadLetter] = []
        for message in batch:
            try:
                envelope = decode_value(message.value)
            except (json.JSONDecodeError, UnsupportedSchemaVersionError, ValidationError) as exc:
                failures.append(DeadLetter(message, _reason(exc), str(exc)))
                continue
            if seen.add(envelope.event.id):
                fresh.append(envelope)
        if fresh:
            await store(fresh)
        if failures:
            await dead_letter(failures)
            by_reason = Counter(letter.reason for letter in failures)
            logger.warning("dead-lettered %d message(s): %s", len(failures), dict(by_reason))
        counters.record_batch(consumed=len(batch), stored=len(fresh), dead_lettered=len(failures))
        if report_every > 0 and counters.batches % report_every == 0:
            logger.info("consume progress: %s", counters.as_dict())
    logger.info("consume stream stopped: %s", counters.as_dict())
    return counters
