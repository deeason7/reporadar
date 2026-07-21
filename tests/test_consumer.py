from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from reporadar.github.events import RawEvent
from reporadar.ingest.consumer import ConsumedMessage, DeadLetter, consume_stream
from reporadar.ingest.wire import WireEnvelope, encode_key, encode_value


class _BatchSource(AsyncIterator[Sequence[ConsumedMessage]]):
    """An async source of message batches that records how many pulls it served."""

    def __init__(self, *batches: Sequence[ConsumedMessage]) -> None:
        self._batches = iter(batches)
        self.pulls = 0

    async def __anext__(self) -> Sequence[ConsumedMessage]:
        try:
            batch = next(self._batches)
        except StopIteration:
            raise StopAsyncIteration from None
        self.pulls += 1
        return batch


class _CollectStore:
    """A ValidatedStore that records every envelope it is handed."""

    def __init__(self) -> None:
        self.stored: list[WireEnvelope] = []
        self.calls = 0

    async def __call__(self, envelopes: Sequence[WireEnvelope]) -> None:
        self.calls += 1
        self.stored.extend(envelopes)

    @property
    def ids(self) -> list[str]:
        return [envelope.event.id for envelope in self.stored]


class _CollectDeadLetters:
    """A DeadLetterSink that records every dead letter it is handed."""

    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []
        self.calls = 0

    async def __call__(self, letters: Sequence[DeadLetter]) -> None:
        self.calls += 1
        self.letters.extend(letters)


def _event(event_dict: dict[str, Any], id_: str, repo_id: int = 2) -> RawEvent:
    """A RawEvent differing from the fixture only by id (and optionally repo id)."""
    return RawEvent.model_validate(
        {**event_dict, "id": id_, "repo": {**event_dict["repo"], "id": repo_id}}
    )


def _valid(event_dict: dict[str, Any], id_: str, repo_id: int = 2) -> ConsumedMessage:
    """A well-formed wire message carrying the event with ``id_``."""
    event = _event(event_dict, id_, repo_id)
    return ConsumedMessage(value=encode_value(event), key=encode_key(event))


# The four ways a message fails to decode, mirroring test_wire.
_CORRUPT = ConsumedMessage(value=b"{not json")
# Not text at all — the case the base64 dead-letter envelope was designed for, and
# the one this fixture set was missing: b"{not json" is valid ASCII, so it only ever
# exercised the JSONDecodeError branch.
_NOT_TEXT = ConsumedMessage(value=b"\xff\xfe not text at all \x00\x01", key=b"2")
_FOREIGN_VERSION = ConsumedMessage(value=json.dumps({"v": 2, "frame": {}}).encode())
_WRONG_SHAPE = ConsumedMessage(
    value=json.dumps({"v": 1, "captured_at": "2026-07-14T18:00:00Z"}).encode()
)


async def test_consume_stores_fresh_events_and_dedupes(event_dict: dict[str, Any]) -> None:
    # Two batches overlapping on id "b": the store sees each unique event once, in
    # order, and the cross-batch duplicate is counted rather than stored.
    source = _BatchSource(
        [_valid(event_dict, "a"), _valid(event_dict, "b")],
        [_valid(event_dict, "b"), _valid(event_dict, "c")],
    )
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    counters = await consume_stream(source, store, dead_letters)

    assert store.ids == ["a", "b", "c"]
    assert dead_letters.letters == []
    assert counters.batches == 2
    assert counters.consumed == 4
    assert counters.stored == 3
    assert counters.duplicates == 1
    assert counters.dead_lettered == 0


async def test_the_store_receives_both_clocks_per_message(event_dict: dict[str, Any]) -> None:
    # The store persists capture lag per row, so the whole envelope has to reach it,
    # not the bare event. captured_at is stamped per message: one consumed batch can
    # carry messages a producer stamped at different instants, so a single per-batch
    # stamp downstream would be a fiction.
    first = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    second = datetime(2026, 7, 16, 12, 5, tzinfo=UTC)
    event_a = _event(event_dict, "a")
    source = _BatchSource(
        [
            ConsumedMessage(value=encode_value(event_a, captured_at=first)),
            ConsumedMessage(value=encode_value(_event(event_dict, "b"), captured_at=second)),
        ]
    )
    store = _CollectStore()

    await consume_stream(source, store, _CollectDeadLetters())

    assert [envelope.captured_at for envelope in store.stored] == [first, second]
    assert store.stored[0].event.created_at == event_a.created_at  # event time, kept distinct


async def test_a_message_that_is_not_text_is_dead_lettered_not_fatal(
    event_dict: dict[str, Any],
) -> None:
    # The poison-pill case, and the worst one: a non-UTF-8 payload that escapes as an
    # uncaught exception kills the consumer, leaves offsets uncommitted, and is
    # redelivered on restart — so the same message crashes the process forever and
    # ingestion stops completely. That is the exact outcome dead-lettering exists to
    # prevent, so it gets its own test rather than riding along in a list.
    source = _BatchSource([_valid(event_dict, "a"), _NOT_TEXT, _valid(event_dict, "b")])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    counters = await consume_stream(source, store, dead_letters)

    assert store.ids == ["a", "b"]  # the loop kept going past the poison message
    assert [letter.reason for letter in dead_letters.letters] == ["corrupt"]
    assert dead_letters.letters[0].message is _NOT_TEXT  # original bytes ride to the DLQ
    assert counters.dead_lettered == 1
    assert counters.stored == 2


async def test_decode_failures_are_dead_lettered_and_the_stream_survives(
    event_dict: dict[str, Any],
) -> None:
    # One good message plus each of the three decode failures: the valid event still
    # lands, the bad ones are triaged to the DLQ by reason, and one poison message
    # never halts the loop.
    source = _BatchSource([_valid(event_dict, "a"), _CORRUPT, _FOREIGN_VERSION, _WRONG_SHAPE])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    counters = await consume_stream(source, store, dead_letters)

    assert store.ids == ["a"]
    assert [letter.reason for letter in dead_letters.letters] == [
        "corrupt",
        "unsupported_version",
        "invalid_shape",
    ]
    assert dead_letters.letters[0].message is _CORRUPT  # the original bytes ride along
    assert all(letter.detail for letter in dead_letters.letters)  # the exception detail is kept
    assert counters.consumed == 4
    assert counters.stored == 1
    assert counters.dead_lettered == 3
    assert counters.duplicates == 0


async def test_stop_already_set_consumes_nothing(event_dict: dict[str, Any]) -> None:
    source = _BatchSource([_valid(event_dict, "a")])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()
    stop = asyncio.Event()
    stop.set()

    counters = await consume_stream(source, store, dead_letters, stop=stop)

    assert counters.batches == 0
    assert store.stored == []
    assert source.pulls == 0  # the loop broke before pulling the first batch


async def test_stop_mid_run_finishes_the_current_batch_then_ends(
    event_dict: dict[str, Any],
) -> None:
    source = _BatchSource(
        [_valid(event_dict, "a"), _valid(event_dict, "b")],
        [_valid(event_dict, "c")],
    )
    stop = asyncio.Event()
    dead_letters = _CollectDeadLetters()

    class StopAfterFirstBatch:
        def __init__(self) -> None:
            self.ids: list[str] = []

        async def __call__(self, envelopes: Sequence[WireEnvelope]) -> None:
            self.ids.extend(envelope.event.id for envelope in envelopes)
            stop.set()  # ask the loop to stop once this batch is stored

    store = StopAfterFirstBatch()
    counters = await consume_stream(source, store, dead_letters, stop=stop)

    assert counters.batches == 1
    assert store.ids == ["a", "b"]  # the whole first batch landed
    assert source.pulls == 1  # the second batch was never pulled


async def test_progress_and_exit_are_logged(
    event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    source = _BatchSource([_valid(event_dict, "a")])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    with caplog.at_level(logging.INFO, logger="reporadar.ingest.consumer"):
        await consume_stream(source, store, dead_letters, report_every=1)

    assert "consume progress" in caplog.text  # a long-running loop reports as it goes
    assert "consume stream stopped" in caplog.text  # final summary on exit


async def test_dead_letters_are_logged_not_silent(caplog: pytest.LogCaptureFixture) -> None:
    source = _BatchSource([_CORRUPT])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    with caplog.at_level(logging.WARNING, logger="reporadar.ingest.consumer"):
        await consume_stream(source, store, dead_letters)

    assert "dead-lettered" in caplog.text
    assert "corrupt" in caplog.text  # the reason breakdown is visible, not just a count


async def test_report_every_zero_disables_progress_reporting(
    event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # The documented off switch: report_every=0 must mean "no progress lines",
    # not a division by zero on the first batch.
    source = _BatchSource([_valid(event_dict, "a")], [_valid(event_dict, "b")])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    with caplog.at_level(logging.INFO, logger="reporadar.ingest.consumer"):
        counters = await consume_stream(source, store, dead_letters, report_every=0)

    assert counters.batches == 2  # the run completed
    assert "consume progress" not in caplog.text
    assert "consume stream stopped" in caplog.text  # the exit summary is unconditional


async def test_a_clean_batch_never_invokes_the_dead_letter_sink(
    event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # The dead-letter sink is for dead letters: on an all-valid batch it must not
    # be called at all — a concrete sink would pay a broker round-trip per healthy
    # batch — and no warning may cry wolf about zero failures.
    source = _BatchSource([_valid(event_dict, "a"), _valid(event_dict, "b")])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    with caplog.at_level(logging.WARNING, logger="reporadar.ingest.consumer"):
        await consume_stream(source, store, dead_letters)

    assert dead_letters.calls == 0
    assert "dead-lettered" not in caplog.text


async def test_an_all_failures_batch_never_invokes_the_store() -> None:
    # Symmetric: the store is for validated events. A batch with nothing valid
    # must route to the DLQ without opening a pointless store write.
    source = _BatchSource([_CORRUPT, _FOREIGN_VERSION])
    store = _CollectStore()
    dead_letters = _CollectDeadLetters()

    counters = await consume_stream(source, store, dead_letters)

    assert store.calls == 0
    assert dead_letters.calls == 1
    assert counters.dead_lettered == 2
