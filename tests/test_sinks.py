from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from reporadar.config import Settings
from reporadar.github.events import RawEvent, iter_ndjson
from reporadar.ingest.service import poll_stream
from reporadar.ingest.sinks import HourlyNdjsonSink, TeeSink

EVENTS_URL = "https://api.github.com/events"


class RecordingSink:
    """An ``EventSink`` that records the batches it receives, or fails on every call."""

    def __init__(self, *, fail: bool = False) -> None:
        self.batches: list[list[RawEvent]] = []
        self._fail = fail

    async def __call__(self, events: Sequence[RawEvent]) -> None:
        if self._fail:
            raise RuntimeError("sink boom")
        self.batches.append(list(events))


def _event(event_id: str, created_at: str) -> RawEvent:
    return RawEvent.model_validate(
        {
            "id": event_id,
            "type": "PushEvent",
            "actor": {"id": 1, "login": "octo-tester"},
            "repo": {"id": 2, "name": "octo/widgets"},
            "created_at": created_at,
            "payload": {},
        }
    )


async def test_buckets_events_into_hourly_files_by_event_time(tmp_path: Path) -> None:
    sink = HourlyNdjsonSink(tmp_path)

    # One batch straddling the 15:00→16:00 boundary must split across two files.
    await sink(
        [
            _event("1", "2026-07-07T15:10:00Z"),
            _event("2", "2026-07-07T15:59:59Z"),
            _event("3", "2026-07-07T16:00:00Z"),
        ]
    )

    h15 = list(iter_ndjson(sink.path_for("2026-07-07-15").read_text(encoding="utf-8").splitlines()))
    h16 = list(iter_ndjson(sink.path_for("2026-07-07-16").read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in h15] == ["1", "2"]
    assert [event.id for event in h16] == ["3"]


async def test_hour_bucket_follows_the_instant_not_the_written_offset(tmp_path: Path) -> None:
    # The buckets exist to line up with GH Archive's UTC hours, so the bucket has to follow
    # the instant rather than whatever offset the timestamp was written in. 23:30-05:00 is
    # 04:30Z the *next day* — same moment, different hour and different date. Without the
    # UTC conversion this files under 2026-07-07-23 and every reconciliation against the
    # archive reports a capture-rate hole that isn't there.
    sink = HourlyNdjsonSink(tmp_path)

    await sink([_event("1", "2026-07-07T23:30:00-05:00")])

    h04 = list(iter_ndjson(sink.path_for("2026-07-08-04").read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in h04] == ["1"]
    assert not sink.path_for("2026-07-07-23").exists()


async def test_appends_across_calls_without_truncating(tmp_path: Path) -> None:
    sink = HourlyNdjsonSink(tmp_path)

    await sink([_event("1", "2026-07-07T15:10:00Z")])
    await sink([_event("2", "2026-07-07T15:20:00Z")])  # same hour, later call

    events = list(
        iter_ndjson(sink.path_for("2026-07-07-15").read_text(encoding="utf-8").splitlines())
    )
    assert [event.id for event in events] == ["1", "2"]  # appended, not overwritten


@respx.mock
async def test_poll_stream_writes_through_the_hourly_sink(
    settings: Settings, event_dict: dict[str, Any], tmp_path: Path
) -> None:
    # End-to-end: the sink satisfies EventSink and works as poll_stream's target.
    # event_dict's created_at is 2026-07-07T15:00:00Z → the 15:00 hour file.
    respx.get(EVENTS_URL).mock(return_value=httpx.Response(200, json=[{**event_dict, "id": "z"}]))
    sink = HourlyNdjsonSink(tmp_path)

    await poll_stream(settings, sink, interval_s=0.0, pages=1, max_cycles=1)

    written = list(
        iter_ndjson(sink.path_for("2026-07-07-15").read_text(encoding="utf-8").splitlines())
    )
    assert [event.id for event in written] == ["z"]


async def test_tees_a_batch_to_every_sink_in_order() -> None:
    primary, first, second = RecordingSink(), RecordingSink(), RecordingSink()
    tee = TeeSink(primary, first, second)

    batch = [_event("1", "2026-07-07T15:00:00Z")]
    await tee(batch)

    # every sink saw the batch, and nothing was dropped
    assert primary.batches == first.batches == second.batches == [batch]
    assert tee.dropped == 0


async def test_a_primary_failure_is_fatal_and_never_touches_the_stream() -> None:
    # The primary is the reconciliation record: if it cannot be written the run
    # must stop, and the stream must not be fed a batch the record does not have.
    primary = RecordingSink(fail=True)
    stream = RecordingSink()
    tee = TeeSink(primary, stream)

    with pytest.raises(RuntimeError, match="sink boom"):
        await tee([_event("1", "2026-07-07T15:00:00Z")])

    assert stream.batches == []  # never reached — primary comes first and failed
    assert tee.dropped == 0  # a primary failure is not a "drop", it is fatal


async def test_a_best_effort_failure_is_logged_counted_and_survived(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A stream failure must not stop capture: the record is already written, and
    # the archive reconciles anything the stream misses. It is loud and counted,
    # never silent, and a later best-effort sink still runs.
    primary, failing, healthy = RecordingSink(), RecordingSink(fail=True), RecordingSink()
    tee = TeeSink(primary, failing, healthy)

    batch = [_event("1", "2026-07-07T15:00:00Z")]
    with caplog.at_level(logging.WARNING, logger="reporadar.ingest.sinks"):
        await tee(batch)  # must not raise

    assert primary.batches == [batch]  # the record was written
    assert healthy.batches == [batch]  # one failure did not skip the rest
    assert tee.dropped == 1
    assert any("dropped a batch" in message for message in caplog.messages)


async def test_dropped_accumulates_across_batches() -> None:
    tee = TeeSink(RecordingSink(), RecordingSink(fail=True))

    for event_id in ("1", "2", "3"):
        await tee([_event(event_id, "2026-07-07T15:00:00Z")])

    assert tee.dropped == 3
