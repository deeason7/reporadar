from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import respx

from reporadar.config import Settings
from reporadar.github.events import RawEvent, iter_ndjson
from reporadar.ingest.service import poll_stream
from reporadar.ingest.sinks import HourlyNdjsonSink

EVENTS_URL = "https://api.github.com/events"


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
