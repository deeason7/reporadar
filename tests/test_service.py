from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

import httpx
import pytest
import respx

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.service import poll_stream

EVENTS_URL = "https://api.github.com/events"


def _page(event_dict: dict[str, Any], *ids: str) -> httpx.Response:
    """A /events page whose items differ from the fixture only by id."""
    return httpx.Response(200, json=[{**event_dict, "id": id_} for id_ in ids])


class _CollectSink:
    """An EventSink that records everything it is handed."""

    def __init__(self) -> None:
        self.events: list[RawEvent] = []

    async def __call__(self, events: Sequence[RawEvent]) -> None:
        self.events.extend(events)


@respx.mock
async def test_poll_stream_delivers_fresh_events_and_dedupes(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # Overlapping cycles: (a,b) then (b,c). The sink sees only fresh events, and
    # the counters account for the cross-cycle duplicate.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    sink = _CollectSink()

    counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, max_cycles=2)

    assert [event.id for event in sink.events] == ["a", "b", "c"]
    assert counters.cycles == 2
    assert counters.fetched == 4
    assert counters.fresh == 3
    assert counters.duplicates == 1


@respx.mock
async def test_poll_stream_does_not_poll_when_stop_already_set(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    route = respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    stop = asyncio.Event()
    stop.set()
    sink = _CollectSink()

    counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, stop=stop)

    assert counters.cycles == 0
    assert sink.events == []
    assert route.call_count == 0  # the loop broke before its first request


@respx.mock
async def test_poll_stream_stops_gracefully_mid_run(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a", "b"))
    stop = asyncio.Event()

    class StopAfterFirstBatch:
        def __init__(self) -> None:
            self.events: list[RawEvent] = []

        async def __call__(self, events: Sequence[RawEvent]) -> None:
            self.events.extend(events)
            stop.set()  # ask the loop to stop after this batch

    sink = StopAfterFirstBatch()
    counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, stop=stop)

    assert counters.cycles == 1
    assert [event.id for event in sink.events] == ["a", "b"]


@respx.mock
async def test_poll_stream_survives_rate_limiting(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    rate_limited = httpx.Response(
        403,
        json={"message": "API rate limit exceeded"},
        headers={"X-RateLimit-Remaining": "0", "Retry-After": "0"},
    )
    respx.get(EVENTS_URL).mock(side_effect=[rate_limited, _page(event_dict, "a")])
    sink = _CollectSink()

    counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, max_cycles=2)

    assert [event.id for event in sink.events] == ["a"]  # the throttled cycle is a gap, not a crash
    assert counters.rate_limited == 1
    assert counters.cycles == 2
    assert counters.fresh == 1


@respx.mock
async def test_poll_stream_logs_progress_and_exit(
    settings: Settings, event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    sink = _CollectSink()

    with caplog.at_level(logging.INFO, logger="reporadar.ingest.service"):
        await poll_stream(settings, sink, interval_s=0.0, pages=1, report_every=1, max_cycles=1)

    assert "poll progress" in caplog.text  # periodic report (Rule 10)
    assert "poll stream stopped" in caplog.text  # final summary on exit
