from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from typing import Any

import httpx
import pytest
import respx

from conftest import TEST_API_BASE
from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest import service as service_module
from reporadar.ingest.poller import effective_interval
from reporadar.ingest.service import MAX_RATE_LIMIT_PAUSE_S, poll_stream

EVENTS_URL = f"{TEST_API_BASE}/events"


def _page(event_dict: dict[str, Any], *ids: str) -> httpx.Response:
    """A /events page whose items differ from the fixture only by id."""
    return httpx.Response(200, json=[{**event_dict, "id": id_} for id_ in ids])


def _throttled(retry_after: str) -> httpx.Response:
    return httpx.Response(
        403,
        json={"message": "API rate limit exceeded"},
        headers={"X-RateLimit-Remaining": "0", "Retry-After": retry_after},
    )


@pytest.fixture()
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record what the loop asks to sleep for, without actually waiting.

    Patched at ``asyncio.sleep`` rather than at ``interruptible_sleep``: the
    interesting mutations are the ones that bypass the interruptible wrapper, and
    a double that sits above them would let the real sleep through — turning a
    failing assertion into a two-minute hang.
    """
    recorded: list[float] = []
    real_sleep = asyncio.sleep

    async def recording_sleep(delay: float) -> None:
        recorded.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", recording_sleep)
    return recorded


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

    # Bounded on purpose: with no max_cycles, a loop that stopped honouring the
    # stop event would run forever rather than fail, and a hanging suite gives a
    # far worse signal than a red one.
    async with asyncio.timeout(5):
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
    respx.get(EVENTS_URL).mock(side_effect=[_throttled("0"), _page(event_dict, "a")])
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

    assert "poll progress" in caplog.text  # a long-running loop reports as it goes
    assert "poll stream stopped" in caplog.text  # final summary on exit


@respx.mock
async def test_report_every_zero_disables_progress_logging(
    settings: Settings, event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # The signature documents 0 as "disabled", which makes it a supported value
    # rather than a mistake — and it is the one value that reaches a modulo, so
    # anything short of an explicit guard is a crash on a documented input.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    sink = _CollectSink()

    with caplog.at_level(logging.INFO, logger="reporadar.ingest.service"):
        await poll_stream(settings, sink, interval_s=0.0, pages=1, report_every=0, max_cycles=1)

    assert "poll progress" not in caplog.text
    assert "poll stream stopped" in caplog.text  # the closing summary is not optional


@respx.mock
async def test_a_cycle_with_no_fresh_events_leaves_the_sink_alone(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # The same page twice: the second cycle is entirely duplicates. The sink's
    # contract is fresh events, and an empty hand-off is not a smaller version of
    # that — it is work for whatever sits behind the seam (a transaction, a
    # request, a file) in exchange for nothing.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    handed_over: list[int] = []

    async def sink(events: Sequence[RawEvent]) -> None:
        handed_over.append(len(events))

    counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, max_cycles=2)

    assert handed_over == [1]  # cycle 2 polled, deduped to nothing, and said nothing
    assert counters.cycles == 2  # the quiet cycle still happened, and still counts


@respx.mock
async def test_a_bounded_run_waits_between_cycles_and_not_after_the_last(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # The interval spaces one cycle from the next, so a run of n cycles owes n-1
    # waits. Sleeping after the last one waits for a cycle that will never run,
    # and the caller cannot tell that from a hang — see the single-cycle case
    # below for how bad that gets. The exact list is asserted rather than its
    # length: a fix that skipped the *first* wait instead would keep the count.
    respx.get(EVENTS_URL).mock(side_effect=[_page(event_dict, "a"), _page(event_dict, "b")])

    counters = await poll_stream(settings, _CollectSink(), interval_s=7.0, pages=1, max_cycles=2)

    assert counters.cycles == 2
    assert slept == [7.0]


@respx.mock
async def test_a_single_cycle_run_returns_without_waiting_at_all(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # The case that makes it a defect rather than an inefficiency: bounded at one
    # cycle, every wait the run performs is a wait for nothing. The work finishes
    # immediately and the process then sits for a full interval before returning,
    # which is indistinguishable from a stuck poll to anyone watching it.
    respx.get(EVENTS_URL).mock(side_effect=[_page(event_dict, "a")])

    await poll_stream(settings, _CollectSink(), interval_s=60.0, pages=1, max_cycles=1)

    assert slept == []


@respx.mock
async def test_an_unbounded_run_still_waits_after_every_cycle(
    settings: Settings, event_dict: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The case that must not change, and the one that actually runs in production:
    # with no bound there is always a next cycle, so every cycle owes its wait.
    # Guarding the wait on the cycle bound must not leak into the unbounded path.
    #
    # Patched at interruptible_sleep rather than at asyncio.sleep, unlike the
    # bounded tests above: a run with a stop event waits on the event with a
    # timeout instead of sleeping, so asyncio.sleep is never reached and a double
    # placed there would record nothing while the loop ran forever.
    waited: list[float] = []
    stop = asyncio.Event()

    async def record_and_stop_after_two(seconds: float, event: asyncio.Event | None) -> None:
        waited.append(seconds)
        if len(waited) == 2:
            stop.set()

    monkeypatch.setattr(service_module, "interruptible_sleep", record_and_stop_after_two)
    respx.get(EVENTS_URL).mock(side_effect=lambda request: _page(event_dict, "a"))

    async with asyncio.timeout(5):
        counters = await poll_stream(settings, _CollectSink(), interval_s=3.0, pages=1, stop=stop)

    assert counters.cycles == 2
    assert waited == [3.0, 3.0]  # one wait per cycle, including the last


@respx.mock
async def test_a_rate_limit_pause_is_capped(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # Retry-After comes from outside and is trusted for the wait, so it needs a
    # ceiling: a day-long value would park the service somewhere indistinguishable
    # from a hang, and an always-on ingester is exactly where that goes unnoticed.
    respx.get(EVENTS_URL).mock(side_effect=[_throttled("86400"), _page(event_dict, "a")])
    sink = _CollectSink()

    await poll_stream(settings, sink, interval_s=0.0, pages=1, max_cycles=2)

    assert max(slept) == MAX_RATE_LIMIT_PAUSE_S  # capped, not the 86400 asked for
    assert MAX_RATE_LIMIT_PAUSE_S <= 300.0  # and the cap itself is a sane wait


@respx.mock
async def test_a_stop_cuts_short_a_rate_limit_pause(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # A throttled service is still a service being asked to shut down. Without an
    # interruptible pause, SIGTERM during a long back-off is ignored until the
    # back-off ends, and the container runtime resorts to SIGKILL instead.
    stop = asyncio.Event()

    def throttle_then_request_stop(request: httpx.Request) -> httpx.Response:
        stop.set()  # the signal lands while the pause is being served
        return _throttled("30")

    respx.get(EVENTS_URL).mock(side_effect=throttle_then_request_stop)
    sink = _CollectSink()

    async with asyncio.timeout(5):  # an uninterruptible pause would take 30s
        counters = await poll_stream(settings, sink, interval_s=0.0, pages=1, stop=stop)

    assert counters.rate_limited == 1
    assert counters.cycles == 1  # a throttled cycle still counts: the run did look


def test_the_servers_cadence_overrides_a_faster_configured_one() -> None:
    # The defect this fixes: `serve` defaulted to 10s while GitHub asks for 60 on
    # every response. Polling faster cannot surface more events — the endpoint is
    # cached for longer than that — it only spends quota re-reading a page we
    # already hold and books the result as duplicates, which then reads as a
    # property of the feed rather than of our own cadence.
    assert effective_interval(10.0, 60.0) == 60.0


def test_a_slower_configured_interval_is_left_alone() -> None:
    # The server states a *minimum*. An operator who deliberately polls gently
    # must not be sped up to it.
    assert effective_interval(300.0, 60.0) == 300.0


def test_without_server_guidance_the_configured_interval_stands() -> None:
    # Before the first response, and against any endpoint that states nothing,
    # there is no guidance to honour — inventing a default here would be a
    # number nothing measured.
    assert effective_interval(10.0, None) == 10.0


# See tests/test_events.py for provenance: this is a real item's envelope, not a
# constructed one. `repo` is blanked upstream when a repository goes private.
_REDACTED = {
    "id": "12166351577",
    "type": "ForkEvent",
    "actor": {"id": 9384210, "login": "octo-forker"},
    "repo": {},
    "public": False,
    "created_at": "2026-07-22T23:14:07Z",
    "payload": {"forkee": {"private": True}},
}


@respx.mock
@pytest.mark.asyncio
async def test_one_redacted_event_does_not_end_the_run(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # This is the regression that matters. The page was built in a list
    # comprehension, so a single item the envelope refused raised out of the sweep
    # and ended an always-on service -- at the measured rate, about every 29 to 36
    # hours. The run must now continue, and the good events on the same page must
    # still arrive.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            200, json=[{**event_dict, "id": "1"}, _REDACTED, {**event_dict, "id": "2"}]
        )
    )
    batches: list[Sequence[RawEvent]] = []

    async def sink(events: Sequence[RawEvent]) -> None:
        batches.append(events)

    counters = await poll_stream(settings, sink, pages=1, max_cycles=1, interval_s=0)

    assert counters.cycles == 1
    assert [e.id for e in batches[0]] == ["1", "2"]
    assert counters.rejected == 1
    assert counters.fetched == 2  # the reject is not an event and is not counted as one


@respx.mock
@pytest.mark.asyncio
async def test_a_redacted_event_is_kept_not_dropped(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # Surviving the item is half of it. The capture is documented as the only copy,
    # so an item it refuses still has to land somewhere inside it -- silently
    # discarding one would make the counters overstate what the feed contained.
    respx.get(EVENTS_URL).mock(return_value=httpx.Response(200, json=[_REDACTED]))

    async def sink(events: Sequence[RawEvent]) -> None:  # pragma: no cover - never called
        raise AssertionError("no valid events on this page")

    await poll_stream(settings, sink, pages=1, max_cycles=1, interval_s=0)

    rejects = settings.live_dir / "rejects.ndjson"
    assert rejects.exists()
    written = json.loads(rejects.read_text(encoding="utf-8").strip())
    assert written["raw"] == _REDACTED
    assert "repo" in written["reason"]


@respx.mock
@pytest.mark.asyncio
async def test_a_clean_run_leaves_no_rejects_file(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # An empty file would have to be explained by whoever finds it, and "the feed
    # served nothing malformed" is the overwhelmingly common case.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "1"))

    async def sink(events: Sequence[RawEvent]) -> None:
        return None

    counters = await poll_stream(settings, sink, pages=1, max_cycles=1, interval_s=0)

    assert counters.rejected == 0
    assert not (settings.live_dir / "rejects.ndjson").exists()


@respx.mock
async def test_a_server_imposed_cadence_is_announced_once_not_every_cycle(
    settings: Settings,
    event_dict: dict[str, Any],
    slept: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two guards, one line apart, and both were unconstrained. The outer one makes
    # the notice fire only when the cadence CHANGES — inverted, an operator paced
    # at 60s gets a line every single cycle for the life of the process. The inner
    # one is what makes the message true: it may only claim the server is
    # "overriding the configured" value when the two actually differ.
    paced = {"X-Poll-Interval": "60"}
    respx.get(EVENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[{**event_dict, "id": i}], headers=paced)
            for i in ("a", "b", "c")
        ]
    )

    with caplog.at_level(logging.INFO):
        await poll_stream(settings, _CollectSink(), interval_s=10.0, pages=1, max_cycles=3)

    assert slept == [60.0, 60.0]  # the server's number won, both waits
    overrides = [r for r in caplog.records if "overriding the configured" in r.getMessage()]
    assert len(overrides) == 1, f"announced {len(overrides)} times, not once"
    assert "60" in overrides[0].getMessage()
    assert "10" in overrides[0].getMessage()  # both numbers, so the operator can act


@respx.mock
async def test_no_override_is_announced_when_the_server_agrees_with_us(
    settings: Settings,
    event_dict: dict[str, Any],
    slept: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The negative half. With the configured interval already at or above what the
    # server asks, nothing is being overridden and saying so would be false.
    respx.get(EVENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[{**event_dict, "id": i}], headers={"X-Poll-Interval": "5"})
            for i in ("a", "b")
        ]
    )

    with caplog.at_level(logging.INFO):
        await poll_stream(settings, _CollectSink(), interval_s=30.0, pages=1, max_cycles=2)

    assert slept == [30.0]  # ours was already slower, so ours stands
    assert not [r for r in caplog.records if "overriding the configured" in r.getMessage()]
