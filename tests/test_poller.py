from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from conftest import TEST_API_BASE
from reporadar.config import Settings
from reporadar.github.client import GitHubClient
from reporadar.github.events import iter_ndjson
from reporadar.ingest import poller
from reporadar.ingest.poller import collect_sample, poll_once

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
    """Record what the code asks to sleep for, without actually waiting.

    Sleep durations are a behaviour worth asserting on — a cap that is never
    applied and a pause that never ends look identical from the outside — and
    they are unassertable while every fixture sleeps for zero.
    """
    recorded: list[float] = []
    real_sleep = asyncio.sleep

    async def recording_sleep(delay: float) -> None:
        recorded.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", recording_sleep)
    return recorded


@pytest.fixture()
def non_utc_local_clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin local time away from UTC for the duration of a test.

    Without this, a naive-clock bug is invisible on any machine configured for
    UTC — which is most CI runners. The test would pass everywhere and protect
    only the developers whose laptops happen to disagree with the server.
    """
    monkeypatch.setenv("TZ", "America/Chicago")  # UTC-5/-6, never UTC
    time.tzset()
    try:
        yield
    finally:
        monkeypatch.undo()
        time.tzset()


def _pin_clock(monkeypatch: pytest.MonkeyPatch, stamps: Iterator[datetime]) -> None:
    """Drive collect_sample's filename clock from ``stamps``.

    The sample filename comes from the wall clock at second resolution, so
    whether two runs collide is a property of how fast the machine is. That is
    not something a test may leave to chance in either direction — a collision
    test that passes only on quick hardware is as useless as a no-collision test
    that fails on it.
    """

    class _Clock:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return next(stamps)

    monkeypatch.setattr(poller, "datetime", _Clock)


@pytest.fixture()
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every run starts at the same instant, so every run wants the same file."""
    fixed = datetime(2026, 7, 21, 12, 0, 0, tzinfo=UTC)
    _pin_clock(monkeypatch, iter(lambda: fixed, None))


@pytest.fixture()
def ticking_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each run starts a second after the last, so each gets its own file."""
    base = datetime(2026, 7, 21, 12, 0, 0, tzinfo=UTC)
    _pin_clock(monkeypatch, (base.replace(second=n) for n in range(60)))


@respx.mock
async def test_poll_once_sweeps_pages_and_dedupes(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # Consecutive pages overlap by design; the sweep must keep first-seen order.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    async with GitHubClient(settings) as client:
        events = await poll_once(client, pages=2, per_page=2)

    assert [event.id for event in events] == ["a", "b", "c"]


@respx.mock
async def test_collect_sample_writes_ndjson_deduped_across_cycles(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    out = await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    assert out.parent == settings.live_dir  # lands where config says samples live
    assert out.name.startswith("events_") and out.name.endswith(".ndjson")
    # Read the file back through the system's own parser: producer/consumer contract.
    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a", "b", "c"]


@respx.mock
async def test_collect_sample_bounded_window_can_reemit(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # A window of 1 keeps only the most recent id: once "b" arrives it evicts
    # "a", so "a" reappearing later is written again. This is the documented,
    # bounded cost of capped-memory dedup — the archive is the completeness
    # arbiter, not this window.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a"), _page(event_dict, "b"), _page(event_dict, "a")]
    )
    out = await collect_sample(settings, cycles=3, interval_s=0.0, pages=1, seen_window=1)

    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a", "b", "a"]


@respx.mock
async def test_collect_sample_logs_run_summary(
    settings: Settings, event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # Overlapping pages across two cycles: 4 fetched, 3 fresh (a,b,c), 1 duplicate.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    with caplog.at_level(logging.INFO, logger="reporadar.ingest.poller"):
        await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    # A long-running poller says what it did. Structured, grep-able fields.
    assert "poll sample complete" in caplog.text
    assert "cycles=2" in caplog.text
    assert "fetched=4" in caplog.text
    assert "fresh=3" in caplog.text
    assert "duplicates=1" in caplog.text
    assert "rate_limited=0" in caplog.text


@respx.mock
async def test_collect_sample_survives_rate_limiting(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    respx.get(EVENTS_URL).mock(side_effect=[_throttled("0"), _page(event_dict, "a")])
    out = await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    # The rate-limited cycle is a gap in the sample, not a crash of the sampler.
    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a"]


@respx.mock
async def test_a_throttled_cycle_is_counted_in_the_summary(
    settings: Settings, event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # Surviving throttling silently would be worse than crashing: the sample has
    # a hole in it, and the count is the only record that the hole is expected
    # rather than a collection failure. Capture rate is computed against this.
    respx.get(EVENTS_URL).mock(side_effect=[_throttled("0"), _page(event_dict, "a")])

    with caplog.at_level(logging.INFO, logger="reporadar.ingest.poller"):
        await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    assert "rate_limited=1" in caplog.text


@respx.mock
async def test_a_rate_limit_pause_is_capped(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # Retry-After arrives from outside and is trusted for the wait, so it needs a
    # ceiling: a day-long value — hostile, buggy, or just a long reset window —
    # would otherwise park the sampler with no way to tell it apart from a hang.
    respx.get(EVENTS_URL).mock(side_effect=[_throttled("86400"), _page(event_dict, "a")])

    await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    assert slept == [120.0]  # the cap, not the 86400 the server asked for


@respx.mock
async def test_the_last_cycle_does_not_sleep_before_returning(
    settings: Settings, event_dict: dict[str, Any], slept: list[float]
) -> None:
    # The interval separates cycles; after the last one there is nothing to
    # separate. Sleeping anyway makes every bounded sample take one interval
    # longer than it needs to, for no observable effect.
    respx.get(EVENTS_URL).mock(side_effect=[_page(event_dict, "a"), _page(event_dict, "b")])

    await collect_sample(settings, cycles=2, interval_s=7.0, pages=1)

    assert slept == [7.0]  # between the two cycles, and nowhere else


@respx.mock
async def test_a_second_sample_run_reuses_the_directory(
    settings: Settings, event_dict: dict[str, Any], ticking_clock: None
) -> None:
    # Only the second run of a machine's life exercises this, and no test had
    # ever called collect_sample twice — so a first-run-only mkdir would have
    # passed the whole suite and failed the second time anyone sampled.
    # The clock has to advance: two runs inside one second are now refused, and
    # real time does not reliably tick between two sub-millisecond calls — which
    # is exactly why this test used to overwrite its own first sample in silence.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))

    first = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)
    second = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    assert first.parent == second.parent == settings.live_dir
    assert first != second  # distinct runs, distinct samples


@respx.mock
async def test_a_second_run_in_the_same_second_refuses_to_overwrite(
    settings: Settings, event_dict: dict[str, Any], frozen_clock: None
) -> None:
    # The filename resolves only to the second, so two runs starting inside one
    # second compete for one path. Under mode "w" the second silently destroyed
    # the first, and the directory-reuse test above was doing exactly that on
    # every run of the suite without anything noticing — a lost sample looks
    # just like a sample. Refusing is the whole fix: nothing is collected yet.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    first = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)
    survivor = first.read_bytes()

    with pytest.raises(FileExistsError):
        await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    assert first.read_bytes() == survivor  # the earlier sample is byte-untouched
    assert [path.name for path in settings.live_dir.iterdir()] == [first.name]


@respx.mock
async def test_the_refusal_names_the_file_and_the_cause(
    settings: Settings, event_dict: dict[str, Any], frozen_clock: None
) -> None:
    # The operator sees this message and nothing else. "File exists" alone invites
    # deleting the file — which is the earlier sample, and the thing being saved.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    out = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    with pytest.raises(FileExistsError) as caught:
        await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    assert out.name in str(caught.value)
    assert "same second" in str(caught.value)  # says why, so retrying is obvious


@respx.mock
async def test_the_sample_filename_is_a_sortable_utc_stamp(
    settings: Settings, event_dict: dict[str, Any], non_utc_local_clock: None
) -> None:
    # The trailing Z is a claim about the clock, and a naive local stamp would
    # still end in Z while naming a moment that never happened in UTC. It would
    # also break the ordering the layout depends on: these files are read back
    # by name, so lexicographic order has to equal chronological order — which
    # %Y%m%d gives and %d%m%Y does not.
    respx.get(EVENTS_URL).mock(return_value=_page(event_dict, "a"))
    before = datetime.now(tz=UTC).replace(microsecond=0)

    out = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    after = datetime.now(tz=UTC)
    stamp = out.name.removeprefix("events_").removesuffix(".ndjson")
    written_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    assert before <= written_at <= after


@respx.mock
async def test_non_ascii_repository_names_survive_the_sample_file(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # Every other fixture here is pure ASCII; GH Archive is emphatically not, and
    # the serializer emits these characters raw rather than escaping them. So the
    # writer, the file's encoding and the parser all have to agree — this is the
    # only test where they are asked to.
    name = "ünïcode/repo-名前-🚀"
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            200, json=[{**event_dict, "repo": {**event_dict["repo"], "name": name}}]
        )
    )

    out = await collect_sample(settings, cycles=1, interval_s=0.0, pages=1)

    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.repo.name for event in events] == [name]
