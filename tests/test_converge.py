"""The convergence loop.

The loop has no interesting arithmetic; what it has is a set of promises about
*what it will not do* — attempt an hour that has not closed, run more downloads
at once than it was told to, or issue two overlapping operations on one
connection. Each of those fails silently or intermittently in production and
never in a naive test, so each gets a test that can only pass if the promise
holds.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from reporadar.ingest import converge as converge_module
from reporadar.ingest.converge import (
    DEFAULT_LOOKBACK_DAYS,
    converge_forever,
    converge_once,
)
from reporadar.ingest.hour import HourReport
from reporadar.ingest.ledger import HourStatus

DAY = date(2026, 7, 22)
NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)


class FakeConnection:
    """Returns canned outstanding hours, and notices overlapping operations.

    ``max_inflight`` is the assertion that matters: every call yields to the
    event loop halfway through, so two unserialised callers *will* overlap and
    the count *will* exceed one. A double that did not yield could not tell the
    safe arrangement from the unsafe one.
    """

    def __init__(self, hours: list[tuple[date, int]] | None = None) -> None:
        self.rows = [[day, hour] for day, hour in (hours or [])]
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []
        self.inflight = 0
        self.max_inflight = 0

    async def _busy(self) -> None:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0)
        self.inflight -= 1

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))
        await self._busy()

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.fetched.append((query, args))
        await self._busy()
        return self.rows


class FakeIngest:
    """Stands in for ``ingest_hour``, recording calls and returning canned outcomes."""

    def __init__(self, outcome: HourStatus | None = HourStatus.INGESTED, events: int = 10) -> None:
        self.calls: list[tuple[date, int]] = []
        self.inflight = 0
        self.max_inflight = 0
        self.outcome = outcome
        self.events = events

    async def __call__(self, day: date, hour: int, **kwargs: Any) -> HourReport:
        self.calls.append((day, hour))
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        # Touch the connection the loop handed us, twice around a yield: this is
        # what makes the serialisation observable rather than assumed.
        connection = kwargs["connection"]
        await connection.execute("-- ledger write")
        await asyncio.sleep(0)
        self.inflight -= 1
        events = self.events if self.outcome is HourStatus.INGESTED else None
        return HourReport(day, hour, self.outcome, "fake", events=events)


@pytest.fixture
def ingest(monkeypatch: pytest.MonkeyPatch) -> FakeIngest:
    fake = FakeIngest()
    monkeypatch.setattr(converge_module, "ingest_hour", fake)
    return fake


async def _once(connection: FakeConnection, tmp_path: Path, **kwargs: Any) -> Any:
    return await converge_once(
        connection,
        archive_dir=tmp_path / "raw",
        lake_dir=tmp_path / "lake",
        now=NOW,
        first_day=DAY,
        last_day=DAY,
        **kwargs,
    )


async def _bounded(coro: Any) -> Any:
    """Run a loop test under a hard time limit.

    The loop's termination conditions are what these tests exist to pin, and a
    broken one does not fail — it spins forever. Unbounded, that is a CI job
    that dies on the runner's own timeout hours later with no useful output;
    bounded, it is a failed assertion in five seconds naming the test. Proven
    against the real thing: removing the pass counter turns this from a hang
    into a failure.
    """
    return await asyncio.wait_for(coro, timeout=5)


async def test_a_pass_ingests_every_outstanding_closed_hour(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    connection = FakeConnection([(DAY, 1), (DAY, 2), (DAY, 3)])

    counters = await _once(connection, tmp_path)

    assert ingest.calls == [(DAY, 1), (DAY, 2), (DAY, 3)]
    assert counters.passes == 1
    assert counters.due == 3
    assert counters.ingested == 3
    assert counters.events == 30


async def test_an_hour_that_has_not_closed_is_never_attempted(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    """The scan honestly reports today's hours as unsettled; most are not ingestible.

    Filed as a whole hour, a partial one is unrecoverable without noticing, so
    the boundary is enforced here rather than trusted to the caller's range.
    """
    now = datetime(2026, 7, 22, 5, 30, tzinfo=UTC)  # hour 5 is still running
    connection = FakeConnection([(DAY, 3), (DAY, 4), (DAY, 5), (DAY, 6)])

    counters = await converge_once(
        connection,
        archive_dir=tmp_path / "raw",
        lake_dir=tmp_path / "lake",
        now=now,
        first_day=DAY,
        last_day=DAY,
    )

    assert ingest.calls == [(DAY, 3), (DAY, 4)]  # 5 is open, 6 has not started
    assert counters.due == 2


async def test_no_two_ledger_operations_ever_overlap(tmp_path: Path, ingest: FakeIngest) -> None:
    """One connection, many hours: overlapping operations are what drivers refuse."""
    connection = FakeConnection([(DAY, hour) for hour in range(12)])

    await _once(connection, tmp_path, concurrency=6)

    assert connection.max_inflight == 1


async def test_downloads_are_bounded_by_the_concurrency_asked_for(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    connection = FakeConnection([(DAY, hour) for hour in range(12)])

    await _once(connection, tmp_path, concurrency=3)

    assert ingest.max_inflight == 3


async def test_a_concurrency_below_one_is_refused(tmp_path: Path, ingest: FakeIngest) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await _once(FakeConnection([(DAY, 1)]), tmp_path, concurrency=0)


async def test_each_outcome_is_counted_as_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An hour left deliberately unrecorded is neither a success nor an error."""
    outcomes = iter(
        [HourStatus.INGESTED, HourStatus.MISSING, HourStatus.FAILED, None, HourStatus.INGESTED]
    )

    async def varied(day: date, hour: int, **kwargs: Any) -> HourReport:
        status = next(outcomes)
        events = 7 if status is HourStatus.INGESTED else None
        return HourReport(day, hour, status, "fake", events=events)

    monkeypatch.setattr(converge_module, "ingest_hour", varied)
    connection = FakeConnection([(DAY, hour) for hour in range(5)])

    counters = await _once(connection, tmp_path)

    assert (counters.ingested, counters.missing, counters.failed, counters.outstanding) == (
        2,
        1,
        1,
        1,
    )
    assert counters.events == 14


async def test_a_pass_with_nothing_outstanding_is_still_a_pass(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    """Otherwise a quiet loop is indistinguishable from a stopped one."""
    counters = await _once(FakeConnection([]), tmp_path)

    assert counters.passes == 1
    assert counters.due == 0
    assert ingest.calls == []


async def test_failures_are_left_alone_by_default_and_picked_up_on_request(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    """The whole difference between the always-on loop and an explicit range."""
    default = FakeConnection([])
    await _once(default, tmp_path)
    explicit = FakeConnection([])
    await _once(explicit, tmp_path, retry_failed=True)

    settled_by_default = default.fetched[0][1][2]
    settled_when_retrying = explicit.fetched[0][1][2]
    assert "failed" in settled_by_default  # skipped, so the loop cannot spin on it
    assert "failed" not in settled_when_retrying


async def test_the_loop_stops_after_the_passes_it_was_given(
    tmp_path: Path, ingest: FakeIngest, monkeypatch: pytest.MonkeyPatch
) -> None:
    slept: list[float] = []

    async def record_sleep(seconds: float, stop: asyncio.Event | None) -> None:
        slept.append(seconds)

    monkeypatch.setattr(converge_module, "interruptible_sleep", record_sleep)
    connection = FakeConnection([(DAY, 1)])

    counters = await _bounded(
        converge_forever(
            connection,
            archive_dir=tmp_path / "raw",
            lake_dir=tmp_path / "lake",
            max_passes=3,
            interval_s=42.0,
            clock=lambda: NOW,
        )
    )

    assert counters.passes == 3
    assert slept == [42.0, 42.0, 42.0]
    assert "CREATE TABLE IF NOT EXISTS archive_hours" in connection.executed[0][0]


async def test_a_stop_ends_the_loop_before_the_next_pass(
    tmp_path: Path, ingest: FakeIngest, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop = asyncio.Event()

    async def stop_after_first(seconds: float, event: asyncio.Event | None) -> None:
        stop.set()

    monkeypatch.setattr(converge_module, "interruptible_sleep", stop_after_first)

    counters = await _bounded(
        converge_forever(
            FakeConnection([(DAY, 1)]),
            archive_dir=tmp_path / "raw",
            lake_dir=tmp_path / "lake",
            stop=stop,
            clock=lambda: NOW,
        )
    )

    assert counters.passes == 1


async def test_the_scan_window_ends_today_and_covers_the_lookback(
    tmp_path: Path, ingest: FakeIngest, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sleep(seconds: float, stop: asyncio.Event | None) -> None:
        return None

    monkeypatch.setattr(converge_module, "interruptible_sleep", no_sleep)
    connection = FakeConnection([])

    await _bounded(
        converge_forever(
            connection,
            archive_dir=tmp_path / "raw",
            lake_dir=tmp_path / "lake",
            max_passes=1,
            lookback_days=DEFAULT_LOOKBACK_DAYS,
            clock=lambda: NOW,
        )
    )

    _, args = connection.fetched[0]
    assert args[1] == NOW.date()
    assert args[0] == NOW.date() - timedelta(days=DEFAULT_LOOKBACK_DAYS - 1)


async def test_a_lookback_below_one_day_is_refused(tmp_path: Path, ingest: FakeIngest) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await _bounded(
            converge_forever(
                FakeConnection([]),
                archive_dir=tmp_path / "raw",
                lake_dir=tmp_path / "lake",
                lookback_days=0,
            )
        )
