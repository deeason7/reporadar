"""Repairing hours the record claims and the lake does not hold.

This command is the only destructive one in the project, and destructive is the
easy part to get right. The properties worth testing are the ones that fail
*quietly*: an hour re-fetched while its false row still stands would be reported
as repaired and would not be, and a comparison that invents the missing half of
itself would report every hour as a disagreement of exactly its own size.

Everything here runs against doubles, so every outcome — an hour that comes back
different, an hour that does not come back, a row the database declines to remove
— is reachable on demand. What a double cannot show is that the SQL parses or
that the upsert really refuses to downgrade a success; that is the integration
test in ``test_ledger.py``, and neither substitutes for the other.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from reporadar.ingest import repair as repair_module
from reporadar.ingest.hour import HourReport
from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir
from reporadar.ingest.ledger import HourRecord, HourStatus
from reporadar.ingest.repair import INCOMPLETE_EXIT_CODE, repair_unbacked
from reporadar.ingest.verify import Finding, Problem

DAY = date(2026, 7, 28)
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


class FakeConnection:
    """Answers the two queries the repair issues, and notices overlapping calls.

    ``deleted`` is settable independently of what was asked for, which is the
    only way to reach the case this module treats as most dangerous: the database
    declining to remove a row the caller believed it would.
    """

    def __init__(
        self,
        claims: list[tuple[date, int, int, int | None]],
        deleted: list[tuple[date, int]] | None = None,
    ) -> None:
        self.claims = claims
        self._deleted = deleted
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.inflight = 0
        self.max_inflight = 0

    @property
    def deletes(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [call for call in self.fetched if "DELETE FROM archive_hours" in call[0]]

    async def _busy(self) -> None:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0)
        self.inflight -= 1

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))
        await self._busy()

    async def fetch(self, query: str, *args: Any) -> list[list[Any]]:
        self.fetched.append((query, args))
        await self._busy()
        if "DELETE FROM archive_hours" in query:
            # Absent an override, the database removed exactly what was asked for.
            if self._deleted is not None:
                return [[day, hour] for day, hour in self._deleted]
            days, hours = args[0], args[1]
            return [[day, hour] for day, hour in zip(days, hours, strict=True)]
        return [[day, hour, events, size] for day, hour, events, size in self.claims]


class FakeIngest:
    """Stands in for ``ingest_hour``, with a per-hour canned outcome."""

    def __init__(
        self,
        outcomes: dict[tuple[date, int], tuple[HourStatus | None, int | None]] | None = None,
        default: tuple[HourStatus | None, int | None] = (HourStatus.INGESTED, 3_921_223),
    ) -> None:
        self.calls: list[tuple[date, int]] = []
        self.kwargs: list[dict[str, Any]] = []
        self.inflight = 0
        self.max_inflight = 0
        self._outcomes = outcomes or {}
        self._default = default

    async def __call__(self, day: date, hour: int, **kwargs: Any) -> HourReport:
        self.calls.append((day, hour))
        self.kwargs.append(kwargs)
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        # Touch the shared connection around a yield, so serialisation is
        # observable rather than assumed — the same trick the loop's tests use.
        await kwargs["connection"].execute("-- ledger write")
        await asyncio.sleep(0)
        self.inflight -= 1
        status, events = self._outcomes.get((day, hour), self._default)
        return HourReport(day, hour, status, "fake", events=events)


@pytest.fixture
def ingest(monkeypatch: pytest.MonkeyPatch) -> FakeIngest:
    fake = FakeIngest()
    monkeypatch.setattr(repair_module, "ingest_hour", fake)
    return fake


def _back(lake: Path, day: date, hour: int, size: int = 10) -> None:
    """Put a file where the record says one is, so the hour verifies."""
    directory = partition_dir(lake, day, hour)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / PARQUET_FILENAME).write_bytes(b"x" * size)


async def _repair(connection: FakeConnection, tmp_path: Path, **kwargs: Any) -> Any:
    return await repair_unbacked(
        connection,
        lake_dir=tmp_path / "lake",
        archive_dir=tmp_path / "raw",
        now=NOW,
        **kwargs,
    )


async def test_nothing_is_touched_when_every_claim_is_backed(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    _back(tmp_path / "lake", DAY, 5)
    connection = FakeConnection([(DAY, 5, 100, 10)])

    report = await _repair(connection, tmp_path)

    assert report.ok
    assert report.unbacked == []
    # The two that matter: a repair with nothing to repair must not delete and
    # must not fetch. A command that re-downloads a healthy lake is worse than one
    # that does nothing.
    assert connection.deletes == []
    assert ingest.calls == []


async def test_a_dry_run_reports_the_fault_and_changes_nothing(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    connection = FakeConnection([(DAY, 5, 100, 10)])  # no file written: unbacked

    report = await _repair(connection, tmp_path, dry_run=True)

    assert len(report.unbacked) == 1
    assert connection.deletes == []
    assert ingest.calls == []
    # Not ok, so the command exits non-zero and says what it would have done. A
    # dry run that reported success would be indistinguishable from a healthy lake.
    assert not report.ok


async def test_an_unbacked_hour_is_cleared_and_fetched_again(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    connection = FakeConnection([(DAY, 5, 3_921_223, 10)])

    report = await _repair(connection, tmp_path)

    assert ingest.calls == [(DAY, 5)]
    assert len(report.recovered) == 1
    assert report.disagreed == []
    assert report.ok
    reconciliation = report.reconciliations[0]
    assert (reconciliation.claimed_events, reconciliation.actual_events) == (3_921_223, 3_921_223)
    assert reconciliation.verdict == "match"


async def test_the_delete_names_only_the_unbacked_hours(tmp_path: Path, ingest: FakeIngest) -> None:
    # Hour 5 is backed and hour 6 is not. A repair that cleared both would destroy
    # a true record to fix a false one.
    _back(tmp_path / "lake", DAY, 5)
    connection = FakeConnection([(DAY, 5, 100, 10), (DAY, 6, 200, 10)])

    await _repair(connection, tmp_path)

    _query, args = connection.deletes[0]
    assert (list(args[0]), list(args[1])) == ([DAY], [6])
    assert ingest.calls == [(DAY, 6)]


async def test_a_claim_that_does_not_reproduce_is_reported_rather_than_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The finding this command exists to preserve. A row claiming 100 events for an
    # hour that really holds 165,892 was a test fixture written into the real
    # ledger; a repair that quietly fixed it would have destroyed the only evidence
    # that it had ever been there.
    fake = FakeIngest(default=(HourStatus.INGESTED, 165_892))
    monkeypatch.setattr(repair_module, "ingest_hour", fake)
    connection = FakeConnection([(DAY, 5, 100, 10)])

    report = await _repair(connection, tmp_path)

    assert len(report.disagreed) == 1
    reconciliation = report.disagreed[0]
    assert (reconciliation.claimed_events, reconciliation.actual_events) == (100, 165_892)
    assert reconciliation.verdict == "DIFFERS"
    # Still a success: the hour is now correct and backed. The disagreement is a
    # finding about the record that was destroyed, not a failure of the repair.
    assert reconciliation.recovered
    assert report.ok


async def test_an_hour_whose_row_could_not_be_cleared_is_never_fetched(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    # The quiet-failure case, and the reason the delete happens first. The upsert
    # refuses to move a row off `ingested`, so re-fetching an hour whose false row
    # still stands would leave the lie in place while reporting the hour repaired.
    connection = FakeConnection([(DAY, 5, 100, 10), (DAY, 6, 200, 10)], deleted=[(DAY, 6)])

    report = await _repair(connection, tmp_path)

    assert report.unclearable == [(DAY, 5)]
    assert ingest.calls == [(DAY, 6)]
    assert not report.ok


async def test_an_hour_that_does_not_come_back_is_reported_unrecovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A dropped connection records nothing at all, which is the publisher
    # throttling a repair rather than a fault in the hour.
    fake = FakeIngest(default=(None, None))
    monkeypatch.setattr(repair_module, "ingest_hour", fake)
    connection = FakeConnection([(DAY, 5, 100, 10)])

    report = await _repair(connection, tmp_path)

    assert len(report.unrecovered) == 1
    assert report.reconciliations[0].verdict == "NOT RECOVERED"
    assert not report.ok


async def test_a_missing_hour_counts_as_unrecovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The publisher does not have it. The row is now honest — `missing` rather than
    # a false `ingested` — but the hour is not in the lake, so the repair did not
    # fully succeed and must not say it did.
    fake = FakeIngest(default=(HourStatus.MISSING, None))
    monkeypatch.setattr(repair_module, "ingest_hour", fake)
    connection = FakeConnection([(DAY, 5, 100, 10)])

    report = await _repair(connection, tmp_path)

    assert not report.ok
    assert report.reconciliations[0].outcome is HourStatus.MISSING


async def test_the_claimed_count_is_never_invented(
    tmp_path: Path, ingest: FakeIngest, monkeypatch: pytest.MonkeyPatch
) -> None:
    # If the claim vanished between reading it and reporting on it, the comparison
    # this command exists to print cannot be made. Defaulting to zero would report
    # the hour as a disagreement of exactly its own size — a false finding that
    # looks precise.
    connection = FakeConnection([(DAY, 5, 100, 10)])
    monkeypatch.setattr(repair_module, "_claimed_events", _raise_lookup)

    with pytest.raises(LookupError, match="changed underneath"):
        await _repair(connection, tmp_path)


def _raise_lookup(*_args: Any, **_kwargs: Any) -> int:
    raise LookupError("the ledger changed underneath this repair")


async def test_more_hours_are_never_fetched_at_once_than_allowed(
    tmp_path: Path, ingest: FakeIngest
) -> None:
    connection = FakeConnection([(DAY, hour, 100, 10) for hour in range(6)])

    await _repair(connection, tmp_path, concurrency=2)

    assert len(ingest.calls) == 6
    assert ingest.max_inflight <= 2
    # The connection is shared across those fetches, and a driver refuses two
    # overlapping operations on one. The wrapper is what makes that safe, and this
    # is what proves the wrapper is actually in the path.
    assert connection.max_inflight == 1


async def test_concurrency_below_one_is_refused(tmp_path: Path, ingest: FakeIngest) -> None:
    connection = FakeConnection([(DAY, 5, 100, 10)])

    with pytest.raises(ValueError, match="at least 1"):
        await _repair(connection, tmp_path, concurrency=0)


async def test_the_source_is_kept_by_default(tmp_path: Path, ingest: FakeIngest) -> None:
    # Opposite to the long-running commands, deliberately. A repair follows
    # something having gone wrong, and the downloaded source is the cheapest way
    # for an operator to look at what actually arrived.
    connection = FakeConnection([(DAY, 5, 100, 10)])

    await _repair(connection, tmp_path)

    assert ingest.kwargs[0]["keep_source"] is True


async def test_the_incomplete_exit_code_is_not_one_a_convention_already_claims() -> None:
    # 0, 1 and 2 mean success, error and usage to everything that runs this. A
    # wrapper branching on any of them would act on a crash or a typo.
    assert INCOMPLETE_EXIT_CODE not in (0, 1, 2)


# --------------------------------------------------------------------------- #
# What the pre-flight announcement counts
# --------------------------------------------------------------------------- #


def _record(day: date, hour: int, *, events: int | None, size: int | None) -> HourRecord:
    """A ledger row. Status follows the count, because the ledger refuses the other pairing.

    ``HourRecord`` rejects an ``INGESTED`` hour with no event count — the same rule the
    table's own CHECK constraint enforces — so a null count here necessarily belongs to
    an hour that was never ingested. That is exactly the state `_claimed_events` guards.
    """
    status = HourStatus.INGESTED if events is not None else HourStatus.FAILED
    return HourRecord(day=day, hour=hour, status=status, events=events, bytes=size)


def test_the_announcement_sizes_only_the_hours_being_repaired(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The membership test decides which claims are summed. Inverted, it reports the
    # size of every hour that is FINE — a number that looks authoritative, is the
    # wrong one, and is printed immediately before a destructive step.
    day = date(2026, 7, 22)
    claims = {
        (day, 1): _record(day, 1, events=10, size=5_000_000),  # unbacked
        (day, 2): _record(day, 2, events=20, size=90_000_000),  # healthy, must not count
    }
    unbacked = [Finding(day=day, hour=1, problem=Problem.ABSENT, detail="gone")]

    with caplog.at_level(logging.WARNING):
        repair_module._announce(unbacked, claims)

    assert "5.0 MB" in caplog.text
    assert "90" not in caplog.text  # the healthy hour's size never appears


def test_a_claim_with_no_recorded_size_contributes_zero_rather_than_failing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A row can legitimately carry a null size. The announcement is a courtesy
    # printed before the real work, so it must not be the thing that raises.
    day = date(2026, 7, 22)
    claims = {(day, 1): _record(day, 1, events=10, size=None)}
    unbacked = [Finding(day=day, hour=1, problem=Problem.ABSENT, detail="gone")]

    with caplog.at_level(logging.WARNING):
        repair_module._announce(unbacked, claims)

    assert "0.0 MB" in caplog.text


def test_a_lost_event_count_is_refused_rather_than_invented() -> None:
    """Both arms of the guard, because either alone leaves a fabricated zero reachable.

    A zero here reports every hour as disagreeing by exactly its own size — a false
    finding that looks precise, in the one number the command exists to print.
    """
    day = date(2026, 7, 22)

    with pytest.raises(LookupError, match="no recorded event count"):
        repair_module._claimed_events({}, day, 1)  # the row is gone entirely

    only_a_null_count = {(day, 1): _record(day, 1, events=None, size=1)}
    with pytest.raises(LookupError, match="no recorded event count"):
        repair_module._claimed_events(
            only_a_null_count, day, 1
        )  # the row is there, the count is not

    present = {(day, 1): _record(day, 1, events=42, size=1)}
    assert repair_module._claimed_events(present, day, 1) == 42
