"""Comparing the marts against the hours the lake actually holds.

The lake side is real directories, because "which hours are on disk" is the whole
question and a stubbed answer to it would pass whether or not the walk is right.
The marts side is a double, because what matters there is which *combinations* of
lake and mart are possible, and a double reaches all of them.

What that leaves unproven is the one SQL query this module issues. It is proven
in CI against a real database with real marts, in the only place that has both.
Named here rather than left implicit, because a suite that looks complete while
testing half a module is exactly the failure this project keeps finding.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir
from reporadar.marts.freshness import (
    MART_DAYS,
    MARTS_EXIST,
    DayDrift,
    Drift,
    lake_hours_by_day,
    marts_freshness,
)

DAY = date(2026, 7, 22)
NEXT = date(2026, 7, 23)


class FakeConnection:
    """An asyncpg-shaped double answering the module's two queries.

    Keyed by the query itself rather than by call order, so a test cannot pass
    because two queries happened to be issued in the sequence it expected.
    """

    def __init__(self, *, exists: bool, rows: list[list[Any]]) -> None:
        self.fetched: list[str] = []
        self._exists = exists
        self._rows = rows

    async def execute(self, query: str, *args: Any) -> None:
        return None

    async def fetch(self, query: str, *args: Any) -> list[list[Any]]:
        self.fetched.append(query)
        if query == MARTS_EXIST:
            return [[self._exists]]
        return self._rows


def _lake(root: Path, hours: dict[date, list[int]]) -> Path:
    """A lake directory holding the named partitions, as `write_hour` lays them out."""
    lake = root / "lake"
    for day, day_hours in hours.items():
        for hour in day_hours:
            partition = partition_dir(lake, day, hour)
            partition.mkdir(parents=True, exist_ok=True)
            (partition / PARQUET_FILENAME).write_bytes(b"")
    lake.mkdir(parents=True, exist_ok=True)
    return lake


async def test_days_built_from_every_hour_on_disk_are_current(tmp_path: Path) -> None:
    lake = _lake(tmp_path, {DAY: [21, 22, 23], NEXT: [3]})
    connection = FakeConnection(exists=True, rows=[[DAY, 3], [NEXT, 1]])

    report = await marts_freshness(connection, lake_dir=lake)

    assert report.ok
    assert report.drift == []
    assert report.as_dict() == {
        "built": True,
        "days": 2,
        "current": 2,
        "stale": 0,
        "surplus": 0,
        "hours_behind": 0,
    }


async def test_an_hour_added_since_the_last_build_is_stale(tmp_path: Path) -> None:
    # The failure the whole module exists for, and the one nothing could see
    # before: the marts are present, the dashboard renders, and the day is
    # reported from three of the four hours the lake now holds.
    lake = _lake(tmp_path, {DAY: [20, 21, 22, 23]})
    connection = FakeConnection(exists=True, rows=[[DAY, 3]])

    report = await marts_freshness(connection, lake_dir=lake)

    assert not report.ok
    assert [day.kind for day in report.drift] == [Drift.BEHIND]
    assert report.hours_behind == 1
    assert report.stale_days[0].day == DAY


async def test_a_day_absent_from_the_marts_is_stale_for_all_its_hours(tmp_path: Path) -> None:
    # A whole day missing, which is what an ingest that ran overnight leaves
    # behind. It counts for every hour it holds, not for one day.
    lake = _lake(tmp_path, {DAY: [21, 22], NEXT: [0, 1, 2]})
    connection = FakeConnection(exists=True, rows=[[DAY, 2]])

    report = await marts_freshness(connection, lake_dir=lake)

    assert not report.ok
    assert report.stale_days[0].kind is Drift.UNBUILT
    assert report.hours_behind == 3
    assert "no row for this day" in report.stale_days[0].detail


async def test_hours_the_lake_no_longer_holds_are_reported_not_failed(tmp_path: Path) -> None:
    # The asymmetry, and the reason the exit code is not "anything disagreed".
    # These marts were built from partitions since removed, so they describe
    # hours that are gone — real numbers about a lake that has shrunk. Nothing
    # published is understated, so it prints and the check still passes.
    lake = _lake(tmp_path, {DAY: [21, 22]})
    connection = FakeConnection(exists=True, rows=[[DAY, 5]])

    report = await marts_freshness(connection, lake_dir=lake)

    assert report.ok
    assert [day.kind for day in report.drift] == [Drift.SURPLUS]
    assert report.hours_behind == 0
    assert "removed since" in report.drift[0].detail


async def test_a_day_only_the_marts_know_about_is_surplus_rather_than_negative(
    tmp_path: Path,
) -> None:
    # The other arm of the union: a mart day with nothing on disk comes through
    # as zero lake hours. Reading that as "behind by minus five" would make
    # hours_behind nonsense, so it has to land as surplus.
    lake = _lake(tmp_path, {})
    connection = FakeConnection(exists=True, rows=[[DAY, 5]])

    report = await marts_freshness(connection, lake_dir=lake)

    assert report.ok
    assert report.drift[0].kind is Drift.SURPLUS
    assert report.hours_behind == 0


async def test_marts_that_have_never_been_built_report_every_lake_day_waiting(
    tmp_path: Path,
) -> None:
    # The first-run state. It must not be an exception, and it must not be a bare
    # "no marts" flag either: the useful answer is how much is waiting, which is
    # what makes the same wrapper work on a fresh clone and on a stale one.
    lake = _lake(tmp_path, {DAY: [21, 22], NEXT: [0]})
    connection = FakeConnection(exists=False, rows=[])

    report = await marts_freshness(connection, lake_dir=lake)

    assert not report.built
    assert not report.ok
    assert [day.kind for day in report.days] == [Drift.UNBUILT, Drift.UNBUILT]
    assert report.hours_behind == 3
    # The mart query is never issued, because there is nothing to read.
    assert connection.fetched == [MARTS_EXIST]


async def test_an_empty_lake_and_empty_marts_are_current_rather_than_stale(
    tmp_path: Path,
) -> None:
    # Nothing ingested and nothing built is not a problem to report, and a lake
    # directory that does not exist yet is the same fact as an empty one. A check
    # that fails on a fresh clone is a check somebody turns off on day one.
    connection = FakeConnection(exists=False, rows=[])

    report = await marts_freshness(connection, lake_dir=tmp_path / "never-created")

    assert report.ok
    assert report.as_dict()["days"] == 0


async def test_the_mart_query_is_issued_once_the_table_exists(tmp_path: Path) -> None:
    connection = FakeConnection(exists=True, rows=[[DAY, 1]])

    await marts_freshness(connection, lake_dir=_lake(tmp_path, {DAY: [21]}))

    assert connection.fetched == [MARTS_EXIST, MART_DAYS]


def test_the_lake_walk_counts_partitions_per_day(tmp_path: Path) -> None:
    # The denominator itself. It is the files rather than the hours ledger on
    # purpose: a build reads the lake, so an hour the ledger claims and the lake
    # does not hold is one no rebuild could ever supply, and counting it would
    # report a gap that no action closes.
    lake = _lake(tmp_path, {DAY: [21, 22, 23], NEXT: [0]})

    assert lake_hours_by_day(lake) == {DAY: 3, NEXT: 1}


def test_a_directory_with_no_parquet_file_is_not_counted(tmp_path: Path) -> None:
    # A partition directory is created before the file is written, so an ingest
    # interrupted at the wrong moment leaves one behind. Counting it would report
    # a day behind for an hour that is not there.
    lake = _lake(tmp_path, {DAY: [21]})
    partition_dir(lake, DAY, 22).mkdir(parents=True)

    assert lake_hours_by_day(lake) == {DAY: 1}


def test_the_four_states_are_pinned_where_the_report_reads_them() -> None:
    # DayDrift is what the report and the exit code both read, so its states are
    # asserted directly rather than only through a report that could be
    # summarising them wrongly.
    assert DayDrift(DAY, 3, 3).kind is Drift.CURRENT
    assert DayDrift(DAY, 3, 2).kind is Drift.BEHIND
    assert DayDrift(DAY, 3, 4).kind is Drift.SURPLUS
    assert DayDrift(DAY, 3, None).kind is Drift.UNBUILT
    assert DayDrift(DAY, 3, 2).stale
    assert not DayDrift(DAY, 3, 4).stale
