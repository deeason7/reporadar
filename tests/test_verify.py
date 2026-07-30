"""Verifying the hours ledger against the lake it describes.

The ledger side is a double, because what matters is which *combinations* of row
and file are possible and a double reaches all of them. The lake side is real
Parquet written by ``write_hour``, because the count check is a DuckDB query over
the partition columns stored inside each file — against a stub of that query the
test would pass whether or not the query is correct, which is the one thing worth
knowing here.
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir, write_hour
from reporadar.ingest.ledger import INGESTED_HOURS, HourStatus, ingested_hours
from reporadar.ingest.verify import Problem, verify_lake

DAY = date(2026, 7, 22)


class FakeConnection:
    """An asyncpg-shaped double replaying canned ledger rows."""

    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.fetched: list[str] = []
        self._rows = rows or []

    async def execute(self, query: str, *args: Any) -> None:
        return None

    async def fetch(self, query: str, *args: Any) -> list[list[Any]]:
        self.fetched.append(query)
        return self._rows


def _hour_in_lake(lake: Path, day: date, hour: int, events: int) -> int:
    """Write a real Parquet partition for ``day``/``hour``; return its byte size."""
    src = lake.parent / f"src-{day}-{hour}.json.gz"
    with gzip.open(src, "wt", encoding="utf-8") as fh:
        for i in range(events):
            fh.write(
                json.dumps(
                    {
                        "id": f"{hour}-{i}",
                        "type": "PushEvent",
                        "actor": {"id": 1, "login": "octocat"},
                        "repo": {"id": 2, "name": "octocat/hello"},
                        "org": None,
                        "payload": {"ref": "refs/heads/main"},
                        "public": True,
                        "created_at": f"{day}T{hour:02d}:30:00",
                    }
                )
                + "\n"
            )
    return write_hour(src, lake, day, hour).bytes_written


async def test_a_recorded_hour_backed_by_its_file_agrees(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    size = _hour_in_lake(lake, DAY, 22, events=3)
    connection = FakeConnection([[DAY, 22, 3, size]])

    report = await verify_lake(connection, lake_dir=lake)

    assert report.ok
    assert (report.claimed, report.agreed) == (1, 1)
    assert report.findings == []


async def test_a_recorded_hour_with_no_file_is_unbacked(tmp_path: Path) -> None:
    # The dangerous direction, and the reason this command exists: nothing ever
    # revisits a settled hour, so a claim with no file is a permanent hole that
    # every coverage number reports as complete.
    lake = tmp_path / "lake"
    connection = FakeConnection([[DAY, 22, 157_856, 15_853_763]])

    report = await verify_lake(connection, lake_dir=lake)

    assert not report.ok
    assert report.agreed == 0
    assert [(f.hour, f.problem) for f in report.findings] == [(22, Problem.ABSENT)]
    assert report.findings[0].unbacked
    assert "157,856" in report.findings[0].detail  # what was claimed, so the log is diagnostic


async def test_a_file_whose_size_contradicts_the_record_is_unbacked(tmp_path: Path) -> None:
    # Existence alone would pass this. The ledger records the byte count, so
    # comparing it costs one stat and catches a truncated or replaced file — the
    # case where something is there and is not what was recorded.
    lake = tmp_path / "lake"
    size = _hour_in_lake(lake, DAY, 22, events=3)
    connection = FakeConnection([[DAY, 22, 3, size + 1]])

    report = await verify_lake(connection, lake_dir=lake)

    assert not report.ok
    assert [f.problem for f in report.findings] == [Problem.SIZE]


async def test_a_row_with_no_recorded_size_is_checked_for_presence_only(tmp_path: Path) -> None:
    # The table requires an ingested hour to carry its event count and does not
    # require its size, so a null size must mean "cannot size-check" rather than
    # "size zero" — which would fail every such row.
    lake = tmp_path / "lake"
    _hour_in_lake(lake, DAY, 22, events=3)
    connection = FakeConnection([[DAY, 22, 3, None]])

    report = await verify_lake(connection, lake_dir=lake)

    assert report.ok
    assert (report.agreed, report.unsized) == (1, 1)


async def test_a_file_no_row_claims_is_surplus_and_does_not_fail(tmp_path: Path) -> None:
    # The asymmetry that defines the exit code. A file nothing claims misreports
    # nothing — the next scan simply converts that hour again — while a claim with
    # no file is a number that lies. Reporting both but failing on only one is the
    # whole design, so it needs a test rather than a comment.
    lake = tmp_path / "lake"
    _hour_in_lake(lake, DAY, 21, events=2)
    size = _hour_in_lake(lake, DAY, 22, events=3)
    connection = FakeConnection([[DAY, 22, 3, size]])

    report = await verify_lake(connection, lake_dir=lake)

    assert report.ok  # surplus does not fail
    assert report.agreed == 1
    assert [(f.hour, f.problem) for f in report.findings] == [(21, Problem.UNRECORDED)]
    assert not report.findings[0].unbacked
    assert report.as_dict()["surplus"] == 1
    assert report.as_dict()["unbacked"] == 0


async def test_the_lake_is_not_read_unless_counts_are_asked_for(tmp_path: Path) -> None:
    # Presence and size are two stats; counting rows scans every partition. A lake
    # of a decade is the case that makes the difference matter, so the default must
    # not read files. Proven by pointing the record at a file whose *contents*
    # disagree while its size does not: only a run that reads it can notice.
    lake = tmp_path / "lake"
    size = _hour_in_lake(lake, DAY, 22, events=3)
    connection = FakeConnection([[DAY, 22, 999, size]])

    shallow = await verify_lake(connection, lake_dir=lake)
    assert shallow.ok  # the wrong count is invisible without reading
    assert shallow.counted is False

    deep = await verify_lake(connection, lake_dir=lake, check_counts=True)
    assert not deep.ok
    assert deep.counted is True
    assert [f.problem for f in deep.findings] == [Problem.COUNT]
    assert "999" in deep.findings[0].detail and "3" in deep.findings[0].detail


async def test_counts_come_from_the_hour_inside_the_file_not_its_path(tmp_path: Path) -> None:
    # The partition columns are stored inside each file precisely so a copied or
    # misfiled file cannot forget which hour it holds. Verifying against the path
    # would only confirm a path equals itself, so this moves a real file into
    # another hour's directory: its own dt/hr still say 22, so hour 21's claim has
    # no rows and is caught.
    lake = tmp_path / "lake"
    size = _hour_in_lake(lake, DAY, 22, events=3)
    misfiled = partition_dir(lake, DAY, 21)
    misfiled.mkdir(parents=True)
    (partition_dir(lake, DAY, 22) / PARQUET_FILENAME).rename(misfiled / PARQUET_FILENAME)
    connection = FakeConnection([[DAY, 21, 3, size]])

    report = await verify_lake(connection, lake_dir=lake, check_counts=True)

    assert not report.ok
    assert [f.problem for f in report.findings] == [Problem.COUNT]
    assert "holds no rows" in report.findings[0].detail


async def test_an_empty_lake_reports_every_claim_rather_than_failing(tmp_path: Path) -> None:
    # A glob matching nothing makes DuckDB raise (measured: duckdb.IOException),
    # so a fresh deployment would otherwise crash the check instead of reporting
    # that it has nothing. Asserted with counts on, which is the path that queries.
    connection = FakeConnection([[DAY, 22, 3, 100], [DAY, 23, 4, 200]])

    report = await verify_lake(connection, lake_dir=tmp_path / "nothing-here", check_counts=True)

    assert report.claimed == 2
    assert [f.problem for f in report.findings] == [Problem.ABSENT, Problem.ABSENT]


async def test_an_unparseable_partition_directory_is_skipped_not_fatal(tmp_path: Path) -> None:
    # The lake is a directory anything can be dropped into. One stray folder must
    # not stop a check over thousands of real hours.
    lake = tmp_path / "lake"
    size = _hour_in_lake(lake, DAY, 22, events=3)
    (lake / "dt=not-a-date" / "hr=22").mkdir(parents=True)
    (lake / "dt=not-a-date" / "hr=22" / PARQUET_FILENAME).write_bytes(b"junk")
    (lake / f"dt={DAY}" / "hr=nope").mkdir(parents=True)
    (lake / f"dt={DAY}" / "hr=nope" / PARQUET_FILENAME).write_bytes(b"junk")
    connection = FakeConnection([[DAY, 22, 3, size]])

    report = await verify_lake(connection, lake_dir=lake)

    assert report.ok
    assert report.findings == []  # neither stray directory became a surplus finding


async def test_one_hour_yields_at_most_one_finding(tmp_path: Path) -> None:
    # An absent file has no size and no rows. Reporting three findings for one
    # hour would make "unbacked hours" disagree with the number of hours that are
    # actually unbacked, which is the number the exit code and any panel report.
    lake = tmp_path / "lake"
    connection = FakeConnection([[DAY, 22, 3, 100]])

    report = await verify_lake(connection, lake_dir=lake, check_counts=True)

    assert len(report.findings) == 1
    assert len(report.unbacked) == 1
    assert report.as_dict()["unbacked"] == 1


@pytest.mark.parametrize("problem", [Problem.ABSENT, Problem.SIZE, Problem.COUNT])
def test_every_problem_that_contradicts_a_record_counts_as_unbacked(problem: Problem) -> None:
    # The exit code reads this set. If a new Problem were added and left out of
    # UNBACKED, verify would exit 0 on a broken lake — so the membership is pinned
    # here rather than inferred from whichever cases the tests above happen to hit.
    from reporadar.ingest.verify import Finding

    assert Finding(DAY, 22, problem, "").unbacked


def test_a_surplus_file_is_not_counted_as_unbacked() -> None:
    from reporadar.ingest.verify import Finding

    assert not Finding(DAY, 22, Problem.UNRECORDED, "").unbacked


def test_the_query_asks_only_for_hours_claimed_to_be_in_the_lake() -> None:
    # The double replays canned rows and never parses the SQL, so nothing above
    # can tell whether the query filters at all. Without the status predicate a
    # `missing` hour — one the publisher does not have and never will — would be
    # reported as an absent lake file, turning an expected outcome into a failure
    # on every run. Asserted on the text because that is the only instrument a
    # double leaves available; the real database exercises it in the live run.
    assert "status = 'ingested'" in INGESTED_HOURS
    assert "ORDER BY day, hour" in INGESTED_HOURS  # findings read chronologically


async def test_the_read_shape_carries_the_count_and_a_missing_size(tmp_path: Path) -> None:
    # bytes is nullable in the table and events is not, so the read must preserve
    # exactly that asymmetry: a null size has to arrive as None rather than 0,
    # or every row written before the column meant anything fails its size check.
    connection = FakeConnection([[DAY, 21, 5, None], [DAY, 22, 3, 900]])

    rows = await ingested_hours(connection)

    assert [(r.hour, r.events, r.bytes) for r in rows] == [(21, 5, None), (22, 3, 900)]
    assert all(r.status is HourStatus.INGESTED for r in rows)
