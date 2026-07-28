"""The archive → Parquet lake writer.

Fixtures are built adversarially rather than realistically: a realistic archive
hour exercises none of the cases that decide whether this module is correct (an
hour that straddles two hours, a payload key nobody listed, a null timestamp), so
the happy path is one test here and the rest are the awkward ones.
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from reporadar.ingest.lake import (
    PARQUET_FILENAME,
    PartitionMismatchError,
    partition_dir,
    write_hour,
)


def _event(event_id: str, created_at: str | None, **overrides: Any) -> dict[str, Any]:
    """One archive record, shaped like the published envelope."""
    event: dict[str, Any] = {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 2, "name": "octocat/hello"},
        "org": None,
        "payload": {"ref": "refs/heads/main", "size": 1},
        "public": True,
        "created_at": created_at,
    }
    event.update(overrides)
    return event


def _archive(tmp_path: Path, *events: dict[str, Any], name: str = "hour.json.gz") -> Path:
    """Write events as gzipped NDJSON, exactly as GH Archive publishes them."""
    path = tmp_path / name
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    return path


def _read(path: Path, sql: str) -> list[tuple[Any, ...]]:
    """Run ``sql`` against one Parquet file, bound as ``$f``."""
    con = duckdb.connect()
    try:
        rows = con.execute(sql, {"f": str(path)}).fetchall()
    finally:
        con.close()
    return [tuple(row) for row in rows]


def test_an_hour_lands_in_its_hive_partition(tmp_path: Path) -> None:
    src = _archive(
        tmp_path,
        _event("1", "2026-07-22T22:00:01"),
        _event("2", "2026-07-22T22:59:59"),
    )
    lake = tmp_path / "lake"

    report = write_hour(src, lake, date(2026, 7, 22), 22)

    assert report.path == lake / "dt=2026-07-22" / "hr=22" / PARQUET_FILENAME
    assert report.path.exists()
    assert report.events == 2
    assert report.bytes_written > 0
    assert report.day == date(2026, 7, 22)
    assert report.hour == 22


def test_the_hour_directory_is_unpadded_to_match_duckdbs_own_writer(tmp_path: Path) -> None:
    # DuckDB's PARTITION_BY writes `hr=9`, not `hr=09`. Matching it is what keeps a
    # hand-written partition and a PARTITION_BY-written one readable by one glob.
    assert partition_dir(tmp_path, date(2026, 7, 5), 9).name == "hr=9"
    assert partition_dir(tmp_path, date(2026, 7, 5), 0).name == "hr=0"


@pytest.mark.parametrize("hour", [-1, 24, 99])
def test_an_impossible_hour_is_refused(tmp_path: Path, hour: int) -> None:
    with pytest.raises(ValueError, match="hour must be 0-23"):
        partition_dir(tmp_path, date(2026, 7, 22), hour)


def test_the_partition_columns_are_stored_in_the_file_not_only_in_its_path(
    tmp_path: Path,
) -> None:
    # DuckDB's own PARTITION_BY keeps dt/hr *only* in the directory names, so such a
    # file forgets which hour it holds the moment it is copied elsewhere. Storing
    # them makes the data self-describing, and this asserts it by reading with Hive
    # detection off — the path is not allowed to supply the answer.
    src = _archive(tmp_path, _event("1", "2026-07-22T22:30:00"))
    report = write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)

    rows = _read(report.path, "SELECT dt, hr FROM read_parquet($f, hive_partitioning=false)")

    assert rows == [(date(2026, 7, 22), 22)]


def test_the_payload_survives_as_queryable_json(tmp_path: Path) -> None:
    src = _archive(tmp_path, _event("1", "2026-07-22T22:30:00"))
    report = write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)

    rows = _read(
        report.path,
        "SELECT json_extract_string(payload, '$.ref') FROM read_parquet($f)",
    )

    assert rows == [("refs/heads/main",)]


def test_a_payload_key_no_schema_mentions_is_carried_not_fatal(tmp_path: Path) -> None:
    # The finding that forced `payload` to stay JSON: an inferring reader hard-fails
    # on a key outside its sample. Here two rows of one type carry different keys —
    # the shape that would break a typed struct — and both must land intact.
    src = _archive(
        tmp_path,
        _event("1", "2026-07-22T22:00:00", payload={"ref": "refs/heads/main"}),
        _event("2", "2026-07-22T22:00:01", payload={"repository_hooks": "write", "novel": [1, 2]}),
    )
    report = write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)

    rows = _read(
        report.path,
        "SELECT json_extract_string(payload, '$.repository_hooks') "
        "FROM read_parquet($f) ORDER BY id",
    )

    assert report.events == 2
    assert rows == [(None,), ("write",)]


def test_a_top_level_key_outside_the_schema_is_ignored(tmp_path: Path) -> None:
    # The cost of an explicit column list, asserted so it is a known property rather
    # than a surprise: an unlisted top-level field is dropped, not carried.
    src = _archive(
        tmp_path,
        _event("1", "2026-07-22T22:00:00", some_new_envelope_field="ignored"),
    )
    report = write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)

    columns = {row[0] for row in _read(report.path, "DESCRIBE SELECT * FROM read_parquet($f)")}

    assert report.events == 1
    assert "some_new_envelope_field" not in columns
    assert {"id", "type", "actor", "repo", "org", "payload", "public", "created_at"} <= columns


def test_an_hour_spanning_two_hours_is_refused(tmp_path: Path) -> None:
    # Writing one file per hour is only correct while an hour's events share an
    # hour. That was measured true on three real hours, which is a reason to rely on
    # it and not a reason to stop checking: if it ever stops being true, the events
    # would be silently filed under the wrong hour.
    src = _archive(
        tmp_path,
        _event("1", "2026-07-22T22:59:59"),
        _event("2", "2026-07-22T23:00:00"),
    )

    with pytest.raises(PartitionMismatchError, match="spans 2 partitions"):
        write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)


def test_an_hour_holding_a_different_hour_is_refused(tmp_path: Path) -> None:
    src = _archive(tmp_path, _event("1", "2026-07-22T05:00:00"))

    with pytest.raises(PartitionMismatchError, match="contains dt=2026-07-22 hr=5"):
        write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)


def test_an_undated_event_is_refused_rather_than_filed_somewhere(tmp_path: Path) -> None:
    # A null timestamp has no partition. Hive's convention would file it under a
    # default partition, which is a plausible-looking home for an event whose hour
    # is unknown — the kind of quiet wrongness this project refuses out loud.
    src = _archive(tmp_path, _event("1", None))

    with pytest.raises(PartitionMismatchError, match="contains dt=None hr=None"):
        write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)


def test_a_refused_hour_leaves_no_file_behind(tmp_path: Path) -> None:
    # The staged file must not survive a refusal: a `.part` is invisible to the
    # lake's glob, but a stale one would be handed to the next writer's rename.
    lake = tmp_path / "lake"
    src = _archive(tmp_path, _event("1", "2026-07-22T05:00:00"))

    with pytest.raises(PartitionMismatchError):
        write_hour(src, lake, date(2026, 7, 22), 22)

    assert sorted(p.name for p in partition_dir(lake, date(2026, 7, 22), 22).iterdir()) == []


def test_an_unreadable_source_leaves_no_file_behind(tmp_path: Path) -> None:
    lake = tmp_path / "lake"

    with pytest.raises(duckdb.Error):
        write_hour(tmp_path / "absent.json.gz", lake, date(2026, 7, 22), 22)

    assert list(partition_dir(lake, date(2026, 7, 22), 22).iterdir()) == []


def test_rewriting_an_hour_replaces_it(tmp_path: Path) -> None:
    # Convergence means an hour may be ingested more than once — after a crash, or
    # because the ledger lost a row. The second write must leave one correct hour,
    # not two files or a merged one.
    lake = tmp_path / "lake"
    first = _archive(tmp_path, _event("1", "2026-07-22T22:00:00"), name="a.json.gz")
    second = _archive(
        tmp_path,
        _event("1", "2026-07-22T22:00:00"),
        _event("2", "2026-07-22T22:00:01"),
        name="b.json.gz",
    )

    write_hour(first, lake, date(2026, 7, 22), 22)
    report = write_hour(second, lake, date(2026, 7, 22), 22)

    assert report.events == 2
    assert [p.name for p in partition_dir(lake, date(2026, 7, 22), 22).iterdir()] == [
        PARQUET_FILENAME
    ]
    assert _read(report.path, "SELECT count(*) FROM read_parquet($f)") == [(2,)]


def test_an_empty_hour_is_recorded_as_empty_not_refused(tmp_path: Path) -> None:
    # A published hour with no events is a real archive gap. The ledger wants the
    # zero written down, so this is a successful write of nothing rather than an error.
    src = _archive(tmp_path)

    report = write_hour(src, tmp_path / "lake", date(2026, 7, 22), 22)

    assert report.events == 0
    assert report.path.exists()


def test_two_hours_of_the_same_day_are_separate_partitions(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_hour(
        _archive(tmp_path, _event("1", "2026-07-22T22:00:00"), name="h22.json.gz"),
        lake,
        date(2026, 7, 22),
        22,
    )
    write_hour(
        _archive(tmp_path, _event("2", "2026-07-22T23:00:00"), name="h23.json.gz"),
        lake,
        date(2026, 7, 22),
        23,
    )

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT hr, count(*) FROM read_parquet($f, hive_partitioning=false) "
            "GROUP BY hr ORDER BY hr",
            {"f": str(lake / "**" / "*.parquet")},
        ).fetchall()
    finally:
        con.close()

    assert rows == [(22, 1), (23, 1)]
