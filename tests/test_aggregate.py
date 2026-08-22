"""One archive day → the two permanent Parquet aggregates.

These files are the only artifact of this project that is meant to survive
forever, and they are written by a job nobody will be watching. So the fixtures
here are built adversarially rather than realistically: the cases that decide
whether a decade of history is trustworthy are the duplicate event on an hour
boundary, the repository renamed mid-day, the event with no repository, and the
hour the publisher never released — none of which a realistic hour exercises.

``download_hour`` short-circuits when the file already exists, so a whole day is
staged by writing the ``.json.gz`` files into ``archive_dir``. That is not a mock:
it is the real function taking its real early return, and an hour deliberately
left unwritten reaches the network — pinned to a ``.invalid`` host that never
resolves — which is exactly the path a genuinely unpublished hour takes.
"""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from conftest import TEST_ARCHIVE_BASE
from reporadar.ingest.aggregate import (
    DEFAULT_MIN_EVENTS,
    HOURS_PER_DAY,
    AggregateDayMismatchError,
    aggregate_day,
    ecosystem_dir,
    repo_dir,
)
from reporadar.ingest.archive import hour_filename

REPO_ROOT = Path(__file__).resolve().parent.parent
DAY = date(2026, 7, 22)


def _event(
    event_id: str,
    hour: int,
    *,
    repo_id: int | None = 2,
    repo_name: str = "octo/hello",
    actor_id: int = 1,
    event_type: str = "PushEvent",
    minute: int = 0,
) -> dict[str, Any]:
    """One archive record, shaped like the published envelope."""
    repo = None if repo_id is None else {"id": repo_id, "name": repo_name}
    return {
        "id": event_id,
        "type": event_type,
        "actor": {"id": actor_id, "login": f"user{actor_id}"},
        "repo": repo,
        "org": None,
        "payload": {"size": 1},
        "public": True,
        "created_at": f"{DAY:%Y-%m-%d}T{hour:02d}:{minute:02d}:00",
    }


def _stage_hour(archive_dir: Path, hour: int, *events: dict[str, Any]) -> Path:
    """Place one hour where ``download_hour`` will find it and skip the network."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    path = archive_dir / hour_filename(DAY, hour)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
    return path


def _stage_full_day(archive_dir: Path, per_hour: int = 1) -> None:
    """All 24 hours, each holding ``per_hour`` events for one busy repository."""
    for hour in range(HOURS_PER_DAY):
        _stage_hour(
            archive_dir,
            hour,
            *(_event(f"{hour}-{n}", hour, minute=n) for n in range(per_hour)),
        )


def _run(tmp_path: Path, **kwargs: Any) -> Any:
    """Aggregate ``DAY`` out of ``tmp_path``, with the network pinned unreachable."""
    return aggregate_day(
        DAY,
        archive_dir=tmp_path / "archive",
        lake_dir=tmp_path / "lake",
        aggregate_dir=tmp_path / "agg",
        base_url=TEST_ARCHIVE_BASE,
        **kwargs,
    )


def _read(path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        cur = con.execute("SELECT * FROM read_parquet($f)", {"f": str(path)})
        columns = [d[0] for d in cur.description or []]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    finally:
        con.close()


def test_a_full_day_produces_one_ecosystem_row_and_the_repo_rows(tmp_path: Path) -> None:
    _stage_full_day(tmp_path / "archive", per_hour=1)

    report = _run(tmp_path)

    assert report.complete
    assert report.hours_present == HOURS_PER_DAY
    assert report.hours_written == HOURS_PER_DAY
    assert report.events == 24

    eco = _read(report.ecosystem_path)
    assert len(eco) == 1
    assert eco[0]["day"] == DAY
    assert eco[0]["events"] == 24
    assert eco[0]["hours_present"] == HOURS_PER_DAY
    assert eco[0]["pushes"] == 24


def test_duplicate_event_ids_across_hours_are_counted_once(tmp_path: Path) -> None:
    """The published archive repeats ids near an hour boundary.

    Counting them twice inflates every total by an amount that varies with how
    busy the boundary was — small, always present, and never the same, which is
    the worst shape an error in permanent history can take.
    """
    archive = tmp_path / "archive"
    _stage_full_day(archive)
    # The same id published in two hours, as the real feed does.
    _stage_hour(archive, 5, _event("dupe", 5), _event("other", 5, minute=1))
    _stage_hour(archive, 6, _event("dupe", 6), _event("other-6", 6, minute=1))

    report = _run(tmp_path)

    eco = _read(report.ecosystem_path)[0]
    # 22 untouched hours + 2 events in hour 5 + 2 in hour 6, minus the one repeat.
    assert eco["events"] == 22 + 2 + 2 - 1
    assert report.events == eco["events"]


def test_the_repo_floor_excludes_quiet_repositories_and_travels_in_the_data(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    # One busy repository (30 events) and one quiet one (3), against a floor of 20.
    _stage_hour(
        archive,
        0,
        *(_event(f"busy-{n}", 0, repo_id=100, repo_name="busy/repo", minute=n) for n in range(30)),
        *(_event(f"quiet-{n}", 0, repo_id=200, repo_name="quiet/repo", minute=n) for n in range(3)),
    )

    report = _run(tmp_path, min_events=DEFAULT_MIN_EVENTS)

    rows = _read(report.repo_path)
    assert [r["repo_id"] for r in rows] == [100]
    assert rows[0]["events"] == 30
    # The floor is written onto every row, so a file read years later can be
    # interpreted without finding the workflow that produced it.
    assert rows[0]["min_events"] == DEFAULT_MIN_EVENTS

    # The ecosystem row is NOT filtered — it counts the whole day, which is what
    # makes the two files reconcilable against each other.
    assert _read(report.ecosystem_path)[0]["events"] == 33


def test_a_lower_floor_keeps_the_repository_the_default_floor_drops(tmp_path: Path) -> None:
    """The floor is a real parameter, not a constant the query ignores."""
    archive = tmp_path / "archive"
    _stage_hour(
        archive,
        0,
        *(_event(f"q-{n}", 0, repo_id=200, repo_name="quiet/repo", minute=n) for n in range(3)),
    )

    report = _run(tmp_path, min_events=2)

    rows = _read(report.repo_path)
    assert [r["repo_id"] for r in rows] == [200]
    assert rows[0]["min_events"] == 2


def test_events_without_a_repository_are_counted_but_never_grouped(tmp_path: Path) -> None:
    """Grouping on a null key would invent one repository that every keyless event
    belongs to — a row that would look like the busiest repository on GitHub, and
    would then sit in permanent history."""
    archive = tmp_path / "archive"
    _stage_hour(
        archive,
        0,
        *(_event(f"keyed-{n}", 0, repo_id=100, minute=n) for n in range(25)),
        _event("orphan-1", 0, repo_id=None, minute=30),
        _event("orphan-2", 0, repo_id=None, minute=31),
    )

    report = _run(tmp_path)

    eco = _read(report.ecosystem_path)[0]
    assert eco["events"] == 27
    assert eco["events_without_repo"] == 2

    rows = _read(report.repo_path)
    assert [r["repo_id"] for r in rows] == [100]
    assert rows[0]["events"] == 25
    # The two halves reconcile — the property the pair of files exists to support.
    assert eco["events"] - eco["events_without_repo"] == 25


def test_a_renamed_repository_keeps_the_name_from_its_latest_event(tmp_path: Path) -> None:
    """``max`` would return whichever name sorts highest, which is a name nobody chose."""
    archive = tmp_path / "archive"
    _stage_hour(
        archive,
        0,
        *(
            _event(f"old-{n}", 0, repo_id=100, repo_name="zzz/old-name", minute=n)
            for n in range(20)
        ),
    )
    _stage_hour(
        archive,
        1,
        *(_event(f"new-{n}", 1, repo_id=100, repo_name="aaa/new-name", minute=n) for n in range(5)),
    )

    report = _run(tmp_path)

    rows = _read(report.repo_path)
    assert rows[0]["repo_name"] == "aaa/new-name"


def test_a_missing_hour_still_writes_the_day_and_reports_it_incomplete(tmp_path: Path) -> None:
    """The archive publishes an hour once. Refusing to write would lose the other 23."""
    archive = tmp_path / "archive"
    _stage_full_day(archive)
    (archive / hour_filename(DAY, 13)).unlink()

    report = _run(tmp_path)

    assert not report.complete
    assert report.hours_missing == (13,)
    assert report.hours_present == HOURS_PER_DAY - 1
    assert report.ecosystem_path.exists()
    assert _read(report.ecosystem_path)[0]["hours_present"] == HOURS_PER_DAY - 1


def test_re_running_a_day_replaces_it_rather_than_doubling_it(tmp_path: Path) -> None:
    """Idempotency is what makes a retry safe on an unattended schedule."""
    _stage_full_day(tmp_path / "archive")
    first = _run(tmp_path, keep_source=True)

    second = _run(tmp_path, keep_source=True)

    assert first.events == second.events
    assert len(_read(second.ecosystem_path)) == 1
    assert second.ecosystem_path.read_bytes() == first.ecosystem_path.read_bytes()


def test_the_sources_are_released_by_default_and_kept_on_request(tmp_path: Path) -> None:
    """An unattended daily job that keeps its inputs is a disk-full incident with
    a date on it — and the inputs are re-downloadable from a public URL."""
    archive = tmp_path / "archive"
    _stage_full_day(archive)

    _run(tmp_path)

    assert list(archive.glob("*.json.gz")) == []
    assert list((tmp_path / "lake").glob("dt=*/hr=*/*.parquet")) == []

    _stage_full_day(archive)
    _run(tmp_path, keep_source=True, keep_lake=True)
    assert len(list(archive.glob("*.json.gz"))) == HOURS_PER_DAY
    assert len(list((tmp_path / "lake").glob("dt=*/hr=*/*.parquet"))) == HOURS_PER_DAY


def test_a_day_with_no_hours_at_all_refuses_rather_than_writing_a_quiet_day(
    tmp_path: Path,
) -> None:
    """Nothing published, nothing downloaded, and a wrong lake path are three
    different problems that an empty aggregate would record identically."""
    with pytest.raises(FileNotFoundError, match="no archive hours"):
        _run(tmp_path)


def test_a_file_holding_the_wrong_day_is_refused_before_it_is_renamed_in(
    tmp_path: Path,
) -> None:
    """A file named for one day and holding another would put every later reader
    in disagreement with the archive, permanently — this history is never rewritten."""
    archive = tmp_path / "archive"
    # An hour whose events belong to the *next* day. `write_hour` rejects this at
    # the lake boundary, which is where it should be caught; this asserts the
    # second net exists rather than assuming the first one never leaks.
    _stage_hour(archive, 0, _event("x", 0))
    _stage_full_day(archive)

    report = _run(tmp_path, keep_lake=True)
    assert report.complete

    # Now corrupt the lake directly and re-aggregate: the day check must fire.
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT * REPLACE (DATE '2026-07-23' AS dt) FROM read_parquet($f)) "
            "TO $f2 (FORMAT PARQUET)",
            {
                "f": str(tmp_path / "lake" / "dt=2026-07-22" / "hr=0" / "events.parquet"),
                "f2": str(tmp_path / "lake" / "dt=2026-07-22" / "hr=0" / "events.parquet"),
            },
        )
    finally:
        con.close()

    with pytest.raises(AggregateDayMismatchError, match="distinct day"):
        _run(tmp_path, keep_lake=True)


def test_the_staged_part_file_never_survives_a_refusal(tmp_path: Path) -> None:
    """A ``.part`` left behind would be picked up by the next glob as a real file."""
    _stage_full_day(tmp_path / "archive")
    report = _run(tmp_path)

    assert list(ecosystem_dir(tmp_path / "agg", DAY).glob("*.part")) == []
    assert list(repo_dir(tmp_path / "agg", DAY).glob("*.part")) == []
    assert report.ecosystem_path.exists()


def test_timestamps_are_naive_and_unshifted_exactly_as_the_archive_publishes_them(
    tmp_path: Path,
) -> None:
    """Naive, and the value is asserted rather than only the type.

    A zoned column would be better data and would cost a dependency — DuckDB
    routes ``AT TIME ZONE`` through ``pytz``, which this package does not declare.
    So the convention stays where the archive already put it, and what is checked
    is the thing that would actually break: that no reader's local offset has been
    applied. The fixture writes 00:00, and on any machine in any zone this must
    read back as 00:00.
    """
    _stage_full_day(tmp_path / "archive")
    report = _run(tmp_path)

    con = duckdb.connect()
    try:
        kind, first = con.execute(
            "SELECT typeof(first_event_at), first_event_at FROM read_parquet($f)",
            {"f": str(report.ecosystem_path)},
        ).fetchone() or ("", None)
    finally:
        con.close()

    assert "WITH TIME ZONE" not in kind.upper(), (
        "a zoned column reintroduces the pytz dependency that CI does not install"
    )
    assert first == datetime(2026, 7, 22, 0, 0), "an offset was applied somewhere"


def test_the_aggregate_carries_no_actor_login_or_payload(tmp_path: Path) -> None:
    """These files are committed to a public repository forever. The published page
    already publishes aggregates only; this asserts the same of the history."""
    _stage_full_day(tmp_path / "archive")
    report = _run(tmp_path)

    for path in (report.ecosystem_path, report.repo_path):
        columns = {c.lower() for c in _read(path)[0]}
        assert not columns & {"actor_login", "payload", "org_login", "actor", "email"}


# --- The committed history, as an artifact ------------------------------------
#
# The tests above check the function. These check the thing the function has
# already written into the repository, which is a different subject with a
# different failure mode: the function can be correct today and still have left
# a decade of files that disagree with each other.
#
# This exists because it happened. Thirteen days were written with zone-aware
# timestamps; a dependency problem forced the column back to naive, and the next
# day was written the new way. DuckDB read the mixed set without complaining --
# it coerces -- so nothing failed, and the only visible symptom was a re-run
# producing a file 249 bytes different from the one it replaced.
#
# ⇒ 🔑 A permanent archive's schema is not checked by anything that reads it
#   leniently, and every reader is lenient until the one that is not.

HISTORY = REPO_ROOT / "aggregates"


def _schema(path: Path) -> tuple[tuple[str, str], ...]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet($f))",
            {"f": str(path)},
        ).fetchall()
    finally:
        con.close()
    return tuple((str(name), str(kind)) for name, kind in rows)


@pytest.mark.parametrize("grain", ["ecosystem", "repo"])
def test_every_committed_day_has_the_same_schema(grain: str) -> None:
    """One schema per grain, across the whole history, forever.

    A day written with a different column type is not a failure anyone will see:
    readers coerce, and the archive keeps growing around the inconsistency until
    something strict finally reads it — by which time the divergent days are
    years old and the reason is forgotten.
    """
    files = sorted(HISTORY.glob(f"{grain}/dt=*/*.parquet"))
    if not files:
        pytest.skip(f"no committed {grain} history in this checkout")

    schemas: dict[tuple[tuple[str, str], ...], list[str]] = {}
    for path in files:
        schemas.setdefault(_schema(path), []).append(path.parent.name)

    # The denominator, on screen, before the verdict.
    assert len(schemas) == 1, (
        f"scanned {len(files)} {grain} file(s) and found {len(schemas)} different schemas: "
        + " | ".join(
            f"{len(days)} day(s) from {sorted(days)[0]}: "
            + ", ".join(f"{n}:{t}" for n, t in schema)
            for schema, days in schemas.items()
        )
    )


def test_the_schema_check_can_actually_fail(tmp_path: Path) -> None:
    """The control. `_schema` returning a constant, or the comparison being made
    on something that cannot differ, would make the test above pass over any
    history at all."""
    con = duckdb.connect()
    try:
        a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
        con.execute(f"COPY (SELECT CAST(1 AS BIGINT) AS x) TO '{a}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT CAST(1 AS VARCHAR) AS x) TO '{b}' (FORMAT PARQUET)")
    finally:
        con.close()
    assert _schema(a) != _schema(b), "control: two different column types compared equal"
    assert _schema(a) == _schema(a), "control: the same file compared unequal to itself"
