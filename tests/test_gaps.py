"""The check that says whether anything is still adding days.

This is the instrument that has to work while nobody is watching it, which makes
it the one most worth testing adversarially: a gap detector that reports healthy
because it scanned nothing is indistinguishable from a healthy history, and it is
the exact failure it exists to prevent. So the denominator is asserted alongside
every verdict here, and there is a test whose whole job is to fail if the check
ever passes on an empty directory.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from reporadar.ingest.aggregate import HOURS_PER_DAY
from reporadar.ingest.gaps import DEFAULT_GRACE_DAYS, scan_history

TODAY = date(2026, 8, 22)


def _write_day(
    aggregate_dir: Path,
    day: date,
    hours: int = HOURS_PER_DAY,
    events: int = 4_000_000,
) -> None:
    """One ecosystem row, shaped like the real aggregate's relevant columns.

    The default event count is a real measured day (2026-08-08 produced 4,012,622)
    rounded, so a test that does not care about volume still produces a plausible
    distribution rather than a degenerate one.
    """
    out = aggregate_dir / "ecosystem" / f"dt={day:%Y-%m-%d}"
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT CAST($d AS DATE) AS day, CAST($h AS BIGINT) AS hours_present, "
            "CAST($e AS BIGINT) AS events) TO $f (FORMAT PARQUET)",
            {
                "d": day.isoformat(),
                "h": hours,
                "e": events,
                "f": str(out / "ecosystem_daily.parquet"),
            },
        )
    finally:
        con.close()


def _run_of_days(aggregate_dir: Path, *, ending: date, length: int) -> None:
    for offset in range(length):
        _write_day(aggregate_dir, ending - timedelta(days=offset))


def test_an_empty_directory_is_reported_empty_and_never_healthy(tmp_path: Path) -> None:
    """The single most important case. A check that passes here passes forever,
    including on the morning after the job was silently disabled."""
    report = scan_history(tmp_path / "agg", as_of=TODAY)

    assert report.empty
    assert report.days_scanned == 0
    assert not report.healthy
    assert report.stalled
    assert "EMPTY" in " ".join(report.lines())


def test_a_current_contiguous_history_is_healthy(tmp_path: Path) -> None:
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=1), length=30)

    report = scan_history(agg, as_of=TODAY)

    assert report.days_scanned == 30
    assert report.healthy
    assert not report.stalled
    assert not report.holed
    assert report.latest_day == TODAY - timedelta(days=1)


def test_the_denominator_is_the_first_thing_printed(tmp_path: Path) -> None:
    """A reader who sees "no holes" and stops reading has learned nothing unless
    the number of days that produced it came first. Ten instruments in this
    project have reported clean having looked at nothing."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=1), length=3)

    lines = scan_history(agg, as_of=TODAY).lines()

    assert lines[0].startswith("scanned 3 day(s)")
    assert str(agg) in lines[0]


def test_a_history_that_stopped_is_reported_stalled_with_the_distance(
    tmp_path: Path,
) -> None:
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=10), length=5)

    report = scan_history(agg, as_of=TODAY)

    assert report.stalled
    assert not report.healthy
    # expected_latest is yesterday minus the grace day, so 10 days back is 8 behind.
    assert report.stale_by_days == 10 - (1 + DEFAULT_GRACE_DAYS)
    assert "STALLED" in " ".join(report.lines())


def test_the_grace_day_tolerates_a_run_that_has_not_happened_yet(tmp_path: Path) -> None:
    """For part of every day the newest day is legitimately the day before
    yesterday — the check must not cry wolf once a day, or it gets ignored."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=2), length=5)

    assert scan_history(agg, as_of=TODAY).healthy


def test_one_more_day_of_silence_than_the_grace_allows_does_fail(tmp_path: Path) -> None:
    """The other half of the boundary. A tolerance nothing ever trips is not a
    tolerance, it is a check that cannot fail."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=3), length=5)

    assert not scan_history(agg, as_of=TODAY).healthy


def test_a_hole_behind_a_current_edge_is_found(tmp_path: Path) -> None:
    """Worse than a clean stop: the job is running and has been reporting success
    while failing intermittently."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=1), length=10)
    missing = TODAY - timedelta(days=5)
    (agg / "ecosystem" / f"dt={missing:%Y-%m-%d}" / "ecosystem_daily.parquet").unlink()

    report = scan_history(agg, as_of=TODAY)

    assert report.holed
    assert not report.healthy
    assert report.missing_days == (missing,)
    assert report.days_scanned == 9
    assert "HOLES" in " ".join(report.lines())


def test_holes_are_measured_from_the_first_day_seen_not_from_a_configured_start(
    tmp_path: Path,
) -> None:
    """A history that begins the day the job was switched on has no gap before
    that day — inventing one would fail the check on its own first run, which is
    how a new gate gets disabled in its first week."""
    agg = tmp_path / "agg"
    _write_day(agg, TODAY - timedelta(days=1))

    report = scan_history(agg, as_of=TODAY)

    assert report.days_scanned == 1
    assert report.missing_days == ()
    assert report.healthy


def test_a_partial_day_is_reported_and_deliberately_does_not_fail(tmp_path: Path) -> None:
    """GH Archive genuinely has historical gaps. Folding that into the failure
    condition would make a complete history of an incomplete archive report
    failure forever — and a gate that fails permanently gets turned off."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=1), length=5)
    short = TODAY - timedelta(days=3)
    _write_day(agg, short, hours=23)

    report = scan_history(agg, as_of=TODAY)

    assert report.partial_days == ((short, 23),)
    assert report.healthy
    joined = " ".join(report.lines())
    assert "partial (reported, not failed)" in joined
    assert f"{short} (23/{HOURS_PER_DAY}h)" in joined


def test_both_failures_can_be_reported_at_once(tmp_path: Path) -> None:
    """A stalled history with a hole in it must not report only the first thing
    found — they have different cures and a reader needs both."""
    agg = tmp_path / "agg"
    _run_of_days(agg, ending=TODAY - timedelta(days=20), length=10)
    missing = TODAY - timedelta(days=25)
    (agg / "ecosystem" / f"dt={missing:%Y-%m-%d}" / "ecosystem_daily.parquet").unlink()

    report = scan_history(agg, as_of=TODAY)

    assert report.stalled and report.holed
    joined = " ".join(report.lines())
    assert "STALLED" in joined and "HOLES" in joined


def test_long_lists_are_truncated_but_the_full_count_still_shows(tmp_path: Path) -> None:
    """An unattended job that has been broken for months produces a list nobody
    can read; the count is the part that must survive truncation."""
    agg = tmp_path / "agg"
    _write_day(agg, TODAY - timedelta(days=60))
    _write_day(agg, TODAY - timedelta(days=1))

    report = scan_history(agg, as_of=TODAY)

    assert len(report.missing_days) == 58
    joined = " ".join(report.lines())
    assert "58 missing day(s)" in joined
    assert "+48 more" in joined


def test_as_of_is_a_parameter_so_the_check_is_deterministic(tmp_path: Path) -> None:
    """Reading the clock inside the function would make every test here depend on
    the day it runs — and this suite must still pass in 2027."""
    agg = tmp_path / "agg"
    _write_day(agg, date(2020, 1, 1))

    assert scan_history(agg, as_of=date(2020, 1, 2)).healthy
    assert not scan_history(agg, as_of=date(2021, 1, 2)).healthy


def test_the_report_survives_a_column_added_to_later_files(tmp_path: Path) -> None:
    """A column added years from now must not make the whole history unreadable —
    `union_by_name` is what buys that, and it is asserted rather than assumed."""
    agg = tmp_path / "agg"
    _write_day(agg, TODAY - timedelta(days=2))

    newer = TODAY - timedelta(days=1)
    out = agg / "ecosystem" / f"dt={newer:%Y-%m-%d}"
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT CAST($d AS DATE) AS day, CAST(24 AS BIGINT) AS hours_present, "
            "CAST(4000000 AS BIGINT) AS events, 'something new' AS added_later) "
            "TO $f (FORMAT PARQUET)",
            {"d": newer.isoformat(), "f": str(out / "ecosystem_daily.parquet")},
        )
    finally:
        con.close()

    report = scan_history(agg, as_of=TODAY)

    assert report.days_scanned == 2
    assert report.healthy
