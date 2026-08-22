"""Does the accumulated history still have a day added to it every day?

A scheduled job that silently stops is indistinguishable from one with nothing to
do. Both produce no output, no error and no notification, and the difference only
becomes visible months later when somebody asks a question the missing days were
supposed to answer. That is the failure this module exists to make loud, and it
is written before the first scheduled run rather than after the first outage,
because after the outage the missing days are missing permanently — the archive
publishes an hour once, and a gap in accumulated history cannot be backfilled
from a feed that has moved on.

Two different failures wear the same absence, and they are reported separately
because they need different cures:

*Stalled* — the newest day on disk is older than it should be. The job is not
running: disabled, unscheduled, failing before it writes, or pointed at the wrong
directory. Nothing here can fix it and the only useful thing to do is say so
loudly enough that a human notices.

*Holed* — the newest day is current but earlier days are absent. The job is
running and has been failing intermittently, which is worse than a clean stop,
because every run since has been reporting success.

A third state is reported and deliberately does **not** fail: a *partial* day,
holding fewer than 24 hours. GH Archive genuinely has historical gaps — an hour
the publisher never released is a settled answer, not an unfinished job — and
folding that into the failure condition would make a complete history of an
incomplete archive report failure forever. A gate that fails permanently is a
gate somebody turns off, and then it is not guarding anything. This mirrors the
same exclusion, for the same reason, that ``backfill`` already makes for
``missing`` hours.

⚠️ **A day can hold 24 hours and still be short.** Measured on the real archive:
one hour of 2026-08-19 is 2.1 MB where the same hour a week earlier is 21.2 MB,
and the pattern is not monotonic in recency, so it is feed outages rather than
publication lag. ``hours_present`` counts hours, and an hour published short is
still an hour — so completeness by hour count is necessary and **not sufficient**.
Rather than invent a volume threshold nobody can defend, this reports the
distribution of daily event counts and lets a reader see an outlier. Repairing one
is a single idempotent command; the point is only that it must be *visible* first.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import duckdb

from reporadar.ingest.aggregate import HOURS_PER_DAY

logger = logging.getLogger(__name__)

#: Exit status for a history that has stalled or has holes. Its own name rather
#: than a reuse of `repair.INCOMPLETE_EXIT_CODE`, because it is a different fact:
#: "the day did not converge" is about one day's data, this is about the schedule
#: having stopped. Same 3, as `verify` and `marts-status` also use for their own
#: designed failures.
GAP_EXIT_CODE: Final = 3

#: How far behind "yesterday" the newest day may fall before the history counts as
#: stalled. One day, because the job runs daily and a check may run before that
#: day's run has happened — so the newest day is legitimately the day before
#: yesterday for part of every day. Anything larger starts hiding real outages:
#: at a grace of 3 a job can be dead for most of a week and still pass.
DEFAULT_GRACE_DAYS: Final = 1


@dataclass(frozen=True)
class HistoryReport:
    """The state of the accumulated history, including what was actually read.

    ``days_scanned`` is not a diagnostic extra, it is the point. This project has
    been caught ten times by an instrument printing a clean result because it
    never looked, and every one of them would have been visible immediately if the
    denominator had been on screen next to the finding. A report of "no gaps"
    over zero days is the exact shape of that bug, so the count travels with the
    verdict and every caller prints it.
    """

    aggregate_dir: Path
    as_of: date
    expected_latest: date
    first_day: date | None
    latest_day: date | None
    days_scanned: int
    missing_days: tuple[date, ...]
    partial_days: tuple[tuple[date, int], ...]
    events_by_day: tuple[tuple[date, int], ...] = ()

    @property
    def empty(self) -> bool:
        """No history at all — a distinct state from a stalled one."""
        return self.days_scanned == 0

    @property
    def stale_by_days(self) -> int:
        """How many days behind ``expected_latest`` the newest day is (0 if ahead)."""
        if self.latest_day is None:
            return 0
        return max(0, (self.expected_latest - self.latest_day).days)

    @property
    def stalled(self) -> bool:
        """The job has stopped adding days."""
        return self.empty or self.stale_by_days > 0

    @property
    def holed(self) -> bool:
        """The job is current but skipped days behind it."""
        return bool(self.missing_days)

    @property
    def healthy(self) -> bool:
        """Neither stalled nor holed. Partial days do not count against this."""
        return not self.stalled and not self.holed

    @property
    def volume_summary(self) -> tuple[int, int, int] | None:
        """``(quietest, median, busiest)`` daily event counts, or None if empty.

        Deliberately a distribution rather than a verdict. A day published short
        is a real and measured phenomenon here, but the threshold that separates
        "quiet Sunday" from "the feed was down" is not something this project has
        the evidence to set — and an arbitrary one would either cry wolf every
        weekend or stay silent through a real outage. Showing the spread lets a
        reader see a 5x outlier in one glance and decide, which is the same reason
        every other check here prints its denominator.
        """
        if not self.events_by_day:
            return None
        counts = sorted(n for _, n in self.events_by_day)
        return counts[0], counts[len(counts) // 2], counts[-1]

    def as_dict(self) -> dict[str, object]:
        """A flat mapping for structured logging."""
        return {
            "days_scanned": self.days_scanned,
            "first_day": self.first_day.isoformat() if self.first_day else None,
            "latest_day": self.latest_day.isoformat() if self.latest_day else None,
            "expected_latest": self.expected_latest.isoformat(),
            "stale_by_days": self.stale_by_days,
            "missing_days": [d.isoformat() for d in self.missing_days],
            "partial_days": [[d.isoformat(), h] for d, h in self.partial_days],
            "healthy": self.healthy,
        }

    def lines(self) -> list[str]:
        """Human-readable output — the denominator first, always.

        The order is deliberate. A reader who sees "0 gaps" and stops reading has
        learned nothing unless the number of days that produced it came first.
        """
        out = [
            f"scanned {self.days_scanned} day(s) in {self.aggregate_dir}",
        ]
        if self.empty:
            out.append(f"EMPTY: no aggregated days at all (expected up to {self.expected_latest})")
            return out

        out.append(
            f"  range: {self.first_day} → {self.latest_day} (expected {self.expected_latest})"
        )
        if self.stalled:
            out.append(
                f"STALLED: newest day is {self.stale_by_days} day(s) behind. "
                "Nothing has been adding history — check the schedule, not the data."
            )
        if self.holed:
            shown = ", ".join(str(d) for d in self.missing_days[:10])
            more = "" if len(self.missing_days) <= 10 else f" (+{len(self.missing_days) - 10} more)"
            out.append(f"HOLES: {len(self.missing_days)} missing day(s): {shown}{more}")
        if self.partial_days:
            shown = ", ".join(f"{d} ({h}/{HOURS_PER_DAY}h)" for d, h in self.partial_days[:10])
            more = "" if len(self.partial_days) <= 10 else f" (+{len(self.partial_days) - 10} more)"
            out.append(f"  partial (reported, not failed): {shown}{more}")
        volume = self.volume_summary
        if volume is not None:
            quietest, median, busiest = volume
            out.append(f"  events/day: min {quietest:,} · median {median:,} · max {busiest:,}")
            if quietest * 2 < median:
                # Reported, never failed — see `volume_summary`. An hour published
                # short still counts as an hour, so this is the only place a
                # short day becomes visible at all.
                thin = sorted(d for d, n in self.events_by_day if n * 2 < median)
                shown = ", ".join(str(d) for d in thin[:10])
                more = "" if len(thin) <= 10 else f" (+{len(thin) - 10} more)"
                out.append(
                    f"  thin days, under half the median (reported, not failed): {shown}{more}. "
                    "Re-run `reporadar aggregate <day>` if the archive has since filled in."
                )
        if self.healthy:
            out.append("  no stall, no holes")
        return out


def scan_history(
    aggregate_dir: Path,
    *,
    as_of: date,
    grace_days: int = DEFAULT_GRACE_DAYS,
) -> HistoryReport:
    """Read every aggregated day and report stalls, holes and partial days.

    ``as_of`` is passed in rather than read from the clock inside this function,
    so the whole check is deterministic and a test can place itself at any date
    without freezing time globally. The caller that runs unattended passes today.

    Missing days are computed against the *observed* range — ``first_day`` to
    ``latest_day`` — not against some configured start. A history that begins the
    day the job was switched on has no gap before that day, and inventing one
    would make the check fail permanently on its own first run, which is how a
    new gate gets disabled in its first week.
    """
    expected_latest = as_of - timedelta(days=1 + grace_days)
    pattern = str(aggregate_dir / "ecosystem" / "dt=*" / "*.parquet")

    con = duckdb.connect()
    try:
        try:
            rows = con.execute(
                # `hive_partitioning=false` for the same reason the lake pins it:
                # `day` is a real column in the file, and letting DuckDB supply it
                # from the directory name would make a mislabelled file agree with
                # itself. `union_by_name` so a column added to later files does not
                # make the whole history unreadable.
                "SELECT day, hours_present, events FROM read_parquet($f, hive_partitioning=false, "
                "union_by_name=true) ORDER BY day",
                {"f": pattern},
            ).fetchall()
        except duckdb.IOException:
            # No files matched the glob. An empty history is a real state — it is
            # what the first run sees — and it is reported as EMPTY rather than
            # raised, because the caller's job is to say so loudly and exit, not
            # to hand a traceback to a log nobody is reading.
            rows = []
    finally:
        con.close()

    days = [(row[0], int(row[1])) for row in rows]
    events_by_day = tuple((row[0], int(row[2])) for row in rows)
    present = {day for day, _ in days}
    first_day = min(present) if present else None
    latest_day = max(present) if present else None

    missing: tuple[date, ...] = ()
    if first_day is not None and latest_day is not None:
        span = (latest_day - first_day).days
        missing = tuple(
            first_day + timedelta(days=offset)
            for offset in range(span + 1)
            if first_day + timedelta(days=offset) not in present
        )

    partial = tuple(sorted((day, hours) for day, hours in days if hours < HOURS_PER_DAY))

    report = HistoryReport(
        aggregate_dir=aggregate_dir,
        as_of=as_of,
        expected_latest=expected_latest,
        first_day=first_day,
        latest_day=latest_day,
        days_scanned=len(days),
        missing_days=missing,
        partial_days=partial,
        events_by_day=events_by_day,
    )
    logger.info("history scanned: %s", report.as_dict())
    return report
