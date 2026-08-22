"""One archive day → two small, permanent Parquet aggregates.

The lake keeps every event, which is the right shape for analysis and the wrong
shape for *history*: a day of raw events measures ~325 MB, so a year is ~116 GB
and nothing that has to survive in a git repository can hold it. These aggregates
are the same day at the two grains every later question actually asks — the
ecosystem total and the repository-day — and they measure ~558 KB on a busy day,
which is 0.17% of raw.

That ratio is the whole design. Survival analysis and forecasting are gated on
*accumulated history*, not on effort, so no amount of work brings them forward
while nothing is accumulating; and the thing that accumulates has to be cheap
enough to keep forever, or it will be deleted the first time somebody looks at a
repository's size.

Three properties are load-bearing.

*The grain matches the marts, column for column.* These files are computed from
the lake by this module, and the same numbers are computed from the lake by
``dbt`` (``ecosystem_daily``, ``repo_daily``). Two derivations of one quantity is
normally a defect — here it is deliberate, because they run in different places
for different reasons, and it is only safe while they agree. They are written to
agree by construction: the same dedup rule, the same null-repo exclusion, the
same ``arg_max`` for a renamed repository, the same UTC handling. A test asserts
the agreement rather than trusting this paragraph.

*The floor travels in the data.* ``repo_daily`` keeps only repository-days at or
above ``min_events``. The cost of each choice was measured on one real full day
(2026-08-12, 3,925,039 events) rather than estimated:

===== ========== ========= ================
floor   rows/day   MB/year   events covered
===== ========== ========= ================
   20     19,501       199            62.4%
   50     12,029       120            56.5%
  100      7,238        73            47.5%
  200      2,746        29            31.6%
===== ========== ========= ================

20 ships. It keeps the most signal per byte at a size a repository can carry, and
the asymmetry decides it: a floor can always be raised later by filtering what was
kept, and can never be lowered, because the rows below it were never written.

⚠️ **The floor must then stay fixed.** It defines the risk set — a repository
leaving the data means "fell below 20 events that day", which is a well-defined
observable and is exactly the kind of event the survival analysis this history
exists for will want to model. Change the floor and days before and after stop
being comparable, silently. So it is written as a column on every row: a file can
be read correctly, years later, without finding the job that produced it.

*A day lands whole or not at all.* Both files are staged beside their final names
and renamed into place, and each is checked to hold exactly the day it is named
for before the rename. A truncated or mislabelled Parquet still parses, so
nothing downstream could tell a wrong day from a quiet one.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from reporadar.ingest.archive import DEFAULT_BASE_URL, download_hour
from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir, write_hour

logger = logging.getLogger(__name__)

#: Hours in a published archive day. Named rather than spelled `24` at each use,
#: because two of those uses are completeness tests and a bare literal in a test
#: is indistinguishable from a coincidence.
HOURS_PER_DAY: Final = 24

#: The repository-day floor that ships. See the table in the module docstring for
#: the measured cost of each alternative. ⛔ **Changing this silently breaks
#: comparability across the history** — see the warning there before touching it.
DEFAULT_MIN_EVENTS: Final = 20

ECOSYSTEM_FILENAME: Final = "ecosystem_daily.parquet"
REPO_FILENAME: Final = "repo_daily.parquet"

#: The event types broken out as their own columns. Everything else is still
#: counted in `events`; these are the six that carry the questions people ask of
#: this feed, and they are listed once so the two aggregates cannot drift apart.
BREAKOUT_EVENT_TYPES: Final[dict[str, str]] = {
    "pushes": "PushEvent",
    "stars": "WatchEvent",
    "forks": "ForkEvent",
    "issues": "IssuesEvent",
    "pull_requests": "PullRequestEvent",
    "releases": "ReleaseEvent",
}


class AggregateDayMismatchError(RuntimeError):
    """A staged aggregate does not hold exactly the day it was written for.

    Its own class rather than a bare ``ValueError`` for the same reason
    ``PartitionMismatchError`` has one: the caller has to tell "the archive has
    not published this yet, retry" apart from "this file contradicts its own
    name", and only the second is a structural problem that a retry cannot fix.
    """


@dataclass(frozen=True)
class AggregateReport:
    """What one day's aggregation actually produced, for the caller to log.

    ``hours_written`` and ``hours_present`` answer different questions and both
    are kept. The first is how many hours this run converted; the second is how
    many hours the finished file contains, which includes hours a previous run
    left in the lake. A day can be complete because of what an earlier run did,
    and a report that only knew the first number would call that day empty.
    """

    day: date
    ecosystem_path: Path
    repo_path: Path
    events: int
    repo_rows: int
    hours_written: int
    hours_present: int
    hours_missing: tuple[int, ...]
    min_events: int
    bytes_written: int

    @property
    def complete(self) -> bool:
        """Whether the finished day holds all 24 published hours."""
        return self.hours_present == HOURS_PER_DAY

    def as_dict(self) -> dict[str, object]:
        """A flat mapping for structured logging."""
        return {
            "day": self.day.isoformat(),
            "events": self.events,
            "repo_rows": self.repo_rows,
            "hours_written": self.hours_written,
            "hours_present": self.hours_present,
            "hours_missing": list(self.hours_missing),
            "min_events": self.min_events,
            "bytes": self.bytes_written,
            "complete": self.complete,
        }


def ecosystem_dir(aggregate_dir: Path, day: date) -> Path:
    """``<aggregates>/ecosystem/dt=YYYY-MM-DD`` — the lake's own partition idiom.

    Hive layout rather than one flat file per day so that a reader can point
    DuckDB at the whole tree and let it prune by ``dt``, exactly as it does over
    the lake. The cost is one directory per day per grain, which after a decade is
    7,300 directories and still nothing a filesystem notices.
    """
    return aggregate_dir / "ecosystem" / f"dt={day:%Y-%m-%d}"


def repo_dir(aggregate_dir: Path, day: date) -> Path:
    """``<aggregates>/repo/dt=YYYY-MM-DD`` — see :func:`ecosystem_dir`."""
    return aggregate_dir / "repo" / f"dt={day:%Y-%m-%d}"


def _breakout_clause() -> str:
    """Render :data:`BREAKOUT_EVENT_TYPES` as ``count(*) FILTER`` columns.

    Interpolated rather than bound for the same reason ``lake._columns_clause``
    is: these are part of the query's shape, not values, and every character
    comes from the constant above rather than from a caller.
    """
    return ",\n            ".join(
        f"count(*) FILTER (WHERE event_type = '{event_type}') AS {column}"
        for column, event_type in BREAKOUT_EVENT_TYPES.items()
    )


def _deduped_cte(sources: str) -> str:
    """The staging projection, written to match ``dbt``'s ``stg_events`` exactly.

    Four choices here are copied deliberately rather than reinvented, and each one
    is a number that would differ if it were not:

    ``QUALIFY row_number()`` — the published archive repeats event ids across
    hours near an hour boundary. Counting them twice inflates every total by an
    amount that varies with how busy the boundary was, which is the worst kind of
    error: small, always present, and never the same.

    ``created_at AT TIME ZONE 'UTC'`` — the archive stores the timestamp without a
    zone, meaning UTC by convention. A plain cast reads it as *local* time and
    moves every event by the reader's offset; on a UTC machine the two spellings
    are indistinguishable, so the bug only ever appears for somebody else.

    ``hive_partitioning=false`` — the parquet files sit inside ``dt=``/``hr=``
    directories and also carry ``dt`` and ``hr`` as real columns. With Hive
    detection left on, DuckDB supplies the partition values from the path, so a
    file whose contents contradict its location would agree with itself. The
    values have to come from the data, which is the same reason
    ``lake._verify_hour`` pins it.

    ``repo`` is read as an id and a name and nothing else — the aggregates never
    carry a payload, an actor login, or an email, so there is no path by which a
    committed file could publish something about an individual.
    """
    return f"""
        WITH deduped AS (
            SELECT
                type                                AS event_type,
                CAST(actor ->> 'id' AS BIGINT)      AS actor_id,
                CAST(repo ->> 'id' AS BIGINT)       AS repo_id,
                repo ->> 'name'                     AS repo_name,
                created_at AT TIME ZONE 'UTC'       AS created_at,
                dt                                  AS archive_day,
                hr                                  AS archive_hour
            FROM read_parquet({sources}, hive_partitioning=false)
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY created_at) = 1
        )
    """


def _ecosystem_query(sources: str) -> str:
    """One row: the day's totals, and the coverage that says what they are of.

    ``hours_present`` is not decoration. A daily total computed from seven
    ingested hours is not wrong about the seven hours; it is wrong about the day,
    and nothing in the number itself says which. Carrying the hour count makes
    those two claims separable by construction rather than by a caption somebody
    remembers — and after a year of unattended runs, nobody remembers.
    """
    return f"""
        {_deduped_cte(sources)}
        SELECT
            archive_day                             AS day,
            count(*)                                AS events,
            count(DISTINCT repo_id)                 AS repos,
            count(DISTINCT actor_id)                AS actors,
            {_breakout_clause()},
            count(DISTINCT archive_hour)            AS hours_present,
            count(*) FILTER (WHERE repo_id IS NULL) AS events_without_repo,
            min(created_at)                         AS first_event_at,
            max(created_at)                         AS last_event_at
        FROM deduped
        GROUP BY archive_day
    """


def _repo_query(sources: str) -> str:
    """One row per repository-day at or above the floor.

    ``arg_max(repo_name, created_at)`` rather than ``max``: repositories get
    renamed mid-day, and ``max`` returns whichever name sorts highest, which is a
    name nobody ever chose. This returns the name carried by that day's most
    recent event.

    ``repo_id IS NOT NULL`` excludes rather than groups the handful of published
    events carrying an empty repository object (3 in 5,090,496 measured, all
    ForkEvents). Grouping on a null key would invent one repository that every
    keyless event belongs to — a row that would look like the busiest repository
    on GitHub, and would stay in the history forever.

    The floor is applied in ``HAVING`` against the *ungrouped* count, so it is the
    repository's real activity for the day and not a count of surviving rows.
    """
    return f"""
        {_deduped_cte(sources)}
        SELECT
            archive_day                             AS day,
            repo_id,
            arg_max(repo_name, created_at)          AS repo_name,
            count(*)                                AS events,
            count(DISTINCT actor_id)                AS actors,
            {_breakout_clause()},
            min(created_at)                         AS first_event_at,
            max(created_at)                         AS last_event_at,
            CAST($floor AS INTEGER)                 AS min_events
        FROM deduped
        WHERE repo_id IS NOT NULL
        GROUP BY archive_day, repo_id
        HAVING count(*) >= $floor
    """


def _write_checked(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: dict[str, object],
    final: Path,
    day: date,
) -> tuple[int, int]:
    """Run ``query`` to a staged Parquet, prove it holds ``day``, then rename in.

    Returns ``(rows, bytes)``. The check runs on the staged file rather than on
    the query, because what matters is what a later reader will find in the file —
    and the two are only the same while nothing between them is wrong.
    """
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_suffix(final.suffix + ".part")
    try:
        con.execute(
            f"COPY ({query}) TO $dest (FORMAT PARQUET, COMPRESSION zstd)",
            {**params, "dest": str(part)},
        )
        rows, distinct_days, found_day = con.execute(
            "SELECT count(*), count(DISTINCT day), min(day) FROM read_parquet($f)",
            {"f": str(part)},
        ).fetchone() or (0, 0, None)

        if rows and (distinct_days != 1 or found_day != day):
            raise AggregateDayMismatchError(
                f"aggregate for {day} holds {distinct_days} distinct day(s) "
                f"starting {found_day}. A file named for one day and holding "
                "another would put every later reader in disagreement with the "
                "archive, permanently — this history is never rewritten."
            )

        written = part.stat().st_size
        # Same-directory rename, so it is atomic and it replaces: the partition
        # holds either the previous complete day or this one, never a prefix.
        # This is what makes re-running a day idempotent rather than merely safe.
        os.replace(part, final)
        return int(rows), written
    finally:
        part.unlink(missing_ok=True)


def aggregate_day(
    day: date,
    *,
    archive_dir: Path,
    lake_dir: Path,
    aggregate_dir: Path,
    min_events: int = DEFAULT_MIN_EVENTS,
    base_url: str = DEFAULT_BASE_URL,
    keep_source: bool = False,
    keep_lake: bool = False,
) -> AggregateReport:
    """Download one archive day, aggregate it, and write the two Parquet files.

    Hours are fetched, converted and released one at a time rather than all at
    once. A measured hour is 12–20 MB gzipped, so a whole day held at once is only
    ~400 MB and would fit a hosted runner comfortably — the reason to stream is
    not this day, it is the day the feed doubles. Bounding disk by construction
    costs nothing here and removes a failure that would otherwise arrive years
    from now, unattended, as a job that had been working every day until it wasn't.

    Sequential rather than concurrent for the same class of reason: the publisher
    is somebody else's server, this runs once a day with hours to spare, and
    politeness is a property of the client. There is no deadline to buy.

    ``keep_source``/``keep_lake`` default to removing what they name. An
    unattended daily job that keeps its inputs is a disk-full incident with a date
    on it, and the inputs are re-downloadable from a public URL, which is the
    definition of regenerable.
    """
    hours_written = 0
    missing: list[int] = []

    for hour in range(HOURS_PER_DAY):
        try:
            source = download_hour(day, hour, archive_dir, base_url=base_url)
        except Exception:
            # An hour the publisher has not released — or released and later
            # withdrew — is a fact about the archive, not a failure of this run.
            # It is collected and reported rather than raised, because the other
            # 23 hours are worth keeping and a day that refuses to write anything
            # loses them. The exit status still says the day is incomplete.
            logger.warning("archive hour unavailable: dt=%s hr=%d", day, hour)
            missing.append(hour)
            continue

        try:
            write_hour(source, lake_dir, day, hour)
            hours_written += 1
        finally:
            if not keep_source:
                source.unlink(missing_ok=True)

    sources = str(lake_dir / f"dt={day:%Y-%m-%d}" / "hr=*" / PARQUET_FILENAME)
    # `if not _present_hours(...)`, never `if not any(...)`: the list holds hour
    # *numbers*, and hour 0 is falsy — so `any()` reads a day whose only published
    # hour is midnight as a day with no hours at all. Caught by a test staging a
    # single hour 0; it would otherwise have surfaced as a 1-in-24 failure on a
    # real archive gap, months into an unattended run.
    if not _present_hours(lake_dir, day):
        raise FileNotFoundError(
            f"no archive hours for {day} in {lake_dir}. Nothing was published, "
            "nothing downloaded, or the lake path is wrong — and writing an empty "
            "aggregate for the day would record all three as 'a quiet day'."
        )

    eco_final = ecosystem_dir(aggregate_dir, day) / ECOSYSTEM_FILENAME
    repo_final = repo_dir(aggregate_dir, day) / REPO_FILENAME

    con = duckdb.connect()
    try:
        # Named parameters throughout: DuckDB binds a COPY statement's destination
        # before the query inside it, so positional marks are filled in the
        # opposite order to the one they are written in.
        _, eco_bytes = _write_checked(
            con, _ecosystem_query("$src"), {"src": sources}, eco_final, day
        )
        repo_rows, repo_bytes = _write_checked(
            con,
            _repo_query("$src"),
            {"src": sources, "floor": min_events},
            repo_final,
            day,
        )
        events, hours_present = con.execute(
            "SELECT events, hours_present FROM read_parquet($f)",
            {"f": str(eco_final)},
        ).fetchone() or (0, 0)
    finally:
        con.close()

    if not keep_lake:
        _drop_lake_day(lake_dir, day)

    report = AggregateReport(
        day=day,
        ecosystem_path=eco_final,
        repo_path=repo_final,
        events=int(events),
        repo_rows=int(repo_rows),
        hours_written=hours_written,
        hours_present=int(hours_present),
        hours_missing=tuple(missing),
        min_events=min_events,
        bytes_written=eco_bytes + repo_bytes,
    )
    logger.info("aggregate day written: %s", report.as_dict())
    return report


def _present_hours(lake_dir: Path, day: date) -> list[int]:
    """Which hours of ``day`` the lake currently holds a Parquet file for."""
    return [
        hour
        for hour in range(HOURS_PER_DAY)
        if (partition_dir(lake_dir, day, hour) / PARQUET_FILENAME).exists()
    ]


def _drop_lake_day(lake_dir: Path, day: date) -> None:
    """Remove one day's lake partitions once its aggregate is safely on disk.

    Deliberately narrow: it removes the files it can name and the directories that
    are then empty, and never recurses over anything it did not write. A cleanup
    step in an unattended job is the most dangerous line in it, and the safe shape
    is one that cannot be pointed at a tree by a wrong argument.
    """
    for hour in range(HOURS_PER_DAY):
        part = partition_dir(lake_dir, day, hour)
        (part / PARQUET_FILENAME).unlink(missing_ok=True)
        if part.is_dir() and not any(part.iterdir()):
            part.rmdir()
    day_dir = lake_dir / f"dt={day:%Y-%m-%d}"
    if day_dir.is_dir() and not any(day_dir.iterdir()):
        day_dir.rmdir()
