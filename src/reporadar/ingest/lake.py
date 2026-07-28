"""GH Archive hours → a Parquet lake, one file per hour.

The published ``.json.gz`` files are row-oriented and whole-file: any query over
them decompresses everything, so column pruning — the reason the analytical half
of this project fits on one small machine — is unavailable. The lake is the
columnar copy of the same events, and measured on real hours it is *smaller* than
the gzip it is built from, so keeping both costs little.

Three properties are load-bearing.

*The schema is written down, never inferred.* A sampling reader guesses a type
for ``payload`` from the first rows and then fails on any later row carrying a key
the sample never saw. Worse, whether it fails depends on which columns the query
projects, so an inferring reader passes every test that does not select
``payload`` and then fails months later on an arbitrary hour. Listing the columns
removes inference from the path entirely; the price is that a genuinely new
top-level field is ignored until someone adds it here, deliberately.

*``payload`` stays JSON.* It is heterogeneous *within* a single event type — three
distinct key-sets for PullRequestEvent inside one measured hour — so a typed
struct would turn every key GitHub adds into a dropped hour. Typed extraction
belongs downstream, where a schema change is a failing test rather than an outage.

*An hour lands whole or not at all.* The file is staged beside its final name and
renamed into place, because a truncated Parquet file still parses: nothing
downstream could tell a short hour from a quiet one, which makes a half-written
artifact worse than a missing one.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

logger = logging.getLogger(__name__)

# The archive's top-level envelope, written down rather than inferred. This is the
# single home for the lake's schema: the reader's column list and the file's
# columns are the same definition, so they cannot drift apart.
ARCHIVE_COLUMNS: Final[dict[str, str]] = {
    "id": "VARCHAR",
    "type": "VARCHAR",
    "actor": "JSON",
    "repo": "JSON",
    "org": "JSON",
    "payload": "JSON",
    "public": "BOOLEAN",
    "created_at": "TIMESTAMP",
}

PARQUET_FILENAME: Final = "events.parquet"


class PartitionMismatchError(RuntimeError):
    """A staged hour does not hold exactly the ``(dt, hr)`` it was written for.

    Its own error class rather than a bare ``ValueError`` because the ingest loop
    has to tell failure kinds apart: an hour that is not published yet should be
    retried, while an hour whose contents contradict its name is a structural
    problem that retrying cannot fix and that must not be recorded as done.
    """


@dataclass(frozen=True)
class LakeWriteReport:
    """What actually landed, for the caller to log or record in the ledger."""

    path: Path
    day: date
    hour: int
    events: int
    bytes_written: int


def partition_dir(lake_dir: Path, day: date, hour: int) -> Path:
    """``<lake>/dt=YYYY-MM-DD/hr=H`` — Hive layout, hour unpadded.

    Unpadded to match what DuckDB's own ``PARTITION_BY (dt, hr)`` produces, so the
    two writers stay interchangeable: readers cast the partition value and never
    sort the string, which is the only thing padding would buy.

    ``dt`` above the hour keeps directory fan-out at 24 entries per day rather
    than 8,760 in one directory per year, and the hour is the unit of publication,
    of retry and of reconciliation, so it is the unit of partition.
    """
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be 0-23, got {hour}")
    return lake_dir / f"dt={day:%Y-%m-%d}" / f"hr={hour}"


def _columns_clause() -> str:
    """Render ``ARCHIVE_COLUMNS`` as DuckDB's ``columns=`` struct literal.

    Interpolated rather than bound because a struct literal is part of the query's
    shape, not a value. Safe for exactly that reason: every character comes from
    the constant above, never from a caller — the paths, which *do* come from
    callers, are bound parameters.
    """
    return "{" + ", ".join(f"'{name}':'{sql}'" for name, sql in ARCHIVE_COLUMNS.items()) + "}"


def write_hour(archive_path: Path, lake_dir: Path, day: date, hour: int) -> LakeWriteReport:
    """Convert one downloaded archive hour into its Parquet partition.

    ``day``/``hour`` are what the caller asked for, and the events are checked
    against them rather than trusted: writing one file per hour is only correct
    while an hour's events really do belong to one hour, which is measured here on
    every write instead of being assumed once.

    Always writes, even when the partition already holds a file — the rename
    replaces atomically, so re-running is safe. Deciding *whether* an hour needs
    writing is the ledger's job, not the writer's, which keeps this function a
    seam any scheduler can call.
    """
    target = partition_dir(lake_dir, day, hour)
    target.mkdir(parents=True, exist_ok=True)
    final = target / PARQUET_FILENAME
    part = final.with_suffix(final.suffix + ".part")

    con = duckdb.connect()
    try:
        # Named parameters, not positional: DuckDB binds a COPY statement's
        # destination before the query inside it, so two `?` marks are filled in
        # the opposite order to the one they are written in — silently handing the
        # reader the file it was supposed to write. Naming them removes the
        # question rather than relying on remembering the answer.
        con.execute(
            f"""
            COPY (
                SELECT *, CAST(created_at AS DATE) AS dt, hour(created_at) AS hr
                FROM read_json($src, format='newline_delimited', columns={_columns_clause()})
            ) TO $dest (FORMAT PARQUET, COMPRESSION zstd)
            """,
            {"src": str(archive_path), "dest": str(part)},
        )
        events = _verify_hour(con, part, day, hour)
        bytes_written = part.stat().st_size
        # Same-directory rename, so it is atomic and it replaces: the partition
        # holds either the previous complete hour or this one, never a prefix.
        os.replace(part, final)
    finally:
        con.close()
        # A no-op once the rename has happened; the whole point on every other path.
        part.unlink(missing_ok=True)

    logger.info(
        "lake hour written: dt=%s hr=%d events=%d bytes=%d out=%s",
        day,
        hour,
        events,
        bytes_written,
        final,
    )
    return LakeWriteReport(
        path=final, day=day, hour=hour, events=events, bytes_written=bytes_written
    )


def _verify_hour(con: duckdb.DuckDBPyConnection, part: Path, day: date, hour: int) -> int:
    """Return the staged file's row count, refusing anything but the hour asked for.

    ``hive_partitioning=false`` is load-bearing, not tidiness. The staged file sits
    *inside* ``dt=.../hr=...``, so with Hive detection left on, DuckDB would supply
    ``dt`` and ``hr`` from those directory names — and this function would be
    comparing the path against itself, passing by construction while measuring
    nothing. The values have to come from the data.
    """
    rows = con.execute(
        "SELECT dt, hr, count(*) FROM read_parquet($f, hive_partitioning=false) GROUP BY dt, hr",
        {"f": str(part)},
    ).fetchall()

    if not rows:
        # A published hour with no events is a real (rare) archive gap, not a bug —
        # but it is unusual enough that an operator should hear about it, and the
        # ledger wants the zero recorded rather than inferred from a missing row.
        logger.warning("lake hour is empty: dt=%s hr=%d", day, hour)
        return 0

    if len(rows) > 1:
        found = ", ".join(f"dt={dt} hr={hr} (n={n})" for dt, hr, n in sorted(rows, key=str))
        raise PartitionMismatchError(
            f"archive hour {day} {hour:02d} spans {len(rows)} partitions: {found}. "
            "One file per hour is only correct while an hour's events share an hour; "
            "this file needs partition-per-group writing instead."
        )

    got_day, got_hour, count = rows[0]
    if got_day != day or got_hour != hour:
        raise PartitionMismatchError(
            f"archive hour {day} {hour:02d} contains dt={got_day} hr={got_hour} instead. "
            "The file is named for one hour and holds another, so recording it as that "
            "hour would put the ledger and the lake in disagreement."
        )
    return int(count)
