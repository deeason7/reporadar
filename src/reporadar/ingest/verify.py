"""Check the hours ledger against the lake it describes.

Two records of the same fact exist and have never been compared. The ledger says
which hours are in the lake; the lake is the files. Every counter, every gap
scan and every coverage number reads the ledger, so a row claiming an hour that
is not there is not a small inconsistency — it is a number that lies, and lies
in the safe-looking direction.

**Which direction of disagreement matters is the whole design here.** The ledger
is the thing everything else believes, so:

- a **claim with no file** is a falsehood: something reports coverage it does not
  have, and nothing will ever revisit the hour, because a settled status is never
  rescanned. This fails the check.
- a **file with no claim** is only waste: nothing is misreported, and the next
  scan simply converts the hour again. This is reported and does not fail.

That asymmetry is why the exit code is not just "anything disagreed".

Nothing here writes. A verifier that repaired what it found would be a second
writer of the ledger, and then a disagreement between the two records would have
two possible authors — which is exactly the situation this exists to detect.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Final

import duckdb

from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir
from reporadar.ingest.ledger import Connection, HourRecord, ingested_hours

logger = logging.getLogger(__name__)

#: Per-hour row counts, read from the columns stored *inside* each file rather
#: than from the directory names. The names are what we are checking, so reading
#: them back would only confirm that a path equals itself; the in-file columns are
#: the file's own claim about which hour it holds, which is the claim worth
#: testing. One query over the whole lake, which is what a columnar store is for.
#:
#: ``hive_partitioning=false`` is what makes that true, and it is not optional.
#: Left to its default, DuckDB sees ``dt=…/hr=…`` in the path and derives those
#: columns from the directory names — so the query would read the path back to
#: itself and a misfiled hour would verify perfectly. Worse, the derived values
#: then contradict the file's own column statistics, and the aggregate fails with
#: *"Perfect hash aggregate: aggregate group 260 exceeded total groups 16 … your
#: statistics are corrupt"* — a message that names neither the file nor the real
#: problem. Measured both ways: with inference on, a hour-22 file moved into
#: ``hr=21`` raises that error; with it off, the file correctly reports hour 22.
#: The whole reason the partition columns are written into the file is so a copied
#: or misfiled hour cannot forget which hour it is, and this flag is what collects
#: on that.
LAKE_COUNTS: Final = """
SELECT dt, hr, count(*) AS events
FROM read_parquet($glob, hive_partitioning=false)
GROUP BY dt, hr
"""


class Problem(StrEnum):
    """What is wrong with one hour."""

    ABSENT = "absent"
    """The ledger claims this hour is in the lake and no file is there."""

    SIZE = "size"
    """A file is there and is not the one that was recorded."""

    COUNT = "count"
    """A file is there and holds a different number of events than recorded."""

    UNRECORDED = "unrecorded"
    """A file is there that no ledger row claims. Surplus, not a falsehood."""


#: The problems that make the ledger's own claims untrue. Kept as data rather
#: than an ``if`` chain so the exit code and the report cannot disagree about
#: which findings are serious.
UNBACKED: Final[frozenset[Problem]] = frozenset({Problem.ABSENT, Problem.SIZE, Problem.COUNT})

#: The exit code that means "the record claims hours the lake does not hold", as
#: opposed to any other non-zero exit, which means this check did not finish.
#:
#: It was **1** until something began to act on the answer. That was safe only
#: while nothing branched on it: 1 is also what an unhandled exception exits with,
#: so a caller could not tell "the lake disagrees with the record" from "this
#: command crashed" — and the two call for opposite responses. A repair that
#: treats a crash as a finding will delete and re-fetch on the strength of a
#: traceback.
#:
#: Three, matching the marts check, and for the same measured reason: 0, 1 and 2
#: are claimed by convention (success, error, usage), so a code carrying
#: application meaning starts at 3. The usage code is not theoretical here — the
#: command-line framework returns 2 for an unknown flag, before this module runs
#: at all.
#:
#: Fixed *before* the first caller that branches on it was written, which is the
#: only cheap moment. The identical collision was found in the marts check after
#: its wrapper existed, and it had to be watched rebuilding published aggregates
#: on a misspelled flag before it was believed.
UNBACKED_EXIT_CODE: Final = 3


@dataclass(frozen=True)
class Finding:
    """One hour that does not check out."""

    day: date
    hour: int
    problem: Problem
    detail: str

    @property
    def unbacked(self) -> bool:
        """Whether this finding means a ledger claim is untrue."""
        return self.problem in UNBACKED


@dataclass
class VerifyReport:
    """What the check examined and what it found."""

    claimed: int = 0
    """Hours the ledger says are in the lake."""

    agreed: int = 0
    """Hours whose file matched every check that ran."""

    unsized: int = 0
    """Hours whose row carries no size, so only presence could be checked."""

    counted: bool = False
    """Whether event counts were compared (the lake was read)."""

    findings: list[Finding] = field(default_factory=list)

    @property
    def unbacked(self) -> list[Finding]:
        """Findings that make a ledger claim untrue."""
        return [finding for finding in self.findings if finding.unbacked]

    @property
    def ok(self) -> bool:
        """Whether every claim the ledger makes is backed by a file."""
        return not self.unbacked

    def as_dict(self) -> dict[str, object]:
        """Flat counts for one log line — every finding reduced to a number."""
        return {
            "claimed": self.claimed,
            "agreed": self.agreed,
            "unsized": self.unsized,
            "counted": self.counted,
            "unbacked": len(self.unbacked),
            "surplus": len(self.findings) - len(self.unbacked),
        }


def lake_files(lake_dir: Path) -> Iterator[tuple[date, int, Path]]:
    """Every ``dt=…/hr=…`` partition file present, with the hour its path claims.

    Yields nothing for a lake that does not exist yet, rather than raising: an
    empty lake and a missing one are the same fact to a reader, and a first run
    against a fresh deployment is not an error.

    A directory whose name does not parse is skipped with a warning rather than
    failing the run — the lake is a directory anybody can drop a file into, and
    one stray folder should not stop a check on thousands of real hours.
    """
    for day_dir in sorted(lake_dir.glob("dt=*")):
        try:
            day = date.fromisoformat(day_dir.name.removeprefix("dt="))
        except ValueError:
            logger.warning("skipping unparseable partition directory: %s", day_dir)
            continue
        for hour_dir in sorted(day_dir.glob("hr=*")):
            try:
                hour = int(hour_dir.name.removeprefix("hr="))
            except ValueError:
                logger.warning("skipping unparseable partition directory: %s", hour_dir)
                continue
            path = hour_dir / PARQUET_FILENAME
            if path.exists():
                yield day, hour, path


def _counts_by_hour(lake_dir: Path) -> dict[tuple[date, int], int]:
    """Row counts per hour, from one query over every file in the lake.

    Reads the lake exactly the way it is meant to be read — one scan of two
    columns across every partition — instead of opening files one at a time.
    Returns an empty mapping for an empty lake: DuckDB raises rather than
    returning no rows when a glob matches nothing, and "no files" is a state this
    check has to handle rather than a failure.
    """
    glob = str(lake_dir / "**" / PARQUET_FILENAME)
    con = duckdb.connect()
    try:
        rows = con.execute(LAKE_COUNTS, {"glob": glob}).fetchall()
    except duckdb.IOException:
        # No file matched the pattern. Anything else — a malformed file, a
        # missing column — is a fact about the lake worth failing on loudly.
        logger.info("no lake files matched %s", glob)
        return {}
    finally:
        con.close()
    return {(row[0], int(row[1])): int(row[2]) for row in rows}


async def verify_lake(
    connection: Connection,
    *,
    lake_dir: Path,
    check_counts: bool = False,
) -> VerifyReport:
    """Compare every ingested ledger row against the lake, and the lake against it.

    ``check_counts`` opts into reading the files. Presence and size come from one
    ``stat`` each, which is why they are the default: the ledger already records
    the byte count, so comparing it costs nothing and catches a truncated or
    replaced file that mere existence would pass. Counting rows means scanning
    every partition, which is cheap for a week and not for a decade — so it is a
    choice the caller makes rather than a cost the default imposes.
    """
    report = VerifyReport(counted=check_counts)
    claims = await ingested_hours(connection)
    report.claimed = len(claims)
    counts = _counts_by_hour(lake_dir) if check_counts else {}

    for claim in claims:
        finding = _check(claim, lake_dir=lake_dir, counts=counts if check_counts else None)
        if finding is not None:
            report.findings.append(finding)
            continue
        if claim.bytes is None:
            report.unsized += 1
        report.agreed += 1

    # The other direction. A settled hour is never rescanned, so a file whose row
    # was lost is work the loop will silently redo — worth knowing, and not a
    # reason to fail, because no reported number is wrong because of it.
    claimed_hours = {(claim.day, claim.hour) for claim in claims}
    for day, hour, path in lake_files(lake_dir):
        if (day, hour) not in claimed_hours:
            report.findings.append(
                Finding(day, hour, Problem.UNRECORDED, f"file with no ingested row: {path}")
            )

    logger.info("lake verification: %s", report.as_dict())
    return report


def _check(
    claim: HourRecord,
    *,
    lake_dir: Path,
    counts: dict[tuple[date, int], int] | None,
) -> Finding | None:
    """The first thing wrong with one claimed hour, or ``None`` if it checks out.

    Stops at the first problem on purpose: a file that is absent has no size, and
    reporting three findings for one hour would make the count of unbacked hours
    disagree with the number of hours that are actually unbacked.
    """
    path = partition_dir(lake_dir, claim.day, claim.hour) / PARQUET_FILENAME
    if not path.exists():
        return Finding(
            claim.day,
            claim.hour,
            Problem.ABSENT,
            f"recorded as {claim.events:,} events; no file at {path}",
        )

    if claim.bytes is not None:
        actual = path.stat().st_size
        if actual != claim.bytes:
            return Finding(
                claim.day,
                claim.hour,
                Problem.SIZE,
                f"recorded {claim.bytes:,} bytes, file is {actual:,}",
            )

    if counts is not None:
        actual_events = counts.get((claim.day, claim.hour))
        if actual_events is None:
            # The file is on disk but contributed no rows to a query grouped by
            # the hour stored inside it — so it holds some *other* hour, which is
            # the failure the in-file partition columns exist to make visible.
            return Finding(
                claim.day,
                claim.hour,
                Problem.COUNT,
                f"file exists but holds no rows for dt={claim.day} hr={claim.hour}",
            )
        if actual_events != claim.events:
            return Finding(
                claim.day,
                claim.hour,
                Problem.COUNT,
                f"recorded {claim.events:,} events, file holds {actual_events:,}",
            )

    return None
