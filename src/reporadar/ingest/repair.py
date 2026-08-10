"""Treat what ``verify`` diagnoses: re-derive the record for hours no file backs.

``verify`` has been able to find hours the record claims and the lake does not
hold since the day it was written, and nothing has ever been able to fix one. The
operations guide said to re-run an explicit range, and that was measured and does
not work: the convergence loop asks the ledger which hours are unsettled, and a
row saying ``ingested`` is settled **whether or not its file exists**, so the
hours needing repair are exactly the ones the loop excludes. A full backfill over
the affected days reported forty-one hours due, forty-one ingested, nothing
outstanding — and the same thirty-two failures before and after.

**A loop that converges on the record cannot repair the record.** That is not a
bug in the loop; it is what makes the loop cheap and non-destructive. So the
treatment is a separate, explicit command, and the loop is left alone.

*Why the loop must not simply check the files instead.* Deleting a partition
directory is a documented way to reclaim disk. A loop that treated a missing file
as work to do would re-download those hours for ever, fighting the operator and
spending API budget to undo a deliberate act. The record is the right level
trigger for a loop; it is the wrong one for a repair, and the answer is a second
tool rather than a changed first one.

**The reconciliation is the point, not a nicety.** Before a claim is destroyed it
is written down, and after the hour is fetched again the two are compared. Doing
this by hand is what turned a routine repair into a finding: thirty-one hours
reproduced their recorded counts exactly, and that agreement is the only reason
the *one* hour that disagreed — recorded as 100 events, actually 165,892 — was
legible as a test fixture written into the real ledger rather than as noise. A
repair that silently fixed would have erased the evidence along with the fault.

Nothing here decides *whether* an hour is wrong. That is ``verify``'s question,
and this module asks it rather than re-deriving it, so the two can never disagree
about what is broken.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Final

import httpx

from reporadar.ingest.archive import DEFAULT_BASE_URL
from reporadar.ingest.converge import SerialisedConnection
from reporadar.ingest.hour import DEFAULT_PUBLICATION_GRACE, HourReport, ingest_hour
from reporadar.ingest.ledger import (
    Connection,
    HourRecord,
    HourStatus,
    forget_ingested_hours,
    ingested_hours,
)
from reporadar.ingest.verify import Finding, verify_lake

logger = logging.getLogger(__name__)

#: How many hours to re-fetch at once. Lower than the backfill's three, measured
#: rather than chosen for caution: at three, roughly sixty rapid requests drew
#: thirteen dropped connections from the publisher inside seven tenths of a
#: second. Nothing was lost — the loop leaves a failed fetch outstanding rather
#: than recording it — but a repair is run by somebody watching it, on hours that
#: are already known to be broken, and finishing slowly beats finishing partly.
DEFAULT_REPAIR_CONCURRENCY: Final = 1

#: The exit code that means "some hours are still unrepaired", as opposed to any
#: other non-zero exit, which means the repair did not run. Same value and same
#: reasoning as the other two application codes in this project: 0, 1 and 2 are
#: claimed by convention, so an application meaning starts at 3.
INCOMPLETE_EXIT_CODE: Final = 3


@dataclass(frozen=True)
class Reconciliation:
    """One hour: what the record claimed, and what fetching it again found.

    Both numbers are kept even when they agree, because the agreement is the
    evidence. A list of only the disagreements cannot distinguish "the other
    thirty-one were checked and matched" from "the other thirty-one were not
    checked", and those license completely different conclusions about the one
    row that differs.
    """

    day: date
    hour: int
    claimed_events: int
    """What the destroyed row said the lake held for this hour."""

    problem: str
    """Why ``verify`` called this hour unbacked."""

    outcome: HourStatus | None
    """What re-ingesting produced. ``None`` means nothing was recorded at all."""

    actual_events: int | None
    """What the fresh fetch found, or ``None`` if it did not land."""

    detail: str

    @property
    def recovered(self) -> bool:
        """Whether the hour is now genuinely in the lake."""
        return self.outcome is HourStatus.INGESTED

    @property
    def agrees(self) -> bool:
        """Whether the destroyed claim matched what the hour really holds."""
        return self.recovered and self.actual_events == self.claimed_events

    @property
    def verdict(self) -> str:
        """The word this row gets in the report."""
        if not self.recovered:
            return "NOT RECOVERED"
        return "match" if self.agrees else "DIFFERS"

    def __str__(self) -> str:
        actual = "—" if self.actual_events is None else f"{self.actual_events:,}"
        return (
            f"{self.day} {self.hour:02d}  claimed {self.claimed_events:>12,} "
            f"-> {actual:>12}  {self.verdict}"
        )


@dataclass
class RepairReport:
    """What the repair examined, destroyed and re-derived."""

    dry_run: bool = False

    unbacked: list[Finding] = field(default_factory=list)
    """What ``verify`` found. Populated even on a dry run."""

    unclearable: list[tuple[date, int]] = field(default_factory=list)
    """Hours whose false row could not be removed, so they were not re-fetched.

    Never expected: every hour here came from the ledger's own ingested rows a
    moment earlier. It is carried rather than asserted because the alternative is
    re-fetching an hour whose row still says ``ingested`` — where the upsert's
    no-downgrade guard would swallow any non-success outcome and leave the lie in
    place, repaired in appearance only.
    """

    reconciliations: list[Reconciliation] = field(default_factory=list)

    @property
    def recovered(self) -> list[Reconciliation]:
        return [item for item in self.reconciliations if item.recovered]

    @property
    def disagreed(self) -> list[Reconciliation]:
        """Hours that came back with a different count than was claimed.

        Not a failure of the repair — the hour is now correct. It is a finding
        *about the destroyed record*, and the most interesting output this command
        has.
        """
        return [item for item in self.recovered if not item.agrees]

    @property
    def unrecovered(self) -> list[Reconciliation]:
        return [item for item in self.reconciliations if not item.recovered]

    @property
    def ok(self) -> bool:
        """Whether every hour that needed repairing is now backed by a file.

        A dry run is ``ok`` only when there was nothing to do, so ``--dry-run``
        against a broken lake exits non-zero and says what it would have fixed —
        the same shape as every other check here.
        """
        if self.dry_run:
            return not self.unbacked
        return not self.unrecovered and not self.unclearable

    def as_dict(self) -> dict[str, object]:
        """The shape a log line and an exit message want."""
        return {
            "dry_run": self.dry_run,
            "unbacked": len(self.unbacked),
            "cleared": len(self.reconciliations),
            "recovered": len(self.recovered),
            "disagreed": len(self.disagreed),
            "unrecovered": len(self.unrecovered),
            "unclearable": len(self.unclearable),
        }


async def repair_unbacked(
    connection: Connection,
    *,
    lake_dir: Path,
    archive_dir: Path,
    now: datetime,
    concurrency: int = DEFAULT_REPAIR_CONCURRENCY,
    dry_run: bool = False,
    check_counts: bool = False,
    base_url: str = DEFAULT_BASE_URL,
    client: httpx.Client | None = None,
    grace: timedelta = DEFAULT_PUBLICATION_GRACE,
    keep_source: bool = True,
) -> RepairReport:
    """Find hours the record claims and no file backs, and re-derive them.

    ``check_counts`` is handed to ``verify`` unchanged, so this repairs exactly
    what that command reports at the same setting — including, at the cost of a
    scan, hours whose file is present and holds the wrong number of events.

    ``keep_source`` defaults to keeping, unlike the long-running commands. A
    repair is rare, watched, and follows something having already gone wrong;
    keeping the downloaded source is the cheap way for an operator to look at what
    actually arrived.
    """
    if concurrency < 1:
        # Not a politeness check. `_refetch` builds an `asyncio.Semaphore` from
        # this, and a semaphore of zero is never released by anybody — the repair
        # would hang for ever rather than fail, which is the worst way for a
        # destructive command to end after it has already deleted the rows.
        raise ValueError(f"concurrency must be at least 1, got {concurrency}")

    # Read the claims *before* asking what is broken. Both come from the same
    # table, and a row disappearing between the two reads would otherwise leave a
    # finding whose claim cannot be found — which is a state this would then have
    # to invent a value for.
    claims = {(claim.day, claim.hour): claim for claim in await ingested_hours(connection)}
    verified = await verify_lake(connection, lake_dir=lake_dir, check_counts=check_counts)
    report = RepairReport(dry_run=dry_run, unbacked=verified.unbacked)

    if not report.unbacked:
        logger.info("lake repair: nothing to repair; every claimed hour is backed by its file")
        return report

    targets = [(finding.day, finding.hour) for finding in report.unbacked]
    _announce(report.unbacked, claims)
    if dry_run:
        logger.info("lake repair: dry run, nothing was changed")
        return report

    # Destroy the false claims first, and only re-fetch what was really removed.
    # `record_hour` cannot correct these rows on its own: its upsert refuses to
    # move a row off `ingested`, so an hour that comes back missing, failed, or
    # not at all would leave its untrue row untouched and be reported as repaired.
    removed = set(await forget_ingested_hours(connection, targets))
    report.unclearable = [hour for hour in targets if hour not in removed]
    if report.unclearable:
        logger.error(
            "lake repair: %d hour(s) could not be cleared and were left alone: %s",
            len(report.unclearable),
            ", ".join(f"{day} {hour:02d}" for day, hour in report.unclearable),
        )

    reports = await _refetch(
        connection,
        sorted(removed),
        archive_dir=archive_dir,
        lake_dir=lake_dir,
        now=now,
        concurrency=concurrency,
        base_url=base_url,
        client=client,
        grace=grace,
        keep_source=keep_source,
    )

    problems = {(finding.day, finding.hour): finding.problem for finding in report.unbacked}
    report.reconciliations = [
        Reconciliation(
            day=hour_report.day,
            hour=hour_report.hour,
            claimed_events=_claimed_events(claims, hour_report.day, hour_report.hour),
            problem=str(problems[(hour_report.day, hour_report.hour)]),
            outcome=hour_report.status,
            actual_events=hour_report.events,
            detail=hour_report.detail,
        )
        for hour_report in reports
    ]

    for item in report.reconciliations:
        # Every row, not only the disagreements. See Reconciliation's docstring:
        # the matches are what make one mismatch mean something.
        logger.info("lake repair: %s", item)
    logger.info("lake repair: %s", report.as_dict())
    return report


def _announce(unbacked: list[Finding], claims: dict[tuple[date, int], HourRecord]) -> None:
    """Say what is about to be fetched, before fetching it.

    The size reported is what the *lake* recorded for those hours, because that is
    the only figure the record actually holds. The download is the published
    compressed source for each hour, which is a different quantity and is not
    guessed at here — an estimate assembled from a ratio nobody measured would be
    a number this project is not allowed to print.
    """
    recorded = sum(
        claim.bytes or 0
        for key, claim in claims.items()
        if key in {(f.day, f.hour) for f in unbacked}
    )
    logger.warning(
        "lake repair: %d hour(s) claimed by the record are not backed by their file. "
        "Their rows will be removed and each hour fetched again from the publisher. "
        "The lake recorded %.1f MB for these hours; the download is the published "
        "source for each of them.",
        len(unbacked),
        recorded / 1_000_000,
    )


async def _refetch(
    connection: Connection,
    hours: list[tuple[date, int]],
    *,
    archive_dir: Path,
    lake_dir: Path,
    now: datetime,
    concurrency: int,
    base_url: str,
    client: httpx.Client | None,
    grace: timedelta,
    keep_source: bool,
) -> list[HourReport]:
    """Ingest each hour again, bounded, over one shared connection."""
    if not hours:
        return []
    shared = SerialisedConnection(connection)
    limit = asyncio.Semaphore(concurrency)

    async def one(day: date, hour: int) -> HourReport:
        async with limit:
            return await ingest_hour(
                day,
                hour,
                connection=shared,
                archive_dir=archive_dir,
                lake_dir=lake_dir,
                now=now,
                base_url=base_url,
                client=client,
                grace=grace,
                keep_source=keep_source,
            )

    return list(await asyncio.gather(*(one(day, hour) for day, hour in hours)))


def _claimed_events(claims: dict[tuple[date, int], HourRecord], day: date, hour: int) -> int:
    """What the destroyed row said, refusing to invent a number if it is gone.

    Raises rather than defaulting to zero. This value is one half of the
    comparison the whole command exists to print, and a fabricated zero would
    report every hour as a disagreement of exactly its own size — a false finding
    that looks precise.
    """
    claim = claims.get((day, hour))
    if claim is None or claim.events is None:
        raise LookupError(
            f"no recorded event count for {day} {hour:02d}; the ledger changed underneath "
            "this repair and the comparison it exists to make cannot be trusted"
        )
    return claim.events
