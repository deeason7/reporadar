"""Ingest one archive hour: fetch it, file it in the lake, record what happened.

Deliberately a plain function rather than a scheduler's task. Whatever drives it
later — a loop, an explicit range, or a workflow engine — becomes a *caller*
rather than a dependency, so replacing the driver never touches the ingest.

Its contract is that the ledger ends up agreeing with what is on disk. An hour
that landed is recorded with its counts; an hour the publisher will never have is
recorded so the scan stops asking; and an hour that might still arrive is
recorded **nowhere**, because absence is exactly what makes the next pass pick it
up again. Writing a row for "not yet" would settle a question nobody has answered.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Final

import duckdb
import httpx

from reporadar.ingest.archive import DEFAULT_BASE_URL, download_hour
from reporadar.ingest.lake import PartitionMismatchError, write_hour
from reporadar.ingest.ledger import Connection, HourRecord, HourStatus, record_hour

logger = logging.getLogger(__name__)

#: How long after an hour closes the publisher is still given to release it before
#: the hour is written off as one that will never arrive.
#:
#: This is the one number here that can lose data silently, because ``missing`` is
#: a settled status: nothing revisits an hour once it carries one, so a grace
#: shorter than the publisher's worst late release turns a slow hour into a
#: permanent hole that every counter reports as decided. Publication normally
#: happens a few minutes after the hour closes, so a day is two orders of
#: magnitude of headroom, and being generous costs only that a genuinely absent
#: hour is re-requested for a day first. Asymmetric risks deserve asymmetric
#: defaults.
DEFAULT_PUBLICATION_GRACE: Final = timedelta(hours=24)

# Which DuckDB failures mean "this hour cannot be trusted" as opposed to "this
# code is wrong" — measured, not guessed: a file that is not a gzip raises
# IOException, while an unknown function raises CatalogException. Only the first
# kind is recorded as a failed hour. Catching every duckdb.Error would record a
# query bug as data corruption on every hour in the range, quietly converting a
# five-minute fix into a backfill.
_UNTRUSTWORTHY_HOUR: Final = (
    duckdb.IOException,
    duckdb.InvalidInputException,
    duckdb.ConversionException,
)


@dataclass(frozen=True)
class HourReport:
    """What one ingest attempt did — including deciding to record nothing.

    ``status`` is ``None`` when the hour was deliberately left unrecorded: it is
    still work to do and the next pass will try again. That is a different fact
    from all three statuses, so it is a different value rather than a fourth
    status — a vocabulary that could express "undecided" is a vocabulary in which
    an undecided hour can be marked settled.
    """

    day: date
    hour: int
    status: HourStatus | None
    detail: str
    events: int | None = None
    bytes: int | None = None

    @property
    def recorded(self) -> bool:
        """Whether this attempt wrote a ledger row."""
        return self.status is not None


def hour_end(day: date, hour: int) -> datetime:
    """When the given UTC hour finishes — the instant it becomes publishable."""
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be 0-23, got {hour}")
    return datetime.combine(day, time(hour=hour), tzinfo=UTC) + timedelta(hours=1)


async def ingest_hour(
    day: date,
    hour: int,
    *,
    connection: Connection,
    archive_dir: Path,
    lake_dir: Path,
    now: datetime,
    base_url: str = DEFAULT_BASE_URL,
    client: httpx.Client | None = None,
    grace: timedelta = DEFAULT_PUBLICATION_GRACE,
    keep_source: bool = True,
) -> HourReport:
    """Fetch one closed hour, convert it, and record the outcome.

    ``now`` is supplied rather than read here for the same reason the ledger takes
    it: the caller owns the clock. It matters more here, because the decision this
    function makes about an absent hour is a decision *about time*, and a test
    that cannot move the clock cannot reach it.

    The blocking work runs in worker threads. Downloading and converting are both
    synchronous, and running them inline would make a bounded-concurrency range
    ingest strictly sequential — the semaphore would bound a queue of one.

    Whether an hour *needs* ingesting is not asked here. That is the ledger scan's
    job, and keeping it out keeps this function a seam any scheduler can call.

    ``keep_source`` decides what happens to the downloaded archive after it has been
    converted. It defaults to keeping, so no caller loses a file by forgetting the
    argument; the long-running commands pass ``False``, because a deployment that
    keeps every source file grows two and a half times faster than one that does not.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware; naive datetimes are ambiguous")
    closed_at = hour_end(day, hour)
    if now < closed_at:
        raise ValueError(
            f"hour {day} {hour:02d}:00 UTC has not closed yet "
            f"(closes {closed_at:%Y-%m-%d %H:%M} UTC, now {now:%Y-%m-%d %H:%M} UTC)"
        )

    try:
        archive_path = await asyncio.to_thread(
            download_hour, day, hour, archive_dir, base_url, client
        )
    except httpx.HTTPStatusError as exc:
        return await _absent(
            connection, day, hour, now=now, closed_at=closed_at, grace=grace, response=exc.response
        )
    except httpx.HTTPError as exc:
        # A timeout, a DNS failure, a dropped connection: the hour may well be
        # there next pass, so nothing is recorded and it stays outstanding.
        detail = f"fetch failed: {type(exc).__name__}"
        logger.warning("archive hour %s %02d not fetched (%s); left outstanding", day, hour, detail)
        return HourReport(day, hour, None, detail)

    try:
        written = await asyncio.to_thread(write_hour, archive_path, lake_dir, day, hour)
    except (PartitionMismatchError, *_UNTRUSTWORTHY_HOUR) as exc:
        detail = f"{type(exc).__name__}: {exc}"
        await record_hour(
            connection, HourRecord(day, hour, HourStatus.FAILED, detail=detail), now=now
        )
        logger.error("archive hour %s %02d arrived and could not be trusted: %s", day, hour, detail)
        return HourReport(day, hour, HourStatus.FAILED, detail)

    await record_hour(
        connection,
        HourRecord(
            day,
            hour,
            HourStatus.INGESTED,
            events=written.events,
            bytes=written.bytes_written,
        ),
        now=now,
    )
    logger.info(
        "archive hour %s %02d ingested: %d events, %d bytes → %s",
        day,
        hour,
        written.events,
        written.bytes_written,
        written.path,
    )
    if not keep_source:
        # Strictly after the row exists. Until then the hour is still outstanding,
        # and the next pass needs either this file or a re-download; after it, the
        # hour is settled and nothing will ask for the source again.
        _discard_source(archive_path, day, hour)
    return HourReport(
        day,
        hour,
        HourStatus.INGESTED,
        f"ingested {written.events} events",
        events=written.events,
        bytes=written.bytes_written,
    )


def _discard_source(path: Path, day: date, hour: int) -> None:
    """Delete a converted hour's compressed source, saying how much it reclaimed.

    The source is a cache, not a record: the downloader skips the network whenever
    the final file is already there, and the archive publishes the same immutable
    hour indefinitely, so the only thing this file buys after conversion is a
    re-download that nobody is going to ask for. What the ledger points at is the
    columnar copy.

    Reported rather than silent, because a version that used to keep these files
    now removes them, and the first run after that change should be able to say so
    out loud. The bytes are the interesting part: they are what the disk gets back.

    A filesystem that refuses is a disk-space problem and not a data problem — the
    hour is converted and recorded, and both of those are true whatever happens
    here — so it warns and returns rather than turning a successful ingest into a
    failed one.
    """
    try:
        reclaimed = path.stat().st_size
        path.unlink()
    except OSError as exc:
        logger.warning(
            "archive hour %s %02d: could not remove the converted source %s (%s); "
            "it is safe to delete by hand",
            day,
            hour,
            path,
            exc,
        )
        return
    logger.info(
        "archive hour %s %02d: removed the converted source, %d bytes reclaimed",
        day,
        hour,
        reclaimed,
    )


async def _absent(
    connection: Connection,
    day: date,
    hour: int,
    *,
    now: datetime,
    closed_at: datetime,
    grace: timedelta,
    response: httpx.Response,
) -> HourReport:
    """Decide what an unsuccessful HTTP status means for an hour's record.

    Only 404 carries information about the hour itself; every other status is a
    statement about the request or the server, which says nothing about whether
    the hour exists.
    """
    if response.status_code != httpx.codes.NOT_FOUND:
        detail = f"fetch returned HTTP {response.status_code}"
        logger.warning("archive hour %s %02d not fetched (%s); left outstanding", day, hour, detail)
        return HourReport(day, hour, None, detail)

    waited = now - closed_at
    elapsed = f"{waited // timedelta(hours=1)}h after the hour closed"
    if waited < grace:
        # Absent and still within the window the publisher is allowed: the
        # right record is no record, so the next pass tries again.
        logger.info("archive hour %s %02d not published yet (%s)", day, hour, elapsed)
        return HourReport(day, hour, None, f"not published yet, {elapsed}")

    detail = f"still absent {elapsed}"
    await record_hour(connection, HourRecord(day, hour, HourStatus.MISSING, detail=detail), now=now)
    logger.warning("archive hour %s %02d written off as never published (%s)", day, hour, elapsed)
    return HourReport(day, hour, HourStatus.MISSING, detail)
