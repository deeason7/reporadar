"""Converge the lake on the published archive: no schedule, only a difference.

There is no run to miss here, because there is no run. There is a desired state —
every closed hour is in the lake — and a loop that repeatedly asks what is
missing and closes the gap. Downtime, a partial failure and an hour published
late all resolve on the next pass, with nothing to replay and no catch-up mode to
write. A timer cannot substitute for this: a missed timer fires once when the
machine comes back, not once per interval it slept through, so six hours of
downtime yields one run and five permanent holes. Since a gap scan is needed
either way, the schedule collapses into the scan and stops being a concern.

The same pass serves both callers. The always-on loop runs it over a short
trailing window and leaves failures alone; an explicit range runs it once over
whatever it is given and picks failures back up, which is how a fix reaches the
hours it fixed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final

from reporadar.ingest.archive import DEFAULT_BASE_URL
from reporadar.ingest.hour import DEFAULT_PUBLICATION_GRACE, HourReport, hour_end, ingest_hour
from reporadar.ingest.ledger import Connection, create_schema, pending_hours
from reporadar.ingest.metrics import ArchiveCounters
from reporadar.ingest.signals import interruptible_sleep

logger = logging.getLogger(__name__)

#: Hours to ingest at once. Bounded because the publisher is somebody else's
#: server and a backfill is the one caller that could hammer it: politeness is a
#: property of the client, not of the range it was asked for.
DEFAULT_CONCURRENCY: Final = 3

#: How long between scans. The resource changes hourly, so scanning faster buys
#: nothing but promptness after publication — and polling faster than a resource
#: changes is a lesson this project has already paid for once. Fifteen minutes
#: bounds the lag between an hour appearing and it landing.
DEFAULT_SCAN_INTERVAL_S: Final = 900.0

#: How far back each scan looks. Unbounded would generate a calendar back to 2011
#: on every pass to rediscover the same settled rows; too short and downtime
#: longer than the window leaves hours the loop will never revisit. Three days
#: covers publication lag plus a weekend outage — and anything older is what an
#: explicit range is for, deliberately, because a silent unbounded catch-up is
#: how a loop turns one bad night into a week of surprise traffic.
DEFAULT_LOOKBACK_DAYS: Final = 3


class _Serialised:
    """One connection, safe to share across concurrent hours.

    Every driver worth using refuses two overlapping operations on a single
    connection, and the ledger writes here are issued from whatever hour finishes
    first. The work worth parallelising is the download and the conversion, which
    are seconds; a ledger write is microseconds, so serialising them costs
    nothing and removes the failure entirely.

    Deliberately a wrapper rather than a rule for callers to remember: a pool
    would work too, and this keeps the loop usable with a plain connection while
    making the unsafe arrangement unreachable rather than merely discouraged.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._lock = asyncio.Lock()

    async def execute(self, query: str, *args: object) -> object:
        async with self._lock:
            return await self._connection.execute(query, *args)

    async def fetch(self, query: str, *args: object) -> Sequence[Sequence[object]]:
        async with self._lock:
            return await self._connection.fetch(query, *args)


async def converge_once(
    connection: Connection,
    *,
    archive_dir: Path,
    lake_dir: Path,
    now: datetime,
    first_day: date,
    last_day: date,
    concurrency: int = DEFAULT_CONCURRENCY,
    retry_failed: bool = False,
    base_url: str = DEFAULT_BASE_URL,
    grace: timedelta = DEFAULT_PUBLICATION_GRACE,
    counters: ArchiveCounters | None = None,
) -> ArchiveCounters:
    """One pass: ask what is outstanding, ingest it, and report what happened.

    Hours that have not closed yet are dropped rather than attempted. The ledger
    scan answers "which hours are not settled", and every hour of today is
    honestly unsettled — but a partial hour filed as a whole one is unrecoverable
    without noticing, so the boundary is enforced here rather than left to the
    caller's choice of range.
    """
    if concurrency < 1:
        raise ValueError(f"concurrency must be at least 1, got {concurrency}")
    counters = counters if counters is not None else ArchiveCounters()

    outstanding = await pending_hours(connection, first_day, last_day, retry_failed=retry_failed)
    due = [(day, hour) for day, hour in outstanding if hour_end(day, hour) <= now]
    counters.record_pass(due=len(due))
    if not due:
        logger.info("archive scan %s..%s: nothing outstanding", first_day, last_day)
        return counters

    logger.info(
        "archive scan %s..%s: %d hour(s) outstanding, %d at a time",
        first_day,
        last_day,
        len(due),
        concurrency,
    )
    shared = _Serialised(connection)
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
                grace=grace,
            )

    reports = await asyncio.gather(*(one(day, hour) for day, hour in due))
    for report in reports:
        counters.record_hour(status=report.status, events=report.events)
    logger.info("archive scan done: %s", counters.as_dict())
    return counters


async def converge_forever(
    connection: Connection,
    *,
    archive_dir: Path,
    lake_dir: Path,
    concurrency: int = DEFAULT_CONCURRENCY,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    interval_s: float = DEFAULT_SCAN_INTERVAL_S,
    base_url: str = DEFAULT_BASE_URL,
    grace: timedelta = DEFAULT_PUBLICATION_GRACE,
    max_passes: int | None = None,
    stop: asyncio.Event | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ArchiveCounters:
    """Scan and ingest on an interval until stopped, returning the run's counters.

    Runs indefinitely by default. ``max_passes`` bounds a run for tests and
    one-off catch-ups; ``stop`` ends it after the current pass rather than
    mid-hour, so a shutdown never leaves a downloaded hour unrecorded.

    ``clock`` is injected for the same reason the ledger takes ``now``: this loop
    decides which hours have closed and which absent hours have waited long
    enough, and both are answers about time. A test that cannot move the clock
    cannot reach either.
    """
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be at least 1, got {lookback_days}")
    counters = ArchiveCounters()
    await create_schema(connection)
    while True:
        if stop is not None and stop.is_set():
            break
        if max_passes is not None and counters.passes >= max_passes:
            break
        now = clock()
        last_day = now.date()
        await converge_once(
            connection,
            archive_dir=archive_dir,
            lake_dir=lake_dir,
            now=now,
            first_day=last_day - timedelta(days=lookback_days - 1),
            last_day=last_day,
            concurrency=concurrency,
            base_url=base_url,
            grace=grace,
            counters=counters,
        )
        await interruptible_sleep(interval_s, stop)
    logger.info("archive ingest stopped: %s", counters.as_dict())
    return counters
