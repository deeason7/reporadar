"""The archive-hours ledger: which hours are in the lake, and which are not.

One row per published hour, and one table doing four jobs — the completion
record, the gap detector, the store for the capture KPI, and the source a
dashboard panel reads. It exists under any scheduler, because a control plane's
idea of what ran can drift from what is actually on disk, and the disk wins.

**The ledger is what makes the ingest loop level-triggered.** Nothing here
records an intention to run; each row records an outcome, so "what still needs
doing" is a query rather than a schedule. There is no missed-run failure class
because there is no run — only a desired state (every closed hour is in the lake)
and a loop that converges on it. Downtime, a partial failure, and an hour
published late all heal on the next pass without anybody replaying anything.

The status vocabulary is deliberately small, and it encodes the one distinction
that generic retry policies cannot express: an hour that is *not published yet*
is not an error and gets no row at all — its absence is what makes the next pass
pick it up — while an hour the publisher will never have is a fact worth writing
down, so the loop stops asking. A structurally broken hour is a third thing
again: retrying cannot fix it, but it must not be filed as done either.

*Not a hypertable, deliberately.* This table takes 24 rows a day, so time
partitioning would buy nothing and cost a dependency on an extension for a table
whose whole value is being trivially queryable.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Final, Protocol

logger = logging.getLogger(__name__)


class Connection(Protocol):
    """The slice of an async Postgres connection the ledger needs.

    Declared here rather than widening the store's protocol with a ``fetch`` the
    store never calls: each module names the narrowest surface it actually uses,
    and one real driver connection satisfies both. Structural typing is what makes
    that free — nothing has to inherit anything, so the whole module is testable
    against a double with two methods on it.
    """

    async def execute(self, query: str, *args: object) -> object: ...

    async def fetch(self, query: str, *args: object) -> Sequence[Sequence[object]]: ...


class HourStatus(StrEnum):
    """What became of one archive hour.

    A string enum so the value stored in the database is the value read in
    Python: a lookup table would add a join and an integer nobody can read in a
    dashboard, for a vocabulary that changes about once a year.
    """

    INGESTED = "ingested"
    """In the lake, verified, and counted. The only success."""

    MISSING = "missing"
    """The publisher does not have this hour and is not going to.

    Hundreds of hours are absent from the published record and are never
    backfilled, so this is an expected outcome rather than a failure — recorded
    so the loop stops asking, and counted so a *rise* in it is still visible.
    """

    FAILED = "failed"
    """The hour was fetched and could not be trusted — retrying will not help.

    Distinct from ``MISSING`` because the cause is here rather than upstream, and
    distinct from success because it must never be counted as coverage. The
    automatic loop skips these so it cannot spin; an explicit backfill retries
    them, which is how a fix gets applied.
    """


#: Statuses the convergence loop treats as settled. Anything else — including an
#: hour with no row at all — is work still to do.
TERMINAL: Final[frozenset[HourStatus]] = frozenset({HourStatus.INGESTED, HourStatus.MISSING})

# Idempotent DDL, matching the store's stance: first creation does not justify a
# migration tool; a schema that starts *evolving* does. The CHECK constraints are
# not decoration — they make the database refuse a nonsense row instead of
# trusting every present and future writer to be careful.
CREATE_TABLE: Final = """
CREATE TABLE IF NOT EXISTS archive_hours (
    day         date        NOT NULL,
    hour        smallint    NOT NULL CHECK (hour BETWEEN 0 AND 23),
    status      text        NOT NULL CHECK (status IN ('ingested', 'missing', 'failed')),
    events      bigint      CHECK (events >= 0),
    bytes       bigint      CHECK (bytes >= 0),
    detail      text,
    recorded_at timestamptz NOT NULL,
    -- An ingested hour must carry its count: it is the capture KPI's input, and a
    -- success with a null count is a row that looks measured and is not.
    CONSTRAINT ingested_hours_are_counted
        CHECK (status <> 'ingested' OR events IS NOT NULL),
    PRIMARY KEY (day, hour)
)
"""

# Upsert rather than ``ON CONFLICT DO NOTHING``, which is what the events table
# uses and what would be wrong here. Those rows are immutable facts about an
# event; these rows are the *current* status of an hour, and that legitimately
# changes: an hour recorded missing can be published late, and a rebuilt hour has
# a new count. DO NOTHING would strand the first case forever — the gap scan would
# read a stale `missing` and never look again.
#
# The WHERE clause is the guard that makes the update safe: a settled success is
# never downgraded, so a transient fetch failure cannot erase a real ingest, while
# a re-ingest still refreshes the counts.
RECORD_HOUR: Final = """
INSERT INTO archive_hours (day, hour, status, events, bytes, detail, recorded_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (day, hour) DO UPDATE SET
    status      = EXCLUDED.status,
    events      = EXCLUDED.events,
    bytes       = EXCLUDED.bytes,
    detail      = EXCLUDED.detail,
    recorded_at = EXCLUDED.recorded_at
WHERE archive_hours.status <> 'ingested' OR EXCLUDED.status = 'ingested'
"""

# Left join against a generated calendar rather than reading the ledger and
# subtracting in Python: the answer wanted is "hours in this range that are not
# settled", and the rows that do not exist are most of the answer. Asking the
# database for absence keeps the whole scan one round trip whatever the range.
PENDING_HOURS: Final = """
SELECT calendar.day, calendar.hour
FROM (
    SELECT day::date AS day, hour
    FROM generate_series($1::date, $2::date, interval '1 day') AS day,
         generate_series(0, 23) AS hour
) AS calendar
LEFT JOIN archive_hours AS ledger
       ON ledger.day = calendar.day AND ledger.hour = calendar.hour
WHERE ledger.status IS NULL OR ledger.status <> ALL($3::text[])
ORDER BY calendar.day, calendar.hour
"""

# The ::bigint cast is not cosmetic. Postgres widens sum() over a bigint column to
# numeric, which the driver hands back as Decimal — so without the cast this query
# returns a type the test double (handing back plain ints) never produces, and the
# suite would be green against a shape the database cannot emit. Casting in SQL
# fixes it at the source rather than widening every reader to accept both.
# Every hour the ledger claims is in the lake, with what it claims about it. The
# read side of RECORD_HOUR: those two columns are written as a description of a
# file, and nothing has ever checked that the description still matches.
INGESTED_HOURS: Final = """
SELECT day, hour, events, bytes
FROM archive_hours
WHERE status = 'ingested'
ORDER BY day, hour
"""

COUNT_BY_STATUS: Final = """
SELECT status, count(*)::bigint, coalesce(sum(events), 0)::bigint
FROM archive_hours
GROUP BY status
ORDER BY status
"""


@dataclass(frozen=True)
class HourRecord:
    """One ledger row, as the loop reports it."""

    day: date
    hour: int
    status: HourStatus
    events: int | None = None
    bytes: int | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.hour <= 23:
            raise ValueError(f"hour must be 0-23, got {self.hour}")
        if self.status is HourStatus.INGESTED and self.events is None:
            # The same rule the table's CHECK enforces, enforced here too so the
            # mistake is caught without a database and names the reason.
            raise ValueError("an ingested hour must carry its event count")


async def create_schema(connection: Connection) -> None:
    """Ensure the ledger table exists (idempotent; safe on every startup)."""
    await connection.execute(CREATE_TABLE)
    logger.info("archive_hours ledger ready")


async def record_hour(connection: Connection, record: HourRecord, *, now: datetime) -> None:
    """Write one hour's outcome, refreshing an unsettled row and never downgrading a success.

    ``now`` is passed in rather than read here so the caller owns the clock — a
    test that cannot pin the time is a test the machine can answer differently
    tomorrow.
    """
    await connection.execute(
        RECORD_HOUR,
        record.day,
        record.hour,
        str(record.status),
        record.events,
        record.bytes,
        record.detail,
        now,
    )


async def pending_hours(
    connection: Connection,
    first_day: date,
    last_day: date,
    *,
    retry_failed: bool = False,
) -> list[tuple[date, int]]:
    """Hours in the inclusive day range that are not settled, oldest first.

    ``retry_failed`` is the difference between the two callers it serves:
    the always-on loop leaves failures alone so it cannot spin on an hour that
    will never parse, while an explicit backfill picks them up, which is how a fix
    reaches the hours it fixes.
    """
    settled = [str(status) for status in sorted(TERMINAL)]
    if not retry_failed:
        settled.append(str(HourStatus.FAILED))
    rows = await connection.fetch(PENDING_HOURS, first_day, last_day, settled)
    return [(_as_date(row[0]), _as_int(row[1])) for row in rows]


async def ingested_hours(connection: Connection) -> list[HourRecord]:
    """Every hour recorded as in the lake, oldest first.

    Returns the write shape, because a row read back *is* a ledger row and a
    second near-identical dataclass would only invite the two to drift.

    ``bytes`` stays ``int | None``: the table requires an ingested hour to carry
    its event count and deliberately does not require its size, so a row written
    before that column meant anything cannot be size-checked. That is a different
    fact from "the size disagrees", and the type is what keeps them apart.
    """
    rows = await connection.fetch(INGESTED_HOURS)
    return [
        HourRecord(
            day=_as_date(row[0]),
            hour=_as_int(row[1]),
            status=HourStatus.INGESTED,
            events=_as_int(row[2]),
            bytes=None if row[3] is None else _as_int(row[3]),
        )
        for row in rows
    ]


async def status_counts(connection: Connection) -> dict[HourStatus, tuple[int, int]]:
    """Hours and events per status — the shape a dashboard panel and a log line want."""
    rows = await connection.fetch(COUNT_BY_STATUS)
    return {HourStatus(_as_str(row[0])): (_as_int(row[1]), _as_int(row[2])) for row in rows}


# A driver hands back whatever the server sent, so the column types are only known
# at runtime. These narrow at that boundary and *raise naming the column's actual
# type* rather than being cast away: the failure they exist for is a schema that has
# drifted from this module's DDL, and a confusing TypeError three frames later is a
# far worse way to learn that than a sentence saying so. This is where the Decimal
# from an uncast sum() would have surfaced.


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an integer ledger column, got {type(value).__name__}")
    return value


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"expected a text ledger column, got {type(value).__name__}")
    return value


def _as_date(value: object) -> date:
    if not isinstance(value, date):
        raise TypeError(f"expected a date ledger column, got {type(value).__name__}")
    return value
