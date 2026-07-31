"""The archive-hours ledger.

These tests pin the *semantics* of the table — which transitions are allowed and
which are refused — against a double, because the interesting cases are state
transitions and a double makes all of them reachable. The SQL itself is exercised
against a real database by the integration test at the bottom, which is skipped
unless one is configured: a double cannot tell us the SQL parses, and unit tests
cannot make a late-published hour happen on demand. Neither substitutes.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from typing import Any

import pytest

from reporadar.ingest.ledger import (
    CREATE_TABLE,
    TERMINAL,
    HourRecord,
    HourStatus,
    create_schema,
    ingested_hours,
    pending_hours,
    record_hour,
    status_counts,
)

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
DAY = date(2026, 7, 22)


class FakeConnection:
    """An asyncpg-shaped double that records calls and replays canned rows."""

    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []
        self._rows = rows or []

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))

    async def fetch(self, query: str, *args: Any) -> list[list[Any]]:
        self.fetched.append((query, args))
        return self._rows


async def test_the_schema_is_created_idempotently() -> None:
    connection = FakeConnection()

    await create_schema(connection)

    assert "CREATE TABLE IF NOT EXISTS archive_hours" in connection.executed[0][0]


async def test_an_ingested_hour_records_its_counts() -> None:
    connection = FakeConnection()

    await record_hour(
        connection,
        HourRecord(DAY, 22, HourStatus.INGESTED, events=157856, bytes=15_850_000),
        now=NOW,
    )

    query, args = connection.executed[0]
    assert "ON CONFLICT (day, hour) DO UPDATE" in query
    assert args == (DAY, 22, "ingested", 157856, 15_850_000, None, NOW)


async def test_the_status_is_written_as_its_plain_string() -> None:
    # A dashboard reads this column directly, so the stored value must be the
    # readable word and not an enum's repr.
    connection = FakeConnection()

    await record_hour(connection, HourRecord(DAY, 3, HourStatus.MISSING, detail="404"), now=NOW)

    assert connection.executed[0][1][2] == "missing"


async def test_a_settled_success_is_never_downgraded() -> None:
    # The guard that makes an upsert safe here: a transient fetch failure must not
    # erase a real ingest, so the update is conditional in SQL rather than trusted
    # to caller discipline.
    connection = FakeConnection()

    await record_hour(connection, HourRecord(DAY, 22, HourStatus.FAILED, detail="boom"), now=NOW)

    query = connection.executed[0][0]
    assert "WHERE archive_hours.status <> 'ingested' OR EXCLUDED.status = 'ingested'" in query


def test_an_ingested_hour_without_a_count_is_refused() -> None:
    # A success with a null count is a row that looks measured and is not — the
    # capture KPI would read it as coverage it never observed.
    with pytest.raises(ValueError, match="must carry its event count"):
        HourRecord(DAY, 22, HourStatus.INGESTED)


def test_a_failure_needs_no_count() -> None:
    assert HourRecord(DAY, 22, HourStatus.MISSING).events is None


@pytest.mark.parametrize("hour", [-1, 24, 99])
def test_an_impossible_hour_is_refused(hour: int) -> None:
    with pytest.raises(ValueError, match="hour must be 0-23"):
        HourRecord(DAY, hour, HourStatus.MISSING)


async def test_the_loop_leaves_failed_hours_alone() -> None:
    # Left alone so the always-on loop cannot spin forever on an hour that will
    # never parse: `failed` counts as settled for this caller.
    connection = FakeConnection(rows=[])

    await pending_hours(connection, DAY, DAY)

    settled = connection.fetched[0][1][2]
    assert set(settled) == {"ingested", "missing", "failed"}


async def test_a_backfill_picks_failed_hours_back_up() -> None:
    # The other half of the same distinction: an explicit range retries failures,
    # which is how a fix reaches the hours it fixes.
    connection = FakeConnection(rows=[])

    await pending_hours(connection, DAY, DAY, retry_failed=True)

    settled = connection.fetched[0][1][2]
    assert set(settled) == {"ingested", "missing"}
    assert "failed" not in settled


async def test_pending_hours_returns_day_hour_pairs() -> None:
    connection = FakeConnection(rows=[[DAY, 0], [DAY, 1]])

    assert await pending_hours(connection, DAY, DAY) == [(DAY, 0), (DAY, 1)]


async def test_pending_hours_passes_the_range_through_unchanged() -> None:
    connection = FakeConnection(rows=[])
    last = date(2026, 7, 24)

    await pending_hours(connection, DAY, last)

    assert connection.fetched[0][1][:2] == (DAY, last)


async def test_status_counts_reports_hours_and_events_per_status() -> None:
    connection = FakeConnection(rows=[["ingested", 3, 480_000], ["missing", 1, 0]])

    assert await status_counts(connection) == {
        HourStatus.INGESTED: (3, 480_000),
        HourStatus.MISSING: (1, 0),
    }


def test_only_the_two_upstream_outcomes_are_terminal_for_the_loop() -> None:
    # `failed` is deliberately absent: it is settled for the loop's purposes but it
    # is not an outcome anyone should treat as final.
    assert TERMINAL == frozenset({HourStatus.INGESTED, HourStatus.MISSING})


def test_the_table_refuses_a_counted_success_with_no_count_in_sql_too() -> None:
    # The Python guard above is convenience; this is the one that holds when a
    # future writer bypasses HourRecord entirely.
    assert "CHECK (status <> 'ingested' OR events IS NOT NULL)" in CREATE_TABLE
    assert "CHECK (hour BETWEEN 0 AND 23)" in CREATE_TABLE


# --- against a real database -------------------------------------------------
#
# Opt-in, because CI has no server. A double proves the module calls what it means
# to; only a real server proves the SQL parses, the CHECK constraints bite, and the
# conditional upsert behaves as written.
#
# This test DROPS the ledger table, so it reads its own variable rather than the
# one the application runs on. The precondition is not "a database is configured"
# but "a database I am allowed to destroy", and only the second one can be stated
# by a name. Gating on REPORADAR_POSTGRES_DSN made every developer with a working
# .env one `set -a; source .env` away from losing the ledger — and losing it is
# expensive, because the lake files survive while the only record that they are
# done does not.
#
# Deliberately absent from .env.example: documenting it there is what would put it
# in .env, and anything in .env is one `set -a` from being exported, which is the
# exact hazard this replaces.

DSN = os.environ.get("REPORADAR_TEST_POSTGRES_DSN")


@pytest.mark.skipif(
    not DSN, reason="needs REPORADAR_TEST_POSTGRES_DSN — a database this test may drop tables in"
)
async def test_the_ledger_round_trips_against_a_real_database() -> None:
    import asyncpg

    connection = await asyncpg.connect(DSN)
    try:
        await connection.execute("DROP TABLE IF EXISTS archive_hours")
        await create_schema(connection)

        # A late-published hour: recorded missing, then it arrives.
        await record_hour(connection, HourRecord(DAY, 5, HourStatus.MISSING, detail="404"), now=NOW)
        assert (DAY, 5) not in await pending_hours(connection, DAY, DAY)  # settled: stop asking

        await record_hour(
            connection, HourRecord(DAY, 5, HourStatus.INGESTED, events=100, bytes=10), now=NOW
        )
        row = await connection.fetchrow(
            "SELECT status, events FROM archive_hours WHERE day = $1 AND hour = 5", DAY
        )
        assert (row["status"], row["events"]) == ("ingested", 100)

        # A later failure must NOT erase that success.
        await record_hour(
            connection, HourRecord(DAY, 5, HourStatus.FAILED, detail="transient"), now=NOW
        )
        row = await connection.fetchrow(
            "SELECT status, events FROM archive_hours WHERE day = $1 AND hour = 5", DAY
        )
        assert (row["status"], row["events"]) == ("ingested", 100)

        # The gap scan sees the other 23 hours of the day.
        pending = await pending_hours(connection, DAY, DAY)
        assert len(pending) == 23
        assert (DAY, 5) not in pending

        # Against a real server this is the one that matters: Postgres widens
        # sum(bigint) to numeric, which arrives as Decimal, so an uncast query
        # returns a type the double never produces. The counts must be plain ints.
        counts = await status_counts(connection)
        assert counts == {HourStatus.INGESTED: (1, 100)}
        assert all(type(hours) is int and type(events) is int for hours, events in counts.values())

        # The read the verifier uses. Its unit tests replay canned rows, so this is
        # the only place the query is known to parse and to return the columns in
        # the order the mapping assumes — and the only place the driver's own types
        # are involved, which is where this project has been bitten before.
        claimed = await ingested_hours(connection)
        assert [(row.day, row.hour, row.events, row.bytes) for row in claimed] == [
            (DAY, 5, 100, 10)
        ]
        assert claimed[0].status is HourStatus.INGESTED
        assert type(claimed[0].events) is int and type(claimed[0].bytes) is int

        # Only settled successes: a missing hour is an expected outcome, and a
        # verifier told about it would report a lake file that was never meant to
        # exist. The filter is in SQL, so only a server can prove it applies.
        await record_hour(connection, HourRecord(DAY, 6, HourStatus.MISSING, detail="404"), now=NOW)
        await record_hour(connection, HourRecord(DAY, 7, HourStatus.FAILED, detail="bad"), now=NOW)
        assert [(row.day, row.hour) for row in await ingested_hours(connection)] == [(DAY, 5)]

        # A null size must survive as None rather than arriving as 0, or every row
        # written before that column meant anything fails its size comparison.
        await record_hour(connection, HourRecord(DAY, 8, HourStatus.INGESTED, events=7), now=NOW)
        sizes = {row.hour: row.bytes for row in await ingested_hours(connection)}
        assert sizes == {5: 10, 8: None}

        # The CHECK constraints are real, not advisory.
        with pytest.raises(asyncpg.PostgresError):
            await connection.execute(
                "INSERT INTO archive_hours (day, hour, status, recorded_at) "
                "VALUES ($1, 99, 'missing', $2)",
                DAY,
                NOW,
            )
        with pytest.raises(asyncpg.PostgresError):
            await connection.execute(
                "INSERT INTO archive_hours (day, hour, status, recorded_at) "
                "VALUES ($1, 6, 'ingested', $2)",
                DAY,
                NOW,
            )
    finally:
        await connection.close()
