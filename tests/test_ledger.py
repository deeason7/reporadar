"""The archive-hours ledger.

These tests pin the *semantics* of the table — which transitions are allowed and
which are refused — against a double, because the interesting cases are state
transitions and a double makes all of them reachable. The SQL itself is exercised
against a real database by the integration test at the bottom, which is skipped
unless one is configured: a double cannot tell us the SQL parses, and unit tests
cannot make a late-published hour happen on demand. Neither substitutes.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime
from typing import Any

import pytest

from reporadar.ingest.ledger import (
    CREATE_TABLE,
    FORGET_INGESTED_HOURS,
    TERMINAL,
    HourRecord,
    HourStatus,
    create_schema,
    forget_ingested_hours,
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


async def test_forgetting_no_hours_asks_the_database_nothing() -> None:
    # The array form would otherwise send a statement to the server to be told
    # that nothing matched — and this is the one query here that deletes, so it
    # should not be issued speculatively.
    connection = FakeConnection()

    assert await forget_ingested_hours(connection, []) == []
    assert connection.fetched == []


async def test_forgetting_hours_returns_what_the_database_says_it_removed() -> None:
    # Not what was asked for. A destructive statement reports back the rows it
    # actually took, and the caller depends on the difference: an hour it asked to
    # clear and did not get must never be re-fetched.
    connection = FakeConnection([[DAY, 5]])

    removed = await forget_ingested_hours(connection, [(DAY, 5), (DAY, 6)])

    assert removed == [(DAY, 5)]
    query, args = connection.fetched[0]
    assert "DELETE FROM archive_hours" in query
    assert (list(args[0]), list(args[1])) == ([DAY, DAY], [5, 6])


def test_the_delete_refuses_anything_that_is_not_a_claimed_success() -> None:
    # In the SQL, so the restriction holds however the caller was built. A
    # `missing` row is the loop's decision to stop asking and a `failed` row is a
    # triage note; erasing either would silently re-queue settled work.
    assert "WHERE status = 'ingested'" in FORGET_INGESTED_HOURS
    assert "RETURNING day, hour" in FORGET_INGESTED_HOURS


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

        # --- why a repair has to delete, rather than write over ---------------
        #
        # This is the reasoning the repair command rests on, and it is only
        # demonstrable against a server, because it lives in the upsert's WHERE
        # clause. Hour 5 is a claimed success. Suppose its file is gone and a
        # repair simply re-ingests it:
        #
        # *If the hour comes back*, the upsert fires and the row is re-derived —
        # so a delete looks unnecessary.
        await record_hour(
            connection, HourRecord(DAY, 5, HourStatus.INGESTED, events=222, bytes=22), now=NOW
        )
        row = await connection.fetchrow(
            "SELECT status, events FROM archive_hours WHERE day = $1 AND hour = 5", DAY
        )
        assert (row["status"], row["events"]) == ("ingested", 222)

        # *If it does not come back*, the guard refuses the write, and the false
        # claim survives untouched — repaired in appearance only. That guard is
        # correct: it is what stops a transient fetch failure erasing a real
        # ingest. Which is exactly why the repair clears the row first instead of
        # relying on the re-ingest to correct it.
        await record_hour(
            connection, HourRecord(DAY, 5, HourStatus.MISSING, detail="gone"), now=NOW
        )
        row = await connection.fetchrow(
            "SELECT status, events FROM archive_hours WHERE day = $1 AND hour = 5", DAY
        )
        assert (row["status"], row["events"]) == ("ingested", 222)

        # After the delete, any outcome can be recorded truthfully.
        removed = await forget_ingested_hours(connection, [(DAY, 5), (DAY, 6), (DAY, 7)])
        # Only the ingested hour: the guard is in the SQL, so the missing and
        # failed rows are refused however the caller asks.
        assert removed == [(DAY, 5)]
        assert [(r.day, r.hour) for r in await ingested_hours(connection)] == [(DAY, 8)]
        await record_hour(
            connection, HourRecord(DAY, 5, HourStatus.MISSING, detail="gone"), now=NOW
        )
        row = await connection.fetchrow(
            "SELECT status, events FROM archive_hours WHERE day = $1 AND hour = 5", DAY
        )
        assert (row["status"], row["events"]) == ("missing", None)
    finally:
        await connection.close()


# --------------------------------------------------------------------------- #
# The column type guards
# --------------------------------------------------------------------------- #


def test_a_bool_is_not_accepted_as_an_integer_column() -> None:
    """``isinstance(True, int)`` is True in Python, so the guard needs the extra arm.

    Without it a boolean column reaching an integer field passes narrowing and
    becomes 1 or 0 downstream — an event count of ``True`` that reads as one
    event. That is a schema drift reported as data, which is the failure mode
    these helpers exist to turn into a sentence.
    """
    from reporadar.ingest.ledger import _as_int

    assert _as_int(7) == 7
    with pytest.raises(TypeError, match="got bool"):
        _as_int(True)
    with pytest.raises(TypeError, match="got str"):
        _as_int("7")


def test_a_text_column_guard_names_the_type_it_got() -> None:
    from reporadar.ingest.ledger import _as_str

    assert _as_str("ok") == "ok"
    with pytest.raises(TypeError, match="got int"):
        _as_str(1)


async def test_the_removal_warning_names_the_hours_it_removed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # This line is the only record that a destructive statement ran, and the
    # fallback is a display fallback: "none" belongs to the case where the delete
    # matched nothing, never to the case where it matched something. Inverted, the
    # warning says "none" precisely when rows WERE destroyed.
    connection = FakeConnection([[DAY, 5], [DAY, 6]])

    with caplog.at_level(logging.WARNING):
        await forget_ingested_hours(connection, [(DAY, 5), (DAY, 6)])

    assert f"{DAY} 05" in caplog.text
    assert f"{DAY} 06" in caplog.text
    assert "none" not in caplog.text


async def test_the_removal_warning_says_none_when_the_delete_matched_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The other half: hours were asked for, the database removed no rows. An empty
    # list rendered into the sentence would read as a truncated message.
    connection = FakeConnection([])

    with caplog.at_level(logging.WARNING):
        await forget_ingested_hours(connection, [(DAY, 5)])

    assert "none" in caplog.text
