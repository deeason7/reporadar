from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest import store
from reporadar.ingest.store import PostgresStore, pg_store, row_of
from reporadar.ingest.wire import SCHEMA_VERSION, WireEnvelope

CAPTURED_AT = datetime(2026, 7, 16, 15, 30, tzinfo=UTC)


class FakeConnection:
    """asyncpg-shaped double: records DDL and batch inserts, and remembers whether
    a transaction was open when the rows were written."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.batches: list[tuple[str, list[Sequence[object]]]] = []
        self.transactions = 0
        self.in_transaction = False
        self.wrote_inside_transaction = False

    async def execute(self, query: str, *args: object) -> object:
        self.executed.append(query)
        return None

    async def executemany(self, command: str, args: Iterable[Sequence[object]]) -> object:
        self.batches.append((command, list(args)))
        self.wrote_inside_transaction = self.in_transaction
        return None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        self.transactions += 1
        self.in_transaction = True
        try:
            yield None
        finally:
            self.in_transaction = False


class FakePool:
    """asyncpg-shaped pool double: hands out one connection, records its own close."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.acquisitions = 0
        self.closed = False

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeConnection]:
        self.acquisitions += 1
        yield self.connection

    async def close(self) -> None:
        self.closed = True


def _envelope(event_dict: dict[str, Any], id_: str) -> WireEnvelope:
    return WireEnvelope(
        v=SCHEMA_VERSION,
        captured_at=CAPTURED_AT,
        event=RawEvent.model_validate({**event_dict, "id": id_}),
    )


def test_row_carries_both_clocks_and_the_flattened_event(event_dict: dict[str, Any]) -> None:
    # The row is where the envelope becomes columns: actor and repo flatten to the
    # ids and names queries filter on, the two clocks stay separate (so capture lag
    # is subtractable per row), and the per-type payload is serialized exactly once.
    envelope = _envelope(event_dict, "45000000001")

    assert row_of(envelope) == (
        "45000000001",
        "PushEvent",
        1,
        "octo-tester",
        2,
        "octo/widgets",
        envelope.event.created_at,  # event time
        CAPTURED_AT,  # ingest time — a different clock, not a copy
        json.dumps({"push_id": 99, "size": 1}),
    )


async def test_a_batch_is_one_idempotent_insert_inside_one_transaction(
    event_dict: dict[str, Any],
) -> None:
    connection = FakeConnection()
    pool = FakePool(connection)

    await PostgresStore(pool)([_envelope(event_dict, "a"), _envelope(event_dict, "b")])

    [(command, rows)] = connection.batches  # the whole batch, one round trip
    assert "ON CONFLICT (event_id, created_at) DO NOTHING" in command
    assert [row[0] for row in rows] == ["a", "b"]
    assert connection.transactions == 1
    # Store-then-commit: the rows are written inside the transaction, and it has
    # committed by the time the call returns — which is what lets a source commit
    # its offsets only after the batch is durable.
    assert connection.wrote_inside_transaction
    assert not connection.in_transaction


async def test_a_failed_write_raises_instead_of_returning_quietly(
    event_dict: dict[str, Any],
) -> None:
    # Silence here would be the worst outcome: the source would commit offsets for
    # a batch that never landed, and the events would be gone for good.
    class FailingConnection(FakeConnection):
        async def executemany(self, command: str, args: Iterable[Sequence[object]]) -> object:
            raise ConnectionError("database went away")

    with pytest.raises(ConnectionError):
        await PostgresStore(FakePool(FailingConnection()))([_envelope(event_dict, "a")])


async def test_factory_ensures_the_schema_then_serves_a_working_store(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    connection = FakeConnection()
    pool = FakePool(connection)
    dsns: list[str] = []

    async def fake_create_pool(dsn: str) -> FakePool:
        dsns.append(dsn)
        return pool

    monkeypatch.setattr(store, "create_pool", fake_create_pool)

    async with pg_store(settings) as validated_store:
        assert dsns == [str(settings.postgres_dsn)]  # the configured DSN, threaded through
        assert any("CREATE TABLE IF NOT EXISTS events" in query for query in connection.executed)
        assert any("create_hypertable" in query for query in connection.executed)
        assert not pool.closed
        await validated_store([_envelope(event_dict, "a")])

    assert pool.closed  # exit returns the connections
    assert len(connection.batches) == 1


async def test_factory_refuses_to_run_without_a_configured_database(settings: Settings) -> None:
    # Polling needs no database, so the DSN is optional in settings — which makes
    # this the one place a missing one has to fail, loudly and actionably.
    unconfigured = settings.model_copy(update={"postgres_dsn": None})

    with pytest.raises(RuntimeError, match="REPORADAR_POSTGRES_DSN"):
        async with pg_store(unconfigured):
            raise AssertionError("a store must never be served without a database")


async def test_factory_closes_the_pool_when_the_run_crashes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    pool = FakePool(FakeConnection())

    async def fake_create_pool(dsn: str) -> FakePool:
        return pool

    monkeypatch.setattr(store, "create_pool", fake_create_pool)

    with pytest.raises(RuntimeError):
        async with pg_store(settings):
            raise RuntimeError("consumer crashed mid-run")

    assert pool.closed  # no connection leaks behind a crash
