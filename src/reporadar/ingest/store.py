"""Validated store — consumed events into TimescaleDB — and the database's front door.

``consume_stream`` hands each batch of validated, deduped envelopes to a
``ValidatedStore``; this is the durable one. TimescaleDB is Postgres on the
wire, so a Postgres driver is the only client it needs.

This module also owns *how this project opens its database*: ``pg_store`` for a
caller that writes batches, ``pg_connection`` for one that holds a connection
across a run. Both live here so the DSN check and its message exist once — two
entry points with two different phrasings for the same missing setting is a
support question waiting to happen.

Idempotency is the load-bearing part. Delivery is at-least-once and the
consumer's dedup window only catches redeliveries that arrive inside it — a
redelivery after a long gap, or after a restart that re-reads uncommitted
offsets, reaches this store. ``ON CONFLICT DO NOTHING`` makes that write a
no-op in the database itself, which is the only place the guarantee can
actually hold, so at-least-once *delivery* becomes an effectively-once
*effect*. The conflict target is ``(event_id, created_at)`` rather than the
event id alone: a hypertable requires its partitioning column in every unique
index, and the event id is globally unique regardless, so the composite key is
exactly as strict.

Both clocks persist — the event's ``created_at`` (event time, and the
partitioning dimension) and the envelope's ``captured_at`` (ingest time) — so
capture lag is ``captured_at - created_at`` on any row. The per-type payload
stays JSONB, the same "untyped until a per-type model earns its keep" stance
the event envelope takes.

A batch is one transaction and the call returns only once it has committed,
which is what lets a source commit its offsets afterwards: store, then commit.
A failed write raises rather than returning quietly, so offsets stay put and
the batch is redelivered.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol

from asyncpg import connect, create_pool

from reporadar.config import Settings
from reporadar.ingest.ledger import Connection as LedgerConnection
from reporadar.ingest.wire import WireEnvelope

logger = logging.getLogger(__name__)

# Idempotent DDL: the first run creates, later runs no-op — so a fresh database
# needs no manual setup step. A migration tool earns its place when the schema
# starts *evolving*; first creation doesn't justify one.
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    event_id    text        NOT NULL,
    type        text        NOT NULL,
    actor_id    bigint      NOT NULL,
    actor_login text        NOT NULL,
    repo_id     bigint      NOT NULL,
    repo_name   text        NOT NULL,
    created_at  timestamptz NOT NULL,
    captured_at timestamptz NOT NULL,
    payload     jsonb       NOT NULL,
    PRIMARY KEY (event_id, created_at)
)
"""

CREATE_HYPERTABLE = (
    "SELECT create_hypertable('events', by_range('created_at'), if_not_exists => TRUE)"
)

# $9 is bound as text and cast, so the payload is serialized once in Python and
# the driver never has to guess how a dict becomes JSONB.
INSERT_EVENTS = """
INSERT INTO events (
    event_id, type, actor_id, actor_login, repo_id, repo_name,
    created_at, captured_at, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text::jsonb)
ON CONFLICT (event_id, created_at) DO NOTHING
"""


def row_of(envelope: WireEnvelope) -> tuple[object, ...]:
    """One envelope flattened into the row the insert binds.

    The envelope's ingest clock and the event's own clock stay separate columns,
    the actor and repo are flattened to the ids and names every query filters on,
    and the payload is serialized here — so the write itself is a pure push of
    ready values.
    """
    event = envelope.event
    return (
        event.id,
        event.type,
        event.actor.id,
        event.actor.login,
        event.repo.id,
        event.repo.name,
        event.created_at,
        envelope.captured_at,
        json.dumps(event.payload),
    )


class Connection(Protocol):
    """The slice of an async Postgres connection the store needs."""

    async def execute(self, query: str, *args: object) -> object: ...

    async def executemany(self, command: str, args: Iterable[Sequence[object]]) -> object: ...

    def transaction(self) -> AbstractAsyncContextManager[object]: ...


class ConnectionPool(Protocol):
    """The slice of an async Postgres pool the store needs."""

    def acquire(self) -> AbstractAsyncContextManager[Connection]: ...


class PostgresStore:
    """A ``ValidatedStore`` writing each batch as one idempotent transaction."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    async def __call__(self, envelopes: Sequence[WireEnvelope]) -> None:
        rows = [row_of(envelope) for envelope in envelopes]
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.executemany(INSERT_EVENTS, rows)


async def create_schema(connection: Connection) -> None:
    """Ensure the events hypertable exists (idempotent; safe on every startup)."""
    await connection.execute(CREATE_TABLE)
    await connection.execute(CREATE_HYPERTABLE)
    logger.info("events hypertable ready")


def _require_dsn(settings: Settings) -> str:
    """The configured DSN, or a failure naming the variable to set.

    Pure config, so it is checked before anything opens a socket: a misconfigured
    run should fail on its settings rather than on a connection attempt that
    reports a default host nobody chose.
    """
    if settings.postgres_dsn is None:
        raise RuntimeError("no database configured: set REPORADAR_POSTGRES_DSN (see .env.example)")
    return str(settings.postgres_dsn)


@asynccontextmanager
async def pg_connection(settings: Settings) -> AsyncIterator[LedgerConnection]:
    """One connection, opened for the life of a run and closed on the way out.

    A connection rather than a pool, and deliberately not ``pg_store``'s shape.
    ``pg_store`` yields a *store*: each call writes a batch and returns, so any
    free pooled connection will do. The archive ingest is the opposite — it holds
    one connection for a run that may last days and shares it across concurrent
    hours — so a pool here would be machinery wrapped around a single acquisition
    held forever, which is a pool with its one useful property disabled.

    **Nothing here reconnects, and that is a decision rather than an omission.** A
    dropped connection ends the run loudly; the supervisor restarts it; the next
    scan re-derives what is outstanding from the ledger. Because the ingest loop
    is level-triggered, a restart costs one interval and loses no work, so a
    reconnecting pool would re-solve — less well, and with more moving parts — a
    problem the convergence design has already solved.

    Typed as the *ledger's* connection protocol because that is what the caller
    needs: the narrowest surface, not the driver's whole API.
    """
    connection = await connect(_require_dsn(settings))
    try:
        yield connection
    finally:
        # Closed whether the run ended or crashed. A long-lived ingest service
        # that dies without this leaves a backend session pinned server-side,
        # and those accumulate across restarts until connections run out.
        await connection.close()


@asynccontextmanager
async def pg_store(settings: Settings) -> AsyncIterator[PostgresStore]:
    """A ready :class:`PostgresStore`: pool opened on entry, closed on exit.

    The schema is ensured on entry, so a fresh database is usable immediately.
    The pool closes whether the run ends or crashes — a dying consumer returns
    its connections instead of leaking them.
    """
    pool = await create_pool(_require_dsn(settings))
    try:
        async with pool.acquire() as connection:
            await create_schema(connection)
        yield PostgresStore(pool)
    finally:
        await pool.close()
