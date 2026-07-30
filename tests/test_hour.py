"""Ingesting one archive hour, end to end.

Every interesting case here is a failure, and each one asks the same question:
*what gets written down?* An hour that might still arrive must leave no trace, an
hour that never will must leave one, and an hour that arrived broken must leave a
third — so the suite asserts on the ledger rows rather than on return values,
because the rows are what the next pass actually reads.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest
import respx

from reporadar.ingest.hour import (
    DEFAULT_PUBLICATION_GRACE,
    HourReport,
    hour_end,
    ingest_hour,
)
from reporadar.ingest.lake import PARQUET_FILENAME, partition_dir
from reporadar.ingest.ledger import HourStatus

DAY = date(2026, 7, 22)
HOUR = 22
URL = "https://data.gharchive.org/2026-07-22-22.json.gz"
CLOSED_AT = datetime(2026, 7, 22, 23, 0, tzinfo=UTC)
SOON = CLOSED_AT + timedelta(hours=1)
LONG_AFTER = CLOSED_AT + DEFAULT_PUBLICATION_GRACE + timedelta(hours=1)


class RecordingConnection:
    """Captures what the ledger was asked to write, and returns no rows."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))

    async def fetch(self, query: str, *args: Any) -> list[list[Any]]:
        return []


def _recorded(connection: RecordingConnection) -> list[dict[str, Any]]:
    """The ledger rows this run wrote, decoded by the writer's parameter order.

    Decoded in one place rather than asserted inline: the order belongs to the
    ledger and its own tests pin it, so a change there breaks one helper instead
    of a dozen assertions.
    """
    return [
        {"day": a[0], "hour": a[1], "status": a[2], "events": a[3], "bytes": a[4], "detail": a[5]}
        for _, a in connection.executed
    ]


def _event(event_id: str, created_at: str) -> dict[str, Any]:
    """One archive record, shaped like the published envelope."""
    return {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 2, "name": "octocat/hello"},
        "org": None,
        "payload": {"ref": "refs/heads/main", "size": 1},
        "public": True,
        "created_at": created_at,
    }


def _published(*events: dict[str, Any]) -> bytes:
    """Gzipped NDJSON, exactly as GH Archive serves it."""
    return gzip.compress(b"".join(json.dumps(event).encode() + b"\n" for event in events))


async def _ingest(
    tmp_path: Path, connection: RecordingConnection, *, now: datetime, **kwargs: Any
) -> HourReport:
    return await ingest_hour(
        DAY,
        HOUR,
        connection=connection,
        archive_dir=tmp_path / "raw",
        lake_dir=tmp_path / "lake",
        now=now,
        **kwargs,
    )


def test_an_hour_ends_when_the_next_one_starts() -> None:
    assert hour_end(DAY, 22) == datetime(2026, 7, 22, 23, 0, tzinfo=UTC)
    assert hour_end(DAY, 23) == datetime(2026, 7, 23, 0, 0, tzinfo=UTC)  # rolls the date


def test_hour_end_rejects_an_impossible_hour() -> None:
    with pytest.raises(ValueError, match="0-23"):
        hour_end(DAY, 24)


@respx.mock
async def test_a_published_hour_lands_in_the_lake_and_the_ledger(tmp_path: Path) -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(
            200,
            content=_published(
                _event("1", "2026-07-22T22:00:01Z"),
                _event("2", "2026-07-22T22:59:59Z"),
            ),
        )
    )
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=SOON)

    assert report.status is HourStatus.INGESTED
    assert report.recorded
    assert report.events == 2
    assert (partition_dir(tmp_path / "lake", DAY, HOUR) / PARQUET_FILENAME).exists()
    (row,) = _recorded(connection)
    assert row["status"] == "ingested"
    assert row["events"] == 2
    assert row["bytes"] == report.bytes
    assert row["day"] == DAY and row["hour"] == HOUR


@respx.mock
async def test_the_converted_source_is_kept_unless_the_caller_says_otherwise(
    tmp_path: Path,
) -> None:
    # The library default is the non-destructive one: a caller that forgets the
    # argument keeps its files. The commands pass the other value deliberately.
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T22:00:01Z")))
    )

    report = await _ingest(tmp_path, RecordingConnection(), now=SOON)

    assert report.status is HourStatus.INGESTED
    assert (tmp_path / "raw" / "2026-07-22-22.json.gz").exists()


@respx.mock
async def test_a_discarded_source_is_removed_once_its_hour_is_recorded(tmp_path: Path) -> None:
    # The columnar copy is what the ledger points at; the compressed source is a
    # cache of an immutable published file, and the downloader skips the network
    # whenever it is present. So after the row exists it buys nothing.
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T22:00:01Z")))
    )
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=SOON, keep_source=False)

    assert report.status is HourStatus.INGESTED
    assert not (tmp_path / "raw" / "2026-07-22-22.json.gz").exists()
    # What replaced it is the point: the lake file and the row that claims it.
    assert (partition_dir(tmp_path / "lake", DAY, HOUR) / PARQUET_FILENAME).exists()
    (row,) = _recorded(connection)
    assert row["status"] == "ingested"


@respx.mock
async def test_a_source_survives_a_hour_that_could_not_be_recorded(tmp_path: Path) -> None:
    # The ordering property, and the only one here that can lose data. If the row
    # cannot be written the hour is still outstanding, so the next pass needs either
    # this file or a re-download — deleting it before the ledger agrees would throw
    # away the only local copy of an hour nothing yet claims. Asserted rather than
    # commented, because the two orderings differ in no other observable way.
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T22:00:01Z")))
    )

    class RefusingConnection(RecordingConnection):
        async def execute(self, query: str, *args: Any) -> None:
            raise RuntimeError("the ledger is unreachable")

    with pytest.raises(RuntimeError, match="unreachable"):
        await _ingest(tmp_path, RefusingConnection(), now=SOON, keep_source=False)

    assert (tmp_path / "raw" / "2026-07-22-22.json.gz").exists()


@respx.mock
async def test_a_source_that_cannot_be_removed_does_not_fail_the_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A filesystem that refuses is a disk-space problem, not a data problem: the
    # hour is converted and recorded, and both stay true. Turning that into a failed
    # hour would write "failed" over a success and send the next pass back for an
    # hour that is already in the lake.
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T22:00:01Z")))
    )

    # Scoped to the source file by name. Patching Path.unlink outright also catches
    # the downloader's own `.part` cleanup, which would make this test fail inside
    # the fetch and prove nothing about the removal it is aiming at.
    real_unlink = Path.unlink

    def refuse(self: Path, missing_ok: bool = False) -> None:
        if self.name == "2026-07-22-22.json.gz":
            raise PermissionError("read-only file system")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", refuse)
    connection = RecordingConnection()

    with caplog.at_level("WARNING"):
        report = await _ingest(tmp_path, connection, now=SOON, keep_source=False)

    assert report.status is HourStatus.INGESTED  # the ingest still succeeded
    (row,) = _recorded(connection)
    assert row["status"] == "ingested"
    assert "could not remove" in caplog.text  # and it is loud about what it left behind
    assert "safe to delete by hand" in caplog.text


@respx.mock
async def test_an_hour_that_is_not_published_yet_is_recorded_nowhere(tmp_path: Path) -> None:
    """The whole convergence design rests on this: absence is the retry."""
    respx.get(URL).mock(return_value=httpx.Response(404))
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=SOON)

    assert report.status is None
    assert not report.recorded
    assert "not published yet" in report.detail
    assert _recorded(connection) == []


@respx.mock
async def test_an_hour_still_absent_after_the_grace_is_written_off(tmp_path: Path) -> None:
    respx.get(URL).mock(return_value=httpx.Response(404))
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=LONG_AFTER)

    assert report.status is HourStatus.MISSING
    (row,) = _recorded(connection)
    assert row["status"] == "missing"
    assert row["events"] is None  # nothing was counted, and that is not zero


@respx.mock
async def test_the_grace_boundary_decides_and_one_second_moves_it(tmp_path: Path) -> None:
    """The boundary is the whole behaviour, so it is pinned from both sides."""
    respx.get(URL).mock(return_value=httpx.Response(404))
    grace = timedelta(hours=6)

    just_inside = RecordingConnection()
    inside = await _ingest(
        tmp_path, just_inside, now=CLOSED_AT + grace - timedelta(seconds=1), grace=grace
    )
    just_outside = RecordingConnection()
    outside = await _ingest(tmp_path, just_outside, now=CLOSED_AT + grace, grace=grace)

    assert inside.status is None and _recorded(just_inside) == []
    assert outside.status is HourStatus.MISSING and len(_recorded(just_outside)) == 1


@respx.mock
async def test_a_server_error_says_nothing_about_the_hour(tmp_path: Path) -> None:
    """503 is a fact about the server, not about whether the hour exists."""
    respx.get(URL).mock(return_value=httpx.Response(503))
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=LONG_AFTER)

    assert report.status is None
    assert "503" in report.detail
    assert _recorded(connection) == []  # emphatically not 'missing', even past the grace


@respx.mock
async def test_a_transport_failure_leaves_the_hour_outstanding(tmp_path: Path) -> None:
    respx.get(URL).mock(side_effect=httpx.ConnectError("no route to host"))
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=LONG_AFTER)

    assert report.status is None
    assert "ConnectError" in report.detail
    assert _recorded(connection) == []


@respx.mock
async def test_an_hour_that_arrives_unreadable_is_recorded_as_failed(tmp_path: Path) -> None:
    """Fetched, and not to be trusted — a third outcome, and retrying will not fix it."""
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"this is not a gzip stream"))
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=SOON)

    assert report.status is HourStatus.FAILED
    (row,) = _recorded(connection)
    assert row["status"] == "failed"
    assert row["events"] is None
    assert not (partition_dir(tmp_path / "lake", DAY, HOUR) / PARQUET_FILENAME).exists()


@respx.mock
async def test_an_hour_holding_another_hours_events_is_recorded_as_failed(tmp_path: Path) -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T21:00:01Z")))
    )
    connection = RecordingConnection()

    report = await _ingest(tmp_path, connection, now=SOON)

    assert report.status is HourStatus.FAILED
    assert "PartitionMismatchError" in report.detail
    assert _recorded(connection)[0]["status"] == "failed"


@respx.mock
async def test_a_query_bug_is_not_recorded_as_a_bad_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A catalog error means the code is wrong, not the data.

    Recording it as a failed hour would settle every hour in a range as broken
    over one typo, and settle them in the status whose meaning is "retrying will
    not help" — which would then be true for the wrong reason.
    """

    def explode(*_: object, **__: object) -> None:
        raise duckdb.CatalogException("Scalar Function with name nope does not exist!")

    monkeypatch.setattr("reporadar.ingest.hour.write_hour", explode)
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=_published(_event("1", "2026-07-22T22:00:01Z")))
    )
    connection = RecordingConnection()

    with pytest.raises(duckdb.CatalogException):
        await _ingest(tmp_path, connection, now=SOON)

    assert _recorded(connection) == []


async def test_an_hour_that_has_not_closed_is_refused(tmp_path: Path) -> None:
    """Ingesting a live hour would file a partial hour as a whole one."""
    with pytest.raises(ValueError, match="has not closed"):
        await _ingest(tmp_path, RecordingConnection(), now=CLOSED_AT - timedelta(minutes=1))


async def test_a_naive_clock_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await _ingest(tmp_path, RecordingConnection(), now=datetime(2026, 7, 23, 0, 0))  # noqa: DTZ001


def test_the_grace_is_generous_because_missing_is_settled() -> None:
    """A settled status cannot be revisited, so the default must not be tight.

    Asserted rather than commented: the failure this guards against is somebody
    tuning it down to "publication takes five minutes" and turning every slow
    hour into a permanent hole.
    """
    assert DEFAULT_PUBLICATION_GRACE >= timedelta(hours=12)
