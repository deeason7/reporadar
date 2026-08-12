"""What's in an archive hour, and how much of it a live sample overlaps.

DuckDB reads the gzipped NDJSON archives directly, so both are single SQL
queries — no ETL required.

The overlap is a join on event id, and it is **not** an answer to "how much of
the feed did we capture". Measured against real files the two sources share no
event identifier, so the join is empty by construction; ``capture_rate``
returns ``None`` for that rather than the confident zero the same arithmetic
once produced.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


def _q(path: Path) -> str:
    """Embed a local file path as a SQL string literal (single quotes doubled)."""
    return "'" + str(path).replace("'", "''") + "'"


def type_counts(archive_path: Path) -> list[tuple[str, int]]:
    """Event-type histogram for one archive hour, largest first."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT type, count(*) AS n
            FROM read_json_auto({_q(archive_path)}, format='newline_delimited')
            GROUP BY type
            ORDER BY n DESC
            """
        ).fetchall()
    finally:
        con.close()
    return [(str(event_type), int(n)) for event_type, n in rows]


@dataclass(frozen=True)
class CaptureReport:
    archive_events: int
    live_events: int
    matched: int

    @property
    def capture_rate(self) -> float | None:
        """Share of the archived hour's event ids present in the live sample.

        ``None`` when the sample holds events but none of them appear in this
        archive hour. That is not a capture rate of zero: it means the two files
        cannot be reconciled at all — an unshared identifier, or a sample drawn
        from a different hour — and the two facts are not interchangeable. A
        rate of 0.0 says the poller missed everything and would send the next
        investigation after the poller; measured against real data the same
        arithmetic reported a confident 0% while the join was empty by
        construction. Returning ``None`` makes that case unrepresentable as a
        number, and mypy makes every caller say what it does about it.
        """
        if self.archive_events == 0:
            return 0.0
        if self.matched == 0 and self.live_events > 0:
            return None
        return self.matched / self.archive_events


def capture_rate(archive_path: Path, live_path: Path) -> CaptureReport:
    """Compare a complete archive hour against a live poll sample by event id.

    Only meaningful when the live sample's window overlaps the archive hour —
    the caller owns that alignment.
    """
    con = duckdb.connect()
    try:
        row = con.execute(
            f"""
            WITH archive AS (
                SELECT DISTINCT id
                FROM read_json_auto({_q(archive_path)}, format='newline_delimited')
            ),
            live AS (
                SELECT DISTINCT id
                FROM read_json_auto({_q(live_path)}, format='newline_delimited')
            )
            SELECT
                (SELECT count(*) FROM archive)                      AS archive_n,
                (SELECT count(*) FROM live)                         AS live_n,
                (SELECT count(*) FROM archive JOIN live USING (id)) AS matched_n
            """
        ).fetchone()
    finally:
        con.close()
    if row is None:  # pragma: no cover - duckdb always returns one row here
        raise RuntimeError("capture-rate query returned no row")
    archive_n, live_n, matched_n = row
    return CaptureReport(
        archive_events=int(archive_n), live_events=int(live_n), matched=int(matched_n)
    )
