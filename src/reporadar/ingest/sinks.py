"""Concrete event sinks for the service poller.

`poll_stream` pushes fresh events to an ``EventSink``; this module provides the
file sink used for local capture. Events are bucketed into hourly NDJSON files
**by event time** (``created_at``), mirroring GH Archive's hourly layout — so a
captured live hour lines up directly with the archive hour it should be
reconciled against (capture rate). Writes are append-only, so a restart resumes
the current hour's file instead of truncating it.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC
from pathlib import Path

from reporadar.github.events import RawEvent


class HourlyNdjsonSink:
    """An ``EventSink`` that appends events to per-event-hour NDJSON files.

    A batch that straddles an hour boundary is split across two files. Sync file
    I/O inside the async call is fine at poll cadence (a small append every
    interval); a high-throughput sink (e.g. Kafka) would be genuinely async.
    """

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir
        base_dir.mkdir(parents=True, exist_ok=True)

    async def __call__(self, events: Sequence[RawEvent]) -> None:
        buckets: defaultdict[str, list[RawEvent]] = defaultdict(list)
        for event in events:
            hour = event.created_at.astimezone(UTC).strftime("%Y-%m-%d-%H")
            buckets[hour].append(event)
        for hour, group in buckets.items():
            with self.path_for(hour).open("a", encoding="utf-8") as fh:
                for event in group:
                    fh.write(event.model_dump_json() + "\n")

    def path_for(self, hour: str) -> Path:
        """Path of the file events for ``hour`` (``YYYY-MM-DD-HH``) land in."""
        return self._base_dir / f"events_{hour}.ndjson"
