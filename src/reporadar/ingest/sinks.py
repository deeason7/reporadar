"""Concrete event sinks for the service poller.

`poll_stream` pushes fresh events to an ``EventSink``; this module provides the
file sink used for local capture and the tee that sends one batch to two places
at once. Events are bucketed into hourly NDJSON files **by event time**
(``created_at``), mirroring GH Archive's hourly layout — so a captured live hour
lines up directly with the archive hour it should be reconciled against (capture
rate). Writes are append-only, so a restart resumes the current hour's file
instead of truncating it.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC
from pathlib import Path

from reporadar.github.events import RawEvent

logger = logging.getLogger(__name__)

EventSink = Callable[[Sequence[RawEvent]], Awaitable[None]]
"""An async consumer of a batch of fresh events. Defined here as well as in
``service`` so a sink module need not import the loop it feeds."""


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


class TeeSink:
    """An ``EventSink`` that writes each batch to a primary sink, then to others
    best-effort.

    The split is not cosmetic; it encodes which failure is allowed to stop the
    service. The **primary** sink is the reconciliation record — the hourly
    NDJSON files the capture rate is measured against — so a failure there is
    fatal and propagates: losing the completeness record silently is the one
    outcome this whole design exists to prevent. The **best-effort** sinks are
    the hot path (the Kafka stream that feeds the validated store); valuable, but
    not the arbiter, because the archive reconciliation is the ultimate
    completeness check. A failure there is logged loudly and counted, and the run
    continues — so a broker hiccup costs freshness for a while instead of turning
    the capture service into a crash-loop and taking the record down with it.

    "Best-effort" never means "silent" (Rule 10): every dropped batch is a
    warning, and ``dropped`` is surfaced at shutdown. A partially-published batch
    is fine — the consumer dedupes by event id and the archive reconciles the
    rest.
    """

    def __init__(self, primary: EventSink, *best_effort: EventSink) -> None:
        self._primary = primary
        self._best_effort = best_effort
        self.dropped = 0  # batches a best-effort sink failed to accept

    async def __call__(self, events: Sequence[RawEvent]) -> None:
        await self._primary(events)  # fatal on failure — the record comes first
        for sink in self._best_effort:
            try:
                await sink(events)
            except Exception as exc:  # noqa: BLE001 — best-effort by contract: loud, counted, not fatal
                self.dropped += 1
                logger.warning(
                    "stream sink dropped a batch of %d event(s) (%d dropped so far): %s",
                    len(events),
                    self.dropped,
                    exc,
                )
