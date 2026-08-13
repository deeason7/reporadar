"""Run counters for the ingestion pipeline — operational empathy.

A long-running ingester has to be observable: how many events it pulled, how
many survived dedup, how many cycles it lost to rate limiting, how many messages
it had to dead-letter. These counters are monotonic within a run (they only ever
climb), snapshotted for a structured log line now and a Prometheus scrape later
(``as_dict`` is that seam). ``PollCounters`` covers the live poller (produce
side); ``ConsumeCounters`` covers the stream consumer (read side). Silent failure
is the one unforgivable production sin, so each stage reports what it actually did.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from reporadar.ingest.ledger import HourStatus


@dataclass
class PollCounters:
    """Monotonic counters accumulated over one poll run."""

    cycles: int = 0  # cycles attempted (successful + rate-limited)
    rate_limited: int = 0  # cycles skipped because the API was rate limiting
    fetched: int = 0  # events pulled from the API (after per-batch dedupe)
    fresh: int = 0  # events new across the run — i.e. actually written

    @property
    def duplicates(self) -> int:
        """Events re-seen across cycles (fetched but already in the dedup window)."""
        return self.fetched - self.fresh

    def record_cycle(self, *, fetched: int, fresh: int) -> None:
        """Account for one successful cycle."""
        self.cycles += 1
        self.fetched += fetched
        self.fresh += fresh

    def record_rate_limited(self) -> None:
        """Account for one cycle lost to rate limiting (no events fetched)."""
        self.cycles += 1
        self.rate_limited += 1

    def as_dict(self) -> dict[str, int]:
        """Snapshot including the derived values — the machine-readable seam.

        Every figure here is an exact count of something the poller did. There is
        deliberately no estimate of what share of the feed those counts represent:
        the estimator that produced one was retired because its residual error had
        no explanation, and *how much did we pull* is a different and answerable
        question from *how much is there*.
        """
        snapshot: dict[str, int] = asdict(self)
        snapshot["duplicates"] = self.duplicates
        return snapshot


@dataclass
class ConsumeCounters:
    """Monotonic counters accumulated over one stream-consume run."""

    batches: int = 0  # source batches processed
    consumed: int = 0  # messages pulled from the source
    stored: int = 0  # valid, deduped events handed to the store
    dead_lettered: int = 0  # messages that failed to decode (routed to the DLQ)

    @property
    def duplicates(self) -> int:
        """Valid events dropped as already-seen within the run's dedup window.

        Every consumed message is stored, dead-lettered, or a duplicate, so this
        is what is left over — no separate counter can drift out of sync with it.
        """
        return self.consumed - self.stored - self.dead_lettered

    def record_batch(self, *, consumed: int, stored: int, dead_lettered: int) -> None:
        """Account for one processed source batch."""
        self.batches += 1
        self.consumed += consumed
        self.stored += stored
        self.dead_lettered += dead_lettered

    def as_dict(self) -> dict[str, int]:
        """Snapshot including the derived ``duplicates`` — the machine-readable seam."""
        snapshot = asdict(self)
        snapshot["duplicates"] = self.duplicates
        return snapshot


@dataclass
class ArchiveCounters:
    """Monotonic counters accumulated over one archive-ingest run.

    ``outstanding`` is the one that needs explaining: it counts hours that were
    attempted and deliberately left unrecorded — not published yet, or a failure
    that says nothing about the hour. They are neither successes nor errors, and
    folding them into either would hide the loop's most common healthy state.
    """

    passes: int = 0  # scans of the ledger for outstanding hours
    due: int = 0  # closed, unsettled hours the scans found
    ingested: int = 0  # hours converted and recorded
    missing: int = 0  # hours written off as never published
    failed: int = 0  # hours that arrived and could not be trusted
    outstanding: int = 0  # hours attempted and deliberately left for the next pass
    events: int = 0  # events across every ingested hour

    def record_pass(self, *, due: int) -> None:
        """Account for one scan, whatever it found (including nothing)."""
        self.passes += 1
        self.due += due

    def record_hour(self, *, status: HourStatus | None, events: int | None) -> None:
        """Account for one ingest attempt's outcome."""
        if status is None:
            self.outstanding += 1
            return
        if status is HourStatus.INGESTED:
            self.ingested += 1
            self.events += events or 0
        elif status is HourStatus.MISSING:
            self.missing += 1
        else:
            self.failed += 1

    def as_dict(self) -> dict[str, int]:
        """Snapshot — the machine-readable seam."""
        return asdict(self)
