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


@dataclass
class PollCounters:
    """Monotonic counters accumulated over one poll run."""

    cycles: int = 0  # cycles attempted (successful + rate-limited)
    rate_limited: int = 0  # cycles skipped because the API was rate limiting
    fetched: int = 0  # events pulled from the API (after per-batch dedupe)
    fresh: int = 0  # events new across the run — i.e. actually written
    # Coverage is an estimate, so it is accumulated as a mean over the cycles that
    # could produce one rather than as a counter. Cycles that cannot (the first,
    # or one where the feed did not advance) are excluded rather than scored zero:
    # counting "no estimate" as "no coverage" would drag the mean toward a number
    # nothing measured.
    coverage_samples: int = 0
    _coverage_sum: float = 0.0

    @property
    def duplicates(self) -> int:
        """Events re-seen across cycles (fetched but already in the dedup window)."""
        return self.fetched - self.fresh

    @property
    def coverage_estimate(self) -> float | None:
        """Mean estimated share of the feed captured, or ``None`` if never measurable."""
        if self.coverage_samples == 0:
            return None
        return self._coverage_sum / self.coverage_samples

    def record_cycle(self, *, fetched: int, fresh: int, coverage: float | None = None) -> None:
        """Account for one successful cycle."""
        self.cycles += 1
        self.fetched += fetched
        self.fresh += fresh
        if coverage is not None:
            self.coverage_samples += 1
            self._coverage_sum += coverage

    def record_rate_limited(self) -> None:
        """Account for one cycle lost to rate limiting (no events fetched)."""
        self.cycles += 1
        self.rate_limited += 1

    def as_dict(self) -> dict[str, float | int | None]:
        """Snapshot including the derived values — the machine-readable seam.

        ``coverage_estimate`` is ``None`` until a cycle can support one, and it
        is published as null rather than as 0.0: a run that could not measure
        coverage and a run that captured nothing are different outcomes, and only
        one of them is a measurement.
        """
        snapshot: dict[str, float | int | None] = asdict(self)
        snapshot.pop("_coverage_sum", None)  # accumulator, not a reported figure
        snapshot["duplicates"] = self.duplicates
        snapshot["coverage_estimate"] = self.coverage_estimate
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
