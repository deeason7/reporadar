"""Run counters for the ingestion poller — operational empathy (Rule 10).

A long-running poller has to be observable: how many events it pulled, how many
survived cross-cycle dedup, how many cycles it lost to rate limiting. These are
monotonic within a run (they only ever climb), snapshotted for a structured log
line now and a Prometheus scrape later. Silent failure is the one unforgivable
production sin, so the poller reports what it actually did.
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
        """Snapshot including the derived ``duplicates`` — the machine-readable seam."""
        snapshot = asdict(self)
        snapshot["duplicates"] = self.duplicates
        return snapshot
