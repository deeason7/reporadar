"""Bounded "seen recently" set for streaming deduplication.

A long-running poller re-sees the same event id across overlapping page sweeps
and consecutive cycles. Deduping with an unbounded ``set`` is correct but
unbounded — run the process for hours and the set is a slow memory leak.
``RecentIds`` keeps only the most recent ``maxlen`` ids, evicting oldest-first,
so memory is capped by construction.

The cost is explicit and quantifiable: an id can reappear as "new" once more
than ``maxlen`` distinct ids have arrived since it was last seen. Size the
window to the real duplication horizon (a few page-sweeps' worth of events),
not to "forever" — the archive, not the live dedupe, is the completeness
arbiter, so a rare re-emission is a measured cost, never a correctness claim.
"""

from __future__ import annotations

from collections import OrderedDict


class RecentIds:
    """A fixed-capacity, insertion-ordered set of ids with FIFO eviction.

    Backed by an ``OrderedDict`` so membership and oldest-eviction are both
    O(1). Eviction is by *arrival* order, not access order (FIFO, not LRU): a
    duplicate is reported and dropped without refreshing the original's
    position, so re-seeing an id never extends the lifetime of the id it
    duplicates. That is the property that makes the window a true sliding
    window over the stream.
    """

    def __init__(self, maxlen: int) -> None:
        if maxlen < 1:
            raise ValueError(f"maxlen must be >= 1, got {maxlen}")
        self._maxlen = maxlen
        self._ids: OrderedDict[str, None] = OrderedDict()

    def add(self, event_id: str) -> bool:
        """Record ``event_id``; return True iff it was not already in the window.

        A duplicate returns False and leaves the window untouched (no refresh).
        """
        if event_id in self._ids:
            return False
        self._ids[event_id] = None
        if len(self._ids) > self._maxlen:
            self._ids.popitem(last=False)  # drop the oldest arrival
        return True

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    @property
    def maxlen(self) -> int:
        return self._maxlen
