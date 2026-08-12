"""How much of the feed did the poller actually see?

The public /events feed is a rolling window capped by pagination: between two
polls it can advance further than one sweep can carry, and those events are
gone. Measuring that loss needs no second source — event ids run in near
sequence, so the distance the feed travelled between two polls says how many
events went by, and the poller knows how many of them it holds.

Two things make this delicate, and both are handled here rather than assumed
away:

*The feed carries more than one id sequence.* Ids arrive in widely separated
bands, and subtracting an id in one band from an id in another produces a
number with no meaning. Sequences are therefore detected from the data — a gap
that dwarfs the typical one is a boundary — and every figure is computed per
sequence and pooled only at the end.

*Spacing is measured, never configured.* Within a single sweep the events are
consecutive, so their mean id gap is an observation, taken fresh every cycle. A
constant here would be a number that quietly stops being true the day GitHub
changes how ids are handed out.

**What this rests on — two assumptions, and only one of them holds.** The first
is that a returned page is contiguous, that the feed does not omit events
*inside* a page; were that false the observed spacing would be wider than the
real one and coverage would read high. It was checked against complete archive
hours and it holds. The second was never written down here, which is most of why
it went unchecked for so long: that the spacing measured inside a page describes
the spacing outside it. It does not. Events arrive in dense bursts, and the id
distance between two bursts runs to thousands where the distance inside one is a
couple, so spacing taken from a page prices the empty space between bursts at the
density of a burst. The estimate of what went by then comes out too large and
coverage reads low — by a margin that varies with the hour, so no constant
recovers it. The correction belongs in how sequences are split, and until it is
made no figure derived from this is published.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from statistics import median

# This value is load-bearing, and it was documented here as the opposite.
#
# A boundary *between bands* is not a close call: the bands observed sit billions
# apart while neighbouring events sit single digits apart, nine orders of
# magnitude, and any threshold between them separates the two identically. That
# was the whole argument, and it left out the scale in the middle: events arrive
# in dense bursts, and consecutive bursts sit thousands of ids apart. So this
# factor selects which of the two a "sequence" means. Below the gap between
# bursts — where it currently sits — every burst becomes its own sequence and
# spacing is measured inside one. Above it, only a band boundary splits and
# spacing is measured across bursts, which is what the estimate actually needs.
# `test_the_gap_factor_decides_which_scale_counts_as_a_sequence` holds that
# visible; the retired test that claimed the value did not matter was green
# because its fixture had only the two extreme scales in it.
DEFAULT_GAP_FACTOR = 100.0

# Deciding whether last cycle's high-water id belongs to a sequence seen now is a
# *different* question from splitting one page into sequences, and using the gap
# rule for both conflates them: a poller that has fallen a thousand events behind
# leaves a gap far wider than the typical one, and would be read as a brand-new
# sequence — losing exactly the measurement this module exists to take. The
# sequences instead differ in *magnitude* (~12.2e9 against ~15.6e9, i.e. ~22%),
# while any plausible backlog stays a rounding error against the id itself: 1% of
# 15.6e9 is ~156 million ids, on the order of a day of feed. So membership is a
# relative test. Its exact value is not load-bearing — but that is asserted here
# on the strength of the scales it was checked against, not by analogy to the
# factor above, which carried the same claim and turned out not to deserve it.
# The scales that could bear on it are a backlog (~1e4 ids) and the gap between
# bursts (~2.8e3), both negligible against a tolerance of ~1.6e8 ids.
SAME_SEQUENCE_TOLERANCE = 0.01


def _group(values: Sequence[int], gap_factor: float) -> list[list[int]]:
    """Split ascending ``values`` wherever a gap dwarfs the typical one."""
    if len(values) < 3:  # too few gaps to say what "typical" means
        return [list(values)] if values else []
    gaps = [b - a for a, b in pairwise(values)]
    typical = median(gaps)
    if typical <= 0:  # degenerate spacing; treat the whole run as one sequence
        return [list(values)]
    limit = typical * gap_factor
    groups: list[list[int]] = [[values[0]]]
    for gap, value in zip(gaps, values[1:], strict=True):
        if gap > limit:
            groups.append([])
        groups[-1].append(value)
    return groups


@dataclass(frozen=True)
class SequenceSpan:
    """One id sequence as seen in a single poll cycle."""

    low: int
    high: int
    count: int

    @classmethod
    def of(cls, ids: Sequence[int]) -> SequenceSpan:
        return cls(low=ids[0], high=ids[-1], count=len(ids))

    @property
    def ids_per_event(self) -> float | None:
        """Mean id distance between neighbouring events, measured on this cycle.

        ``None`` below two events: one event establishes no spacing, and a
        fabricated default would be indistinguishable from a measurement.
        """
        if self.count < 2:
            return None
        return (self.high - self.low) / (self.count - 1)


def split_sequences(
    ids: Iterable[int], *, gap_factor: float = DEFAULT_GAP_FACTOR
) -> list[SequenceSpan]:
    """Partition ``ids`` into the separate sequences the feed is carrying."""
    ordered = sorted(set(ids))
    return [SequenceSpan.of(group) for group in _group(ordered, gap_factor) if group]


def numeric_ids(ids: Iterable[str]) -> list[int]:
    """Event ids that are plain integers; anything else is skipped, never fatal.

    The wire contract types an event id as a string, and this is the only code
    that reads arithmetic into it. Coverage is a derived, best-effort figure
    sitting beside the critical capture path, so an id that is not a number must
    cost the measurement and nothing else — a monitoring feature that can crash
    the ingester it monitors is worse than no measurement at all.
    """
    numbers: list[int] = []
    for raw in ids:
        try:
            numbers.append(int(raw))
        except (TypeError, ValueError):
            continue
    return numbers


@dataclass
class CoverageTracker:
    """Estimates, cycle by cycle, the share of the feed the poller is holding.

    Stateful by necessity: the measurement is the distance between one cycle and
    the next, so the previous cycle's high-water id per sequence is the whole
    state. A sequence seen for the first time yields no estimate rather than a
    placeholder one.
    """

    gap_factor: float = DEFAULT_GAP_FACTOR
    tolerance: float = SAME_SEQUENCE_TOLERANCE
    _highs: list[int] = field(default_factory=list)

    def _same_sequence(self, high: int, span: SequenceSpan) -> bool:
        """Is ``high`` a previous mark of the sequence ``span`` belongs to?"""
        scale = abs(span.low) * self.tolerance
        return abs(high - span.low) <= scale

    def record(self, ids: Iterable[int]) -> float | None:
        """Fold in one cycle's event ids; return its coverage estimate.

        ``None`` when this cycle cannot support one — the first cycle of a run,
        a sequence that did not advance (a cached response returns the same page,
        and inventing 100% coverage from it would be a lie), or too few events to
        measure spacing. Callers must treat "no estimate" and "poor coverage" as
        different facts; they are not interchangeable.
        """
        current = sorted(set(ids))
        if not current:
            return None

        captured_total = 0
        expected_total = 0.0
        next_highs: list[int] = []
        matched: set[int] = set()

        for cycle_ids in _group(current, self.gap_factor):
            span = SequenceSpan.of(cycle_ids)
            priors = [h for h in self._highs if self._same_sequence(h, span)]
            matched.update(priors)
            next_highs.append(max([*cycle_ids, *priors]))

            if not priors or len(cycle_ids) < 2:
                continue
            spacing = span.ids_per_event
            previous_high = max(priors)
            elapsed = span.high - previous_high
            if spacing is None or spacing <= 0 or elapsed <= 0:
                continue
            captured_total += sum(1 for i in cycle_ids if i > previous_high)
            expected_total += elapsed / spacing

        # A sequence absent from this page keeps its mark, or its next appearance
        # reads as brand new and silently forfeits the measurement.
        next_highs.extend(h for h in self._highs if h not in matched)
        self._highs = sorted(next_highs)
        if expected_total <= 0:
            return None
        # Deliberately unclamped. A ratio above 1 means the measured spacing has
        # drifted from the feed's real spacing, which is information about the
        # estimator; rounding it down to a tidy 100% would hide exactly the
        # signal that says to stop trusting the number.
        return captured_total / expected_total
