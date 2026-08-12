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
couple, so spacing taken from *inside a burst* prices the empty space between
bursts at the density of a burst. The estimate of what went by then comes out too
large and coverage reads low — by a margin that varies with the hour, so no
constant recovers it, and the correction had to go into how sequences are split
rather than into a factor applied afterwards. It has: a burst is not a sequence,
only the boundary between id bands is, and spacing is measured across bursts. What
is left is a per-cycle wobble that depends on where a page happens to fall against
the bursts, which pooling absorbs and a single cycle does not — so a single
cycle's estimate is not a measurement. No ratio derived from this is published
yet; which stretch of time a given ratio describes is a separate open question.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from statistics import median

# This value is load-bearing, and it has to land between two measured scales.
#
# `_group` splits wherever a gap exceeds the typical one by this factor, and the
# typical gap on a real page is the one *inside* a burst. Three scales matter, not
# two: ~2 ids between neighbouring events within a burst, ~2,790 between
# consecutive bursts, and ~3.4e9 between the id bands the feed carries. This
# factor decides which of the last two counts as a boundary, and only one answer
# is usable: a burst is not a sequence. Split at the burst boundary and spacing is
# measured inside a burst, which prices the empty space between bursts at burst
# density; split only at the band boundary and spacing is measured across them,
# which is the distance the estimate actually divides.
#
# So the value must clear the inter-burst gap and stay under the band gap:
#
#     floor     2,790 / 2  ~=   1,400   at or below this, bursts stop merging
#     ceiling    3.4e9 / 2  ~=  1.7e9   at or above this, two bands merge
#
# A million sits ~700x above the floor and ~1,700x below the ceiling. **The
# ceiling is the side to watch**: it is set by how far apart the two bands happen
# to sit, so it moves with the data, while the floor is a property of how the feed
# emits events. Raising this "to be safe" walks toward the ceiling, and merging
# two bands means subtracting an id in one from an id in the other, which is a
# number with no meaning. `test_the_gap_factor_must_clear_both_scales` holds both
# sides visible so neither can be crossed quietly.
DEFAULT_GAP_FACTOR = 1.0e6

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

        # The split scale decides two things here, not one. It sets the spacing
        # each span reports, and it sets how many spans lay claim to the same
        # stretch of id space — because sequence membership is a test of id
        # magnitude, so every burst in a band matches the same previous mark.
        # Splitting per burst therefore had each burst measure `elapsed` from that
        # one origin, and those spans overlap, so the expected total counted the
        # same ids repeatedly on top of pricing them at burst density. One
        # sequence per band leaves one span and one elapsed.
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
