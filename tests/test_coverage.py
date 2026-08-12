from __future__ import annotations

from reporadar.ingest.coverage import (
    DEFAULT_GAP_FACTOR,
    CoverageTracker,
    SequenceSpan,
    numeric_ids,
    split_sequences,
)

# Shapes taken from the real feed: neighbouring events sit a few ids apart, and
# the two sequences it carries sit billions apart. Pinned as literals so the
# tests describe the feed rather than re-deriving it from the code under test.
LOWER_BASE = 12_222_000_000
UPPER_BASE = 15_666_900_000
SPACING = 4  # ~3.4-4.2 ids per event measured on live pages

# The feed has a third scale, sitting between those two, and a fixture without it
# will agree with almost anything said about how sequences are split. Events
# arrive in dense bursts: neighbouring events ~2 ids apart inside a burst, but
# consecutive bursts ~2,790 apart. Measured over complete archive hours, where
# bursts of exactly 100 events are the commonest shape by some margin.
IN_BURST_SPACING = 2
BETWEEN_BURSTS = 2_790
BURST = 100
BURSTS = 6


def _run(base: int, n: int, *, spacing: int = SPACING, start: int = 0) -> list[int]:
    """A contiguous run of ``n`` events, as one uninterrupted sweep would see."""
    return [base + start + i * spacing for i in range(n)]


def _bursty(base: int, bursts: int = BURSTS) -> list[int]:
    """One id band carrying several dense bursts, the way a real page does.

    Three scales at once, which is the entire point of it: inside a burst,
    between bursts, and (once two of these are combined) between bands.
    """
    ids: list[int] = []
    cursor = base
    for _ in range(bursts):
        ids.extend(cursor + i * IN_BURST_SPACING for i in range(BURST))
        cursor = ids[-1] + BETWEEN_BURSTS
    return ids


def test_a_single_sequence_stays_whole() -> None:
    spans = split_sequences(_run(UPPER_BASE, 50))
    assert len(spans) == 1
    assert spans[0].count == 50


def test_two_sequences_are_separated() -> None:
    spans = split_sequences(_run(LOWER_BASE, 30) + _run(UPPER_BASE, 70))
    assert [s.count for s in spans] == [30, 70]
    assert spans[0].high < spans[1].low


def test_the_gap_factor_decides_which_scale_counts_as_a_sequence() -> None:
    # This replaces a test asserting the opposite -- that the factor was not
    # tuned and any threshold across a vast range partitioned the feed the same
    # way. It was green, and it was green because of what its fixture lacked:
    # two scales nine orders of magnitude apart agree with any threshold between
    # them, and the gap between bursts sits in that space. Which side of it the
    # factor falls on decides whether a sequence means one burst or one band, and
    # spacing -- the number this module exists to measure -- is then taken across
    # whichever was chosen. So the factor is load-bearing and must stay visible.
    ids = _bursty(LOWER_BASE) + _bursty(UPPER_BASE)
    shapes = {
        factor: len(split_sequences(ids, gap_factor=factor))
        for factor in (10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 10_000_000.0)
    }
    assert len(set(shapes.values())) > 1, shapes
    assert shapes[10.0] == 2 * BURSTS, "below the gap between bursts: a sequence is a burst"
    assert shapes[10_000_000.0] == 2, "above it: a sequence is a band"


def test_the_scale_selected_decides_the_spacing_that_gets_measured() -> None:
    # Why the test above matters rather than being a curiosity about a constant.
    # Spacing measured inside a burst is not the rate the feed advances at
    # between bursts, and the estimate divides an elapsed id distance spanning
    # both. No factor is asserted here on purpose: how far apart the two readings
    # sit depends on how many bursts this fixture holds, which is a property of
    # the fixture and not of the feed. The relation is the durable part.
    ids = _bursty(UPPER_BASE)
    inside = split_sequences(ids, gap_factor=10.0)
    across = split_sequences(ids, gap_factor=10_000_000.0)
    assert len(across) == 1
    across_spacing = across[0].ids_per_event
    assert across_spacing is not None
    assert all(s.ids_per_event == float(IN_BURST_SPACING) for s in inside)
    assert across_spacing > 10 * IN_BURST_SPACING


def test_the_shipped_default_measures_spacing_inside_a_burst() -> None:
    # Where the shipped default actually falls, pinned rather than left to be
    # inferred from the two tests above: below the gap between bursts. A page
    # carrying several bursts is therefore split into one sequence each, and the
    # spacing that reaches the estimate is the in-burst one. Moving the default
    # across that boundary changes what the reported ratio means, so it should
    # not be possible to move it quietly.
    spans = split_sequences(_bursty(UPPER_BASE), gap_factor=DEFAULT_GAP_FACTOR)
    assert len(spans) == BURSTS
    assert all(s.ids_per_event == float(IN_BURST_SPACING) for s in spans)


def test_ids_per_event_is_measured_not_assumed() -> None:
    span = SequenceSpan.of(_run(UPPER_BASE, 11, spacing=7))
    assert span.ids_per_event == 7.0


def test_one_event_establishes_no_spacing() -> None:
    # A fabricated default here would be indistinguishable from a measurement,
    # which is the failure this whole module exists downstream of.
    assert SequenceSpan.of([UPPER_BASE]).ids_per_event is None


def test_first_cycle_yields_no_estimate() -> None:
    # Nothing to measure against yet. "No estimate" must not arrive as 0.0.
    assert CoverageTracker().record(_run(UPPER_BASE, 100)) is None


def test_a_poller_that_keeps_up_reports_full_coverage() -> None:
    tracker = CoverageTracker()
    tracker.record(_run(UPPER_BASE, 100))
    # The next sweep continues exactly where the last ended: nothing went by
    # unseen, so the estimate should sit at 1.0.
    following = _run(UPPER_BASE, 100, start=100 * SPACING)
    assert tracker.record(following) == 1.0


def test_a_poller_that_falls_behind_reports_the_shortfall() -> None:
    # The real failure this KPI exists for: between two sweeps the feed advanced
    # ten pages' worth, and the poller holds one of them. Ten times the ids
    # elapsed for the same number of events captured => ~10% coverage.
    tracker = CoverageTracker()
    tracker.record(_run(UPPER_BASE, 100))
    behind = _run(UPPER_BASE, 100, start=1000 * SPACING)
    estimate = tracker.record(behind)
    assert estimate is not None
    assert 0.09 < estimate < 0.11


def test_a_repeated_page_yields_no_estimate_rather_than_perfect_coverage() -> None:
    # GitHub caches /events, so polling faster than the cache returns the very
    # same page. The feed did not advance, so nothing about coverage was
    # observed — and scoring that as 100% would turn our own poll cadence into
    # a fake perfect score, which is precisely the failure that this project's
    # capture KPI already made once.
    tracker = CoverageTracker()
    page = _run(UPPER_BASE, 100)
    tracker.record(page)
    assert tracker.record(page) is None


def test_sequences_are_measured_separately_and_never_subtracted_across() -> None:
    # Subtracting an id in one sequence from an id in another produces a number
    # with no meaning; pooling them would make coverage swing on which sequences
    # a page happened to include.
    tracker = CoverageTracker()
    tracker.record(_run(LOWER_BASE, 50) + _run(UPPER_BASE, 50))
    estimate = tracker.record(
        _run(LOWER_BASE, 50, start=50 * SPACING) + _run(UPPER_BASE, 50, start=50 * SPACING)
    )
    assert estimate == 1.0


def test_a_sequence_absent_this_cycle_keeps_its_place() -> None:
    # A page may carry only one of the two sequences. The quiet one must keep its
    # high-water mark, or its next appearance reads as a brand-new sequence and
    # silently forfeits the measurement.
    tracker = CoverageTracker()
    tracker.record(_run(LOWER_BASE, 50) + _run(UPPER_BASE, 50))
    tracker.record(_run(UPPER_BASE, 50, start=50 * SPACING))  # upper only
    resumed = tracker.record(_run(LOWER_BASE, 50, start=50 * SPACING))
    assert resumed == 1.0


def test_sequence_matching_is_insensitive_to_the_tolerance() -> None:
    # Same claim as the gap-factor test, for the other constant: a backlog of a
    # thousand events is ~3.6k ids against an id of ~1.6e10, while the two
    # sequences differ by ~22%. Six orders of magnitude sit between those, so the
    # exact tolerance cannot matter — unless someone makes it matter, and then
    # this goes red instead of the estimate quietly changing meaning.
    for tolerance in (0.0001, 0.001, 0.01, 0.05):
        tracker = CoverageTracker(tolerance=tolerance)
        tracker.record(_run(UPPER_BASE, 100))
        estimate = tracker.record(_run(UPPER_BASE, 100, start=1000 * SPACING))
        assert estimate is not None and 0.09 < estimate < 0.11, tolerance


def test_a_falling_behind_poller_is_not_mistaken_for_a_new_sequence() -> None:
    # The bug this module had first: splitting sequences by "a gap much bigger
    # than typical" also fires when the poller falls behind, because a backlog
    # leaves exactly such a gap. That reclassified the one situation the KPI
    # exists to measure as a brand-new sequence with no history, and returned
    # None instead of a shortfall. Sequence identity is a question about id
    # magnitude, not about gap size.
    tracker = CoverageTracker()
    tracker.record(_run(UPPER_BASE, 100))
    behind = _run(UPPER_BASE, 100, start=5000 * SPACING)
    estimate = tracker.record(behind)
    assert estimate is not None, "a backlog must yield a shortfall, not a fresh sequence"
    assert estimate < 0.05


def test_non_numeric_ids_cost_the_measurement_and_nothing_else() -> None:
    # Coverage rides beside the critical capture path. An id that will not parse
    # must never propagate an exception into the poll loop.
    assert numeric_ids(["12", "not-an-id", "34", "", "56"]) == [12, 34, 56]
    assert CoverageTracker().record(numeric_ids(["nope", "also-nope"])) is None
