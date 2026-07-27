from __future__ import annotations

from reporadar.ingest.metrics import ConsumeCounters, PollCounters


def test_new_counters_start_at_zero() -> None:
    c = PollCounters()
    assert c.cycles == 0
    assert c.fetched == 0
    assert c.fresh == 0
    assert c.rate_limited == 0
    assert c.duplicates == 0


def test_record_cycle_accumulates_and_derives_duplicates() -> None:
    c = PollCounters()
    c.record_cycle(fetched=100, fresh=100)  # first cycle: all new
    c.record_cycle(fetched=100, fresh=20)  # second: 80 already seen

    assert c.cycles == 2
    assert c.fetched == 200
    assert c.fresh == 120
    assert c.duplicates == 80  # 200 fetched - 120 fresh
    assert c.rate_limited == 0


def test_rate_limited_cycle_counts_but_fetches_nothing() -> None:
    c = PollCounters()
    c.record_cycle(fetched=10, fresh=10)
    c.record_rate_limited()

    assert c.cycles == 2  # the skipped cycle still happened
    assert c.rate_limited == 1
    assert c.fetched == 10  # but pulled no events
    assert c.fresh == 10


def test_as_dict_snapshots_all_fields_including_duplicates() -> None:
    c = PollCounters()
    c.record_cycle(fetched=5, fresh=3)

    assert c.as_dict() == {
        "cycles": 1,
        "rate_limited": 0,
        "fetched": 5,
        "fresh": 3,
        "duplicates": 2,
        "coverage_samples": 0,
        "coverage_estimate": None,  # never measured — not the same as zero coverage
    }
    # The running accumulator must not reach the scrape seam: it is an
    # implementation detail whose value would read as a metric.
    assert "_coverage_sum" not in c.as_dict()


def test_coverage_estimate_averages_only_the_cycles_that_measured_one() -> None:
    # Cycles that cannot produce an estimate (the first of a run, or one where the
    # feed did not advance) are excluded, not scored zero. Counting "no estimate"
    # as "no coverage" would drag the mean toward a number nothing measured — the
    # same conflation the capture KPI already made once.
    c = PollCounters()
    c.record_cycle(fetched=5, fresh=5, coverage=None)
    c.record_cycle(fetched=5, fresh=5, coverage=0.4)
    c.record_cycle(fetched=5, fresh=5, coverage=0.6)

    assert c.cycles == 3
    assert c.coverage_samples == 2
    assert c.coverage_estimate == 0.5  # not 0.333 — the None cycle is absent, not zero


def test_consume_counters_as_dict_snapshots_all_fields_including_duplicates() -> None:
    # The scrape seam for the consumer: every field, plus the derived duplicates —
    # a snapshot that silently zeroed or dropped one would poison the metrics
    # downstream while every direct counter assertion stayed green.
    c = ConsumeCounters()
    c.record_batch(consumed=5, stored=3, dead_lettered=1)
    c.record_batch(consumed=4, stored=2, dead_lettered=1)

    assert c.as_dict() == {
        "batches": 2,
        "consumed": 9,
        "stored": 5,
        "dead_lettered": 2,
        "duplicates": 2,
    }
