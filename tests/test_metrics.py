from __future__ import annotations

from reporadar.ingest.ledger import HourStatus
from reporadar.ingest.metrics import ArchiveCounters, ConsumeCounters, PollCounters


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
    }


def test_poll_counters_report_no_estimate_of_the_share_of_the_feed_seen() -> None:
    # The capture-ratio estimate was retired. This asserts the *absence* rather than
    # trusting the snapshot above to stay exhaustive: that dict is an equality check,
    # so a re-added estimate would fail it -- but it would fail it as "an unexpected
    # key", which reads as a test needing an update. Naming the retired field makes
    # the failure say what it means.
    c = PollCounters()
    c.record_cycle(fetched=5, fresh=5)
    c.record_cycle(fetched=5, fresh=4)

    snapshot = c.as_dict()
    for retired in ("coverage_estimate", "coverage_samples", "_coverage_sum"):
        assert retired not in snapshot, (
            f"{retired} is back in the counters; it was retired because its residual "
            "error had no explanation -- reinstating it is a decision, not a fix"
        )
    # Every reported figure is an exact count of something that happened.
    assert all(isinstance(v, int) for v in snapshot.values())
    assert snapshot["fetched"] == 10
    assert snapshot["duplicates"] == 1


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


def test_archive_counters_count_each_outcome_as_itself() -> None:
    # The four outcomes are not interchangeable: an hour left deliberately
    # unrecorded is the loop's most common healthy state, and folding it into
    # either successes or errors would hide exactly that.
    c = ArchiveCounters()
    c.record_pass(due=4)
    c.record_hour(status=HourStatus.INGESTED, events=100)
    c.record_hour(status=HourStatus.MISSING, events=None)
    c.record_hour(status=HourStatus.FAILED, events=None)
    c.record_hour(status=None, events=None)

    assert c.as_dict() == {
        "passes": 1,
        "due": 4,
        "ingested": 1,
        "missing": 1,
        "failed": 1,
        "outstanding": 1,
        "events": 100,
    }


def test_a_pass_that_found_nothing_still_counts_as_a_pass() -> None:
    # Otherwise a healthy, quiet loop is indistinguishable from a stopped one.
    c = ArchiveCounters()
    c.record_pass(due=0)

    assert c.passes == 1
    assert c.due == 0
    assert c.ingested == 0


def test_only_an_ingested_hour_contributes_events() -> None:
    # A missing or failed hour has no count, and 'no count' must never be
    # summed as zero into a figure that reads as coverage.
    c = ArchiveCounters()
    c.record_hour(status=HourStatus.INGESTED, events=50)
    c.record_hour(status=HourStatus.FAILED, events=99)

    assert c.events == 50
