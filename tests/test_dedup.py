from __future__ import annotations

import pytest

from reporadar.ingest.dedup import RecentIds


def test_add_reports_first_sighting_then_duplicate() -> None:
    seen = RecentIds(maxlen=8)
    assert seen.add("a") is True  # first time → new
    assert seen.add("a") is False  # second time → duplicate
    assert "a" in seen
    assert len(seen) == 1


def test_evicts_oldest_when_full() -> None:
    seen = RecentIds(maxlen=2)
    seen.add("a")
    seen.add("b")
    seen.add("c")  # pushes the window past capacity → "a" falls out

    assert "a" not in seen
    assert "b" in seen and "c" in seen
    assert len(seen) == 2


def test_eviction_is_fifo_not_lru() -> None:
    # Re-seeing an id must NOT refresh its position — the window slides by
    # arrival order. With LRU semantics this test would keep {a, c}; FIFO keeps
    # {b, c}. That distinction is the whole point of the structure.
    seen = RecentIds(maxlen=2)
    seen.add("a")
    seen.add("b")
    assert seen.add("a") is False  # duplicate: no refresh
    seen.add("c")  # evicts the oldest *arrival*, which is still "a"

    assert "a" not in seen
    assert "b" in seen and "c" in seen


def test_a_re_added_id_after_eviction_looks_new() -> None:
    seen = RecentIds(maxlen=1)
    assert seen.add("a") is True
    assert seen.add("b") is True  # evicts "a"
    assert seen.add("a") is True  # "a" is gone from the window → new again


@pytest.mark.parametrize("bad", [0, -1, -100])
def test_maxlen_must_be_positive(bad: int) -> None:
    with pytest.raises(ValueError):
        RecentIds(maxlen=bad)


def test_maxlen_is_exposed() -> None:
    assert RecentIds(maxlen=7).maxlen == 7
