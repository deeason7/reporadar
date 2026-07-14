from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest
import respx

from reporadar.config import Settings
from reporadar.github.client import GitHubClient
from reporadar.github.events import iter_ndjson
from reporadar.ingest.poller import collect_sample, poll_once

EVENTS_URL = "https://api.github.com/events"


def _page(event_dict: dict[str, Any], *ids: str) -> httpx.Response:
    """A /events page whose items differ from the fixture only by id."""
    return httpx.Response(200, json=[{**event_dict, "id": id_} for id_ in ids])


@respx.mock
async def test_poll_once_sweeps_pages_and_dedupes(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # Consecutive pages overlap by design; the sweep must keep first-seen order.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    async with GitHubClient(settings) as client:
        events = await poll_once(client, pages=2, per_page=2)

    assert [event.id for event in events] == ["a", "b", "c"]


@respx.mock
async def test_collect_sample_writes_ndjson_deduped_across_cycles(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    out = await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    assert out.parent == settings.live_dir  # lands where config says samples live
    assert out.name.startswith("events_") and out.name.endswith(".ndjson")
    # Read the file back through the system's own parser: producer/consumer contract.
    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a", "b", "c"]


@respx.mock
async def test_collect_sample_bounded_window_can_reemit(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # A window of 1 keeps only the most recent id: once "b" arrives it evicts
    # "a", so "a" reappearing later is written again. This is the documented,
    # bounded cost of capped-memory dedup — the archive is the completeness
    # arbiter, not this window.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a"), _page(event_dict, "b"), _page(event_dict, "a")]
    )
    out = await collect_sample(settings, cycles=3, interval_s=0.0, pages=1, seen_window=1)

    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a", "b", "a"]


@respx.mock
async def test_collect_sample_logs_run_summary(
    settings: Settings, event_dict: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    # Overlapping pages across two cycles: 4 fetched, 3 fresh (a,b,c), 1 duplicate.
    respx.get(EVENTS_URL).mock(
        side_effect=[_page(event_dict, "a", "b"), _page(event_dict, "b", "c")]
    )
    with caplog.at_level(logging.INFO, logger="reporadar.ingest.poller"):
        await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    # Rule 10: a long-running poller says what it did. Structured, grep-able fields.
    assert "poll sample complete" in caplog.text
    assert "cycles=2" in caplog.text
    assert "fetched=4" in caplog.text
    assert "fresh=3" in caplog.text
    assert "duplicates=1" in caplog.text
    assert "rate_limited=0" in caplog.text


@respx.mock
async def test_collect_sample_survives_rate_limiting(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    rate_limited = httpx.Response(
        403,
        json={"message": "API rate limit exceeded"},
        headers={"X-RateLimit-Remaining": "0", "Retry-After": "0"},
    )
    respx.get(EVENTS_URL).mock(side_effect=[rate_limited, _page(event_dict, "a")])
    out = await collect_sample(settings, cycles=2, interval_s=0.0, pages=1)

    # The rate-limited cycle is a gap in the sample, not a crash of the sampler.
    events = list(iter_ndjson(out.read_text(encoding="utf-8").splitlines()))
    assert [event.id for event in events] == ["a"]
