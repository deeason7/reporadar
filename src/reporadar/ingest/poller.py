"""Live /events poller: page sweep → dedupe → NDJSON on disk.

The public /events feed is a rolling, pagination-capped window — a single
poller cannot see everything at peak. The poller's job is *freshness*;
*completeness* comes from the hourly archive, and the gap between the two is
measured (capture rate), never assumed away.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

from reporadar.config import Settings
from reporadar.github.client import GitHubClient, RateLimitedError
from reporadar.github.events import RawEvent, dedupe
from reporadar.ingest.dedup import RecentIds
from reporadar.ingest.metrics import PollCounters

logger = logging.getLogger(__name__)

# Retry-After is chosen by the server, so it is an input, not a decision. Waiting
# out an arbitrary one leaves a poller indistinguishable from a hung process, and
# the caller has no way to tell which it is looking at. Two minutes is long enough
# to outlast a normal reset window and short enough that a stuck run is obvious.
# Shared with the always-on loop in ``service``: both trust the header the same
# far, and two constants holding one number is how they stop agreeing.
MAX_RATE_LIMIT_PAUSE_S = 120.0


async def poll_once(client: GitHubClient, pages: int = 3, per_page: int = 100) -> list[RawEvent]:
    """Sweep the first ``pages`` pages of /events and dedupe across them."""
    events: list[RawEvent] = []
    for page in range(1, pages + 1):
        events.extend(await client.list_public_events(page=page, per_page=per_page))
    return dedupe(events)


async def collect_sample(
    settings: Settings,
    cycles: int = 10,
    interval_s: float = 10.0,
    pages: int = 3,
    seen_window: int = 50_000,
) -> Path:
    """Poll for a while and write one NDJSON sample file, deduped across cycles.

    Cross-cycle dedup uses a bounded ``RecentIds`` window (``seen_window``), so
    a long run can't leak memory; the tradeoff is that an id can re-appear as
    fresh once ``seen_window`` newer ids have arrived since it was last seen.
    On rate limiting, waits out the reset (capped) instead of dying — a sample
    with a gap is still a sample; a crashed sampler is nothing.
    """
    settings.live_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(tz=UTC)
    out = settings.live_dir / f"events_{started:%Y%m%dT%H%M%SZ}.ndjson"

    seen = RecentIds(maxlen=seen_window)
    counters = PollCounters()
    async with GitHubClient(settings) as client:
        with out.open("w", encoding="utf-8") as fh:
            for cycle in range(cycles):
                try:
                    batch = await poll_once(client, pages=pages)
                except RateLimitedError as exc:
                    counters.record_rate_limited()
                    await asyncio.sleep(min(exc.retry_after_s, MAX_RATE_LIMIT_PAUSE_S))
                    continue
                fresh = [event for event in batch if seen.add(event.id)]
                for event in fresh:
                    fh.write(event.model_dump_json() + "\n")
                counters.record_cycle(fetched=len(batch), fresh=len(fresh))
                if cycle < cycles - 1:
                    await asyncio.sleep(interval_s)
    logger.info(
        "poll sample complete: cycles=%d fetched=%d fresh=%d duplicates=%d rate_limited=%d out=%s",
        counters.cycles,
        counters.fetched,
        counters.fresh,
        counters.duplicates,
        counters.rate_limited,
        out.name,
    )
    return out
