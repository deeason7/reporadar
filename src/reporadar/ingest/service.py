"""Always-on live-events poller (service mode).

``collect_sample`` takes a *bounded* sample into an NDJSON file; this is the
*always-on* version. It sweeps /events on an interval, dedupes across the whole
run with a bounded window, hands each batch of fresh events to a **sink**, and
reports progress periodically. The sink is abstracted on purpose — the same
loop feeds an NDJSON writer today and a message producer later; the loop never
learns where events go.

Shutdown is cooperative: pass a ``stop`` event (checked each cycle and used to
cut short the inter-cycle sleep) so a signal handler can stop the service
promptly instead of waiting out a full interval.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence

from reporadar.config import Settings
from reporadar.github.client import GitHubClient, RateLimitedError
from reporadar.github.events import RawEvent
from reporadar.ingest.coverage import CoverageTracker, numeric_ids
from reporadar.ingest.dedup import DEFAULT_SEEN_WINDOW, RecentIds
from reporadar.ingest.metrics import PollCounters
from reporadar.ingest.poller import MAX_RATE_LIMIT_PAUSE_S, effective_interval, poll_once

logger = logging.getLogger(__name__)

EventSink = Callable[[Sequence[RawEvent]], Awaitable[None]]
"""An async consumer of fresh events — an NDJSON writer now, a Kafka producer later."""

# The pause ceiling is defined in ``poller`` and belongs to this module's surface
# too — it is part of what ``poll_stream`` promises about how long it will wait.
# Saying so explicitly is required, not decorative: under strict typing a name
# that a module merely imports is not re-exported, so callers could not read it
# from here without this line.
__all__ = ["MAX_RATE_LIMIT_PAUSE_S", "EventSink", "poll_stream"]


async def poll_stream(
    settings: Settings,
    sink: EventSink,
    *,
    interval_s: float = 10.0,
    pages: int = 3,
    seen_window: int = DEFAULT_SEEN_WINDOW,
    report_every: int = 60,
    max_cycles: int | None = None,
    stop: asyncio.Event | None = None,
) -> PollCounters:
    """Poll /events until stopped, handing each batch of fresh events to ``sink``.

    Runs indefinitely by default. Pass ``stop`` for cooperative shutdown or
    ``max_cycles`` to bound the run (tests, one-off captures). Rate limiting
    *pauses* the loop (capped) rather than ending it — an ingester that dies on
    throttling is worse than one that waits. Progress is logged every
    ``report_every`` cycles (0 disables) and once more on exit. Returns the final
    counters so a caller can assert on or surface the run.
    """
    seen = RecentIds(maxlen=seen_window)
    counters = PollCounters()
    # Fed the whole batch, not just the fresh events: coverage is a question about
    # the feed's own continuity, and dropping the ids we have already stored would
    # punch holes in the very sequence being measured.
    coverage = CoverageTracker()
    announced_interval = interval_s
    async with GitHubClient(settings) as client:
        while True:
            if stop is not None and stop.is_set():
                break
            if max_cycles is not None and counters.cycles >= max_cycles:
                break
            try:
                batch = await poll_once(client, pages=pages)
            except RateLimitedError as exc:
                pause = min(exc.retry_after_s, MAX_RATE_LIMIT_PAUSE_S)
                counters.record_rate_limited()
                logger.warning("rate limited; pausing %.0fs then resuming", pause)
                await _interruptible_sleep(pause, stop)
                continue
            cycle_coverage = coverage.record(numeric_ids(event.id for event in batch))
            fresh = [event for event in batch if seen.add(event.id)]
            if fresh:
                await sink(fresh)
            counters.record_cycle(fetched=len(batch), fresh=len(fresh), coverage=cycle_coverage)
            if report_every > 0 and counters.cycles % report_every == 0:
                logger.info("poll progress: %s", counters.as_dict())
            sleep_s = effective_interval(interval_s, client.last_poll_interval_s)
            if sleep_s != announced_interval:
                # Say it once per change rather than every cycle: an operator who
                # configured 10s and is being paced at 60s must be able to find
                # out why without reading the source.
                if sleep_s != interval_s:
                    logger.info(
                        "polling every %.0fs: the server asks for %.0fs, overriding the "
                        "configured %.0fs",
                        sleep_s,
                        client.last_poll_interval_s or sleep_s,
                        interval_s,
                    )
                announced_interval = sleep_s
            await _interruptible_sleep(sleep_s, stop)
    logger.info("poll stream stopped: %s", counters.as_dict())
    return counters


async def _interruptible_sleep(seconds: float, stop: asyncio.Event | None) -> None:
    """Sleep ``seconds``, but wake immediately if ``stop`` is set."""
    if stop is None:
        await asyncio.sleep(seconds)
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass  # the interval elapsed without a stop — the normal path
