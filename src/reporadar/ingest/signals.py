"""Graceful shutdown: OS signals → the poller's cooperative stop event.

``poll_stream`` stops promptly when its ``stop`` event is set (checked each
cycle and raced against the inter-cycle sleep); this module is the last inch —
translating SIGTERM/SIGINT into that event so ``docker stop``, systemd, and
Ctrl-C end a run after the current cycle (final counters logged, the current
batch fully written) instead of killing it mid-write.

While the context is active, SIGINT no longer raises ``KeyboardInterrupt`` —
Ctrl-C *requests* a stop. Handlers are installed on the running loop and
removed on exit, so nothing outlives the run. Unix-only by design
(``loop.add_signal_handler`` is unavailable on Windows's proactor loop); dev
is macOS and the deploy target is Linux.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DEFAULT_SIGNALS = (signal.SIGINT, signal.SIGTERM)


@contextmanager
def stop_on_signals(*sigs: signal.Signals) -> Iterator[asyncio.Event]:
    """Yield a stop event that a shutdown signal sets (SIGINT + SIGTERM by default).

    Enter inside a running event loop. Not nestable: a process has one set of
    signal dispositions, so one active context per process — which is the
    deployment shape anyway (one service per process).
    """
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    installed = sigs or DEFAULT_SIGNALS
    for sig in installed:
        loop.add_signal_handler(sig, _request_stop, sig, stop)
    try:
        yield stop
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def _request_stop(sig: signal.Signals, stop: asyncio.Event) -> None:
    """Loop-side signal callback: announce once, then set the event."""
    if not stop.is_set():  # a repeated signal is a no-op, not a re-announcement
        logger.info("received %s; stopping after the current cycle", sig.name)
        stop.set()
