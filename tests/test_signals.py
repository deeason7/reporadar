from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Sequence
from typing import Any

import httpx
import pytest
import respx

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.service import poll_stream
from reporadar.ingest.signals import stop_on_signals

EVENTS_URL = "https://api.github.com/events"


async def test_sigterm_sets_the_stop_event(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="reporadar.ingest.signals"):
        with stop_on_signals(signal.SIGTERM) as stop:
            assert not stop.is_set()
            os.kill(os.getpid(), signal.SIGTERM)  # intercepted by the loop handler
            await asyncio.wait_for(stop.wait(), timeout=2.0)

    assert "received SIGTERM" in caplog.text  # the stop announces itself


async def test_repeated_signal_announces_once(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="reporadar.ingest.signals"):
        with stop_on_signals(signal.SIGTERM) as stop:
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(stop.wait(), timeout=2.0)  # first delivery handled
            os.kill(os.getpid(), signal.SIGTERM)  # a second, genuinely separate delivery
            await asyncio.sleep(0.05)  # give the loop a turn to run the callback

    assert caplog.text.count("received SIGTERM") == 1  # a repeat is a no-op, not noise


async def test_handlers_install_and_do_not_outlive_the_context() -> None:
    loop = asyncio.get_running_loop()
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

    with stop_on_signals():  # the default set: SIGINT + SIGTERM
        for sig, previous in before.items():
            assert signal.getsignal(sig) is not previous  # handler actually installed

    assert loop.remove_signal_handler(signal.SIGINT) is False  # nothing left behind
    assert loop.remove_signal_handler(signal.SIGTERM) is False
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL  # default disposition restored


async def test_an_explicit_signal_set_is_not_quietly_widened() -> None:
    # Asking for SIGTERM only must leave SIGINT alone. Installing the default set
    # regardless would take Ctrl-C away from a caller who never asked: inside this
    # context SIGINT stops raising KeyboardInterrupt, so a caller that wanted an
    # interactive abort would silently get a graceful stop instead.
    loop = asyncio.get_running_loop()

    with stop_on_signals(signal.SIGTERM):
        assert loop.remove_signal_handler(signal.SIGINT) is False  # never installed


async def test_handlers_do_not_outlive_a_crashing_run() -> None:
    # The dangerous direction: handlers left installed after the run they belong
    # to has gone. They would point at a stop event nobody is watching, so the
    # next SIGTERM would be absorbed and do nothing at all.
    loop = asyncio.get_running_loop()

    with pytest.raises(RuntimeError):
        with stop_on_signals(signal.SIGTERM):
            raise RuntimeError("service crashed mid-run")

    assert loop.remove_signal_handler(signal.SIGTERM) is False
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


@respx.mock
async def test_sigterm_stops_a_running_poll_stream_promptly(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # The whole chain, end to end: SIGTERM → loop handler → stop event → the 30s
    # inter-cycle sleep is cut short → clean exit with counters. Without the
    # interruption, the 2s wait below would time out.
    respx.get(EVENTS_URL).mock(return_value=httpx.Response(200, json=[event_dict]))
    first_batch = asyncio.Event()

    async def sink(events: Sequence[RawEvent]) -> None:
        assert [event.id for event in events] == ["45000000001"]  # the fixture event arrived
        first_batch.set()

    with stop_on_signals(signal.SIGTERM) as stop:
        task = asyncio.create_task(poll_stream(settings, sink, interval_s=30.0, pages=1, stop=stop))
        await asyncio.wait_for(first_batch.wait(), timeout=2.0)  # one cycle done; loop sleeping
        os.kill(os.getpid(), signal.SIGTERM)
        counters = await asyncio.wait_for(task, timeout=2.0)

    assert counters.cycles == 1
    assert counters.fresh == 1
