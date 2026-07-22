"""Command-line entrypoints for the ingestion workflow."""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path

import typer

from reporadar.analysis.capture import capture_rate, type_counts
from reporadar.config import get_settings
from reporadar.ingest.archive import download_hour
from reporadar.ingest.consumer import consume_stream
from reporadar.ingest.kafka import kafka_dead_letter_sink, kafka_source
from reporadar.ingest.metrics import ConsumeCounters, PollCounters
from reporadar.ingest.poller import collect_sample
from reporadar.ingest.service import poll_stream
from reporadar.ingest.signals import stop_on_signals
from reporadar.ingest.sinks import HourlyNdjsonSink
from reporadar.ingest.store import pg_store

app = typer.Typer(help="RepoRadar — ecosystem intelligence tooling", no_args_is_help=True)


@app.command()
def fetch_archive(day: str, hour: int) -> None:
    """Download one GH Archive hour (DAY = YYYY-MM-DD, HOUR = 0-23)."""
    settings = get_settings()
    path = download_hour(
        date.fromisoformat(day), hour, settings.archive_dir, base_url=settings.archive_base
    )
    typer.echo(f"archive hour → {path}")


@app.command()
def explore(archive_path: Path) -> None:
    """Event-type counts for a downloaded archive hour."""
    total = 0
    for event_type, n in type_counts(archive_path):
        total += n
        typer.echo(f"{event_type:<32} {n:>10,}")
    typer.echo(f"{'TOTAL':<32} {total:>10,}")


@app.command()
def poll(cycles: int = 10, interval_s: float = 10.0, pages: int = 3) -> None:
    """Sample the live /events feed into an NDJSON file (token strongly recommended)."""
    settings = get_settings()
    out = asyncio.run(
        collect_sample(
            settings,
            cycles=cycles,
            interval_s=interval_s,
            pages=pages,
            seen_window=settings.seen_window,
        )
    )
    typer.echo(f"live sample → {out}")


@app.command()
def serve(cycles: int | None = None, interval_s: float = 10.0, pages: int = 3) -> None:
    """Run the always-on poller, capturing fresh events to hourly NDJSON files."""
    # The service's logs are its interface while it runs; the library only ever
    # emits, so the long-running entrypoint is where logging gets configured.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()
    sink = HourlyNdjsonSink(settings.live_dir)

    async def _run() -> PollCounters:
        with stop_on_signals() as stop:  # SIGINT/SIGTERM end the run after the current cycle
            return await poll_stream(
                settings,
                sink,
                interval_s=interval_s,
                pages=pages,
                seen_window=settings.seen_window,
                max_cycles=cycles,
                stop=stop,
            )

    counters = asyncio.run(_run())
    typer.echo(f"stopped: {counters.as_dict()}")


@app.command()
def consume(seen_window: int | None = None, report_every: int = 60) -> None:
    """Read the event stream into the validated store, dead-lettering what will not decode."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()
    # Absent the flag, the deployment's configured window applies — the same
    # precedence the settings themselves use, where an explicit argument
    # outranks the environment. The commands are where that choice is made;
    # the loops below take a number and ask no questions about where it came from.
    window = settings.seen_window if seen_window is None else seen_window

    async def _run() -> ConsumeCounters:
        with stop_on_signals() as stop:  # SIGINT/SIGTERM end the run before the next pull
            # Opened outside-in, so they close inside-out. The store goes first
            # because its missing-DSN check is pure config: a misconfigured run
            # fails before joining the consumer group and making the broker
            # rebalance for nothing. The source goes last, so it is the first
            # thing closed — reading stops before the store and dead-letter topic
            # it feeds are torn down.
            async with (
                pg_store(settings) as store,
                kafka_dead_letter_sink(settings) as dead_letter,
                kafka_source(settings) as source,
            ):
                return await consume_stream(
                    source,
                    store,
                    dead_letter,
                    seen_window=window,
                    report_every=report_every,
                    stop=stop,
                )

    counters = asyncio.run(_run())
    typer.echo(f"stopped: {counters.as_dict()}")


@app.command(name="capture-rate")
def capture_rate_cmd(archive_path: Path, live_path: Path) -> None:
    """The completeness KPI: how much of an archived hour the live sample caught."""
    report = capture_rate(archive_path, live_path)
    typer.echo(f"archive events : {report.archive_events:,}")
    typer.echo(f"live events    : {report.live_events:,}")
    typer.echo(f"matched        : {report.matched:,}")
    typer.echo(f"capture rate   : {report.capture_rate:.1%}")


def main() -> None:
    app()
