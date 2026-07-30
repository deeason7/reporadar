"""Command-line entrypoints for the ingestion workflow."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime
from pathlib import Path

import typer

from reporadar.analysis.capture import capture_rate, type_counts
from reporadar.config import get_settings
from reporadar.ingest.archive import download_hour
from reporadar.ingest.consumer import consume_stream
from reporadar.ingest.converge import (
    DEFAULT_CONCURRENCY,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_SCAN_INTERVAL_S,
    converge_forever,
    converge_once,
)
from reporadar.ingest.kafka import kafka_dead_letter_sink, kafka_sink, kafka_source
from reporadar.ingest.ledger import create_schema
from reporadar.ingest.metrics import ArchiveCounters, ConsumeCounters, PollCounters
from reporadar.ingest.poller import collect_sample
from reporadar.ingest.service import poll_stream
from reporadar.ingest.signals import stop_on_signals
from reporadar.ingest.sinks import HourlyNdjsonSink, TeeSink
from reporadar.ingest.store import pg_connection, pg_store
from reporadar.ingest.topics import provision_topics, require_topics

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
    """Run the always-on poller: fresh events go to hourly NDJSON files and the stream."""
    # The service's logs are its interface while it runs; the library only ever
    # emits, so the long-running entrypoint is where logging gets configured.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    async def _run() -> tuple[PollCounters, int]:
        # Only the live topic: serve produces there and nowhere else, so it must
        # not refuse to start over the dead-letter topic it never touches. This
        # also turns a missing topic or an unreachable broker into an immediate,
        # named failure instead of the producer's 40-second metadata stall.
        await require_topics(settings, [settings.kafka_live_topic])
        with stop_on_signals() as stop:  # SIGINT/SIGTERM end the run after the current cycle
            async with kafka_sink(settings) as stream:
                # The hourly files are the reconciliation record and come first;
                # the stream is best-effort, so a broker blip costs freshness, not
                # the capture service. See TeeSink.
                sink = TeeSink(HourlyNdjsonSink(settings.live_dir), stream)
                counters = await poll_stream(
                    settings,
                    sink,
                    interval_s=interval_s,
                    pages=pages,
                    seen_window=settings.seen_window,
                    max_cycles=cycles,
                    stop=stop,
                )
                return counters, sink.dropped

    counters, stream_drops = asyncio.run(_run())
    typer.echo(f"stopped: {counters.as_dict()} stream_drops={stream_drops}")


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
        # Before anything opens: a missing topic otherwise stalls the consumer for
        # its whole request timeout and then raises an error naming neither the
        # topic nor the broker. One read-only round trip buys a legible failure.
        # Both topics: the consumer reads the live stream and writes the dead-letter one.
        await require_topics(settings, [settings.kafka_live_topic, settings.kafka_dlq_topic])
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


@app.command(name="archive-serve")
def archive_serve(
    interval_s: float = DEFAULT_SCAN_INTERVAL_S,
    concurrency: int = DEFAULT_CONCURRENCY,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    passes: int | None = None,
) -> None:
    """Keep the lake converged on the published archive: scan for gaps, ingest, repeat."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    async def _run() -> ArchiveCounters:
        # Signals outermost, as in serve and consume: a SIGTERM arriving while the
        # connection is still being established should end the run rather than be
        # missed because the handler was not installed yet.
        with stop_on_signals() as stop:
            async with pg_connection(settings) as connection:
                return await converge_forever(
                    connection,
                    archive_dir=settings.archive_dir,
                    lake_dir=settings.lake_dir,
                    concurrency=concurrency,
                    lookback_days=lookback_days,
                    interval_s=interval_s,
                    # The configured publisher, not the library default: an
                    # unreachable host in a test or a mirror in a deployment is a
                    # setting, and fetch-archive already honours the same one.
                    base_url=settings.archive_base,
                    max_passes=passes,
                    stop=stop,
                )

    counters = asyncio.run(_run())
    typer.echo(f"stopped: {counters.as_dict()}")


@app.command()
def backfill(
    first_day: str,
    last_day: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    retry_failed: bool = True,
) -> None:
    """Ingest one explicit range of archive hours (DAYs = YYYY-MM-DD, both inclusive)."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()
    first = date.fromisoformat(first_day)
    last = date.fromisoformat(last_day)
    if first > last:
        # Refused rather than passed through. The ledger scan builds its calendar
        # with generate_series, which yields no rows for a descending range — so a
        # transposed pair of dates would report "nothing outstanding" and exit 0.
        # A typo that reads as a completed backfill is the worst available outcome,
        # because the hours it silently skipped look settled to every later reader.
        raise typer.BadParameter(f"FIRST_DAY {first} is after LAST_DAY {last}")

    async def _run() -> ArchiveCounters:
        # Deliberately no stop_on_signals here. converge_once takes no stop event,
        # so installing handlers would route Ctrl-C into an event nothing reads and
        # leave a long range killable only by SIGKILL. Keeping the default
        # disposition means KeyboardInterrupt is the interruption — and an
        # interrupted range loses nothing, since every hour already recorded stays
        # recorded and a re-run resumes from the ledger rather than the start.
        async with pg_connection(settings) as connection:
            # converge_forever ensures the schema; converge_once does not, and a
            # backfill is often the first thing ever run against a database — the
            # scan would otherwise fail on a table that does not exist yet.
            await create_schema(connection)
            return await converge_once(
                connection,
                archive_dir=settings.archive_dir,
                lake_dir=settings.lake_dir,
                # One instant for the whole pass, so every hour in this range is
                # judged closed-or-not and past-grace-or-not against the same
                # clock. Reading the clock per hour would let a long pass decide
                # two identical hours differently.
                now=datetime.now(UTC),
                first_day=first,
                last_day=last,
                concurrency=concurrency,
                # The default that distinguishes this caller: an explicit range is
                # how a fix reaches the hours it fixed, and those are exactly the
                # ones the always-on loop skips so it cannot spin.
                retry_failed=retry_failed,
                base_url=settings.archive_base,
            )

    counters = asyncio.run(_run())
    typer.echo(f"done: {counters.as_dict()}")


@app.command()
def provision(check: bool = False) -> None:
    """Create the Kafka topics the pipeline needs (idempotent; safe to re-run)."""
    # The warnings are the output that matters here, so the logger is configured
    # exactly as it is for the long-running commands.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()
    report = asyncio.run(provision_topics(settings, check_only=check))
    for outcome in report.outcomes:
        if outcome.missing:
            state = "MISSING"
        elif outcome.created:
            state = "created"
        else:
            state = "exists"
        typer.echo(
            f"{outcome.name:<24} {state:<8} "
            f"partitions={outcome.partitions} replication={outcome.replication_factor}"
        )
    typer.echo(report.as_dict())
    if check and not report.ready:
        # Only --check is strict. Provisioning itself must not fail on drift, or
        # it would brick every run against an existing, deliberately-sized cluster.
        raise typer.Exit(code=1)


@app.command(name="capture-rate")
def capture_rate_cmd(archive_path: Path, live_path: Path) -> None:
    """The completeness KPI: how much of an archived hour the live sample caught."""
    report = capture_rate(archive_path, live_path)
    typer.echo(f"archive events : {report.archive_events:,}")
    typer.echo(f"live events    : {report.live_events:,}")
    typer.echo(f"matched        : {report.matched:,}")
    rate = report.capture_rate
    if rate is None:
        # Naming the cause, not just the condition: "0.0%" here would read as a
        # captured-nothing hour and send the reader after the poller, when the
        # two files in fact share no event identity at all. Exit non-zero so a
        # scheduled run cannot record this as a successful measurement.
        typer.echo(
            f"capture rate   : NOT RECONCILABLE — none of the {report.live_events:,} sampled "
            "events appear in this archive hour, so no rate can be computed. The sample and "
            "the archive hour do not share an event identifier."
        )
        raise typer.Exit(code=1)
    typer.echo(f"capture rate   : {rate:.1%}")


def main() -> None:
    app()
