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
from reporadar.ingest.repair import (
    DEFAULT_REPAIR_CONCURRENCY,
    INCOMPLETE_EXIT_CODE,
    RepairReport,
    repair_unbacked,
)
from reporadar.ingest.service import poll_stream
from reporadar.ingest.signals import stop_on_signals
from reporadar.ingest.sinks import HourlyNdjsonSink, TeeSink
from reporadar.ingest.store import pg_connection, pg_store
from reporadar.ingest.topics import provision_topics, require_topics
from reporadar.ingest.verify import UNBACKED_EXIT_CODE, VerifyReport, verify_lake
from reporadar.marts.freshness import STALE_EXIT_CODE, FreshnessReport, marts_freshness

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
def serve(
    cycles: int | None = None,
    interval_s: float = 10.0,
    pages: int = 3,
    report_every: int = 60,
) -> None:
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
                # The hourly files hold the only copy and come first; the stream
                # is best-effort and carries nothing that is not already on disk,
                # so a broker blip costs freshness, not the capture service.
                # See TeeSink.
                sink = TeeSink(HourlyNdjsonSink(settings.live_dir), stream)
                # report_every counts *cycles*, not seconds, and the cycle length is
                # the server's to set: /events answers X-Poll-Interval: 60 and
                # effective_interval takes the slower of that and --interval-s. So the
                # default is an hour of silence, and lowering --interval-s shortens
                # neither the polling nor the reporting — this flag is the only lever
                # on how often a run says what it has done.
                counters = await poll_stream(
                    settings,
                    sink,
                    interval_s=interval_s,
                    pages=pages,
                    seen_window=settings.seen_window,
                    report_every=report_every,
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
    keep_source: bool = False,
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
                    # A converted hour's source is a cache of an immutable file the
                    # publisher still serves, so an always-on run discards it: keeping
                    # every one grows the data directory two and a half times faster,
                    # and disk is the first thing a loop that never stops runs out of.
                    # --keep-source is for a laptop that wants the raw hour to hand.
                    keep_source=keep_source,
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
    keep_source: bool = False,
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
                # Same policy as the always-on loop, and for a sharper reason here: a
                # range is the command most likely to be pointed at a month.
                keep_source=keep_source,
            )

    counters = asyncio.run(_run())
    typer.echo(f"done: {counters.as_dict()}")
    if counters.outstanding or counters.failed:
        # An explicit range is a claim that these hours are wanted now, so "the pass
        # ran" is not the same as "the range is complete". `outstanding` counts hours
        # attempted and deliberately left for a later pass; `failed` counts hours that
        # arrived and could not be trusted. Either way the range did not converge, and
        # exiting 0 tells the caller it did — a Makefile, a script or CI branches on
        # that, and the hours it skipped then look settled to every later reader. Same
        # failure this command already refuses for a transposed range, reached by a
        # slower road.
        #
        # `missing` is excluded deliberately: an hour the publisher never published is
        # a settled answer, not an unfinished job, and folding it in would make a
        # complete backfill of an incomplete archive report failure forever.
        #
        # See INCOMPLETE_EXIT_CODE — the same fact `repair-lake` reports, so it is the
        # same constant rather than a fourth spelling of 3.
        raise typer.Exit(code=INCOMPLETE_EXIT_CODE)


@app.command()
def verify(counts: bool = False) -> None:
    """Check that every hour the record claims is in the columnar store really is."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    async def _run() -> VerifyReport:
        async with pg_connection(settings) as connection:
            # Read-only, so the schema is ensured rather than assumed: verifying a
            # database that has never been ingested into should report "nothing
            # claimed", not fail on a missing table.
            await create_schema(connection)
            return await verify_lake(connection, lake_dir=settings.lake_dir, check_counts=counts)

    report = asyncio.run(_run())
    for finding in report.findings:
        marker = "UNBACKED" if finding.unbacked else "surplus "
        typer.echo(
            f"{marker} {finding.day} {finding.hour:02d}  {finding.problem}: {finding.detail}"
        )
    typer.echo(report.as_dict())
    if report.unsized:
        # Not a failure, but the report would otherwise imply a stronger check ran
        # than actually did on those hours.
        typer.echo(f"note: {report.unsized} hour(s) carry no recorded size; presence only")
    if not report.ok:
        # Only unbacked claims fail. A surplus file misreports nothing — the next
        # scan converts that hour again — while a claim with no file is a number
        # that lies, and nothing revisits a settled hour to discover it.
        typer.echo(
            f"FAILED: {len(report.unbacked)} of {report.claimed} recorded hour(s) are not "
            "backed by the file they claim. Repair with `reporadar repair-lake`."
        )
        # A distinct code, so a caller can tell this from "the check crashed".
        # See UNBACKED_EXIT_CODE.
        raise typer.Exit(code=UNBACKED_EXIT_CODE)


@app.command(name="repair-lake")
def repair_lake(
    dry_run: bool = False,
    counts: bool = False,
    concurrency: int = DEFAULT_REPAIR_CONCURRENCY,
    keep_source: bool = True,
) -> None:
    """Re-derive the record for hours it claims that the columnar store does not hold.

    Destructive by design: it removes the claims a check has proven untrue, then
    fetches those hours again so the record is written by a real download. Use
    --dry-run to see what it would do.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    async def _run() -> RepairReport:
        async with pg_connection(settings) as connection:
            # Ensured rather than assumed, matching verify: repairing a database
            # that was never ingested into should report "nothing to repair"
            # instead of failing on a table that does not exist.
            await create_schema(connection)
            return await repair_unbacked(
                connection,
                lake_dir=settings.lake_dir,
                archive_dir=settings.archive_dir,
                # One instant for the whole pass, for the same reason the backfill
                # takes one: every hour is judged closed-or-not against the same
                # clock rather than against whenever its turn came round.
                now=datetime.now(UTC),
                concurrency=concurrency,
                dry_run=dry_run,
                check_counts=counts,
                keep_source=keep_source,
            )

    report = asyncio.run(_run())
    for item in report.reconciliations:
        typer.echo(str(item))
    typer.echo(report.as_dict())

    if not report.unbacked:
        typer.echo("nothing to repair; every recorded hour is backed by its file")
        return
    if report.dry_run:
        typer.echo(
            f"DRY RUN: {len(report.unbacked)} hour(s) would have their record removed and "
            "be fetched again. Nothing was changed."
        )
        raise typer.Exit(code=INCOMPLETE_EXIT_CODE)

    if report.disagreed:
        # Printed separately and above the verdict, because it is the finding
        # rather than the failure: those hours are now correct, and what is worth
        # a human's attention is that the record had been wrong about them by a
        # specific amount.
        typer.echo(
            f"NOTE: {len(report.disagreed)} repaired hour(s) hold a different number of "
            "events than the removed record claimed. The counts above are what the "
            "publisher actually serves; the claims were untrue."
        )
    if not report.ok:
        typer.echo(
            f"INCOMPLETE: {len(report.unrecovered)} hour(s) were not recovered and "
            f"{len(report.unclearable)} could not be cleared. Re-run to continue; hours "
            "already repaired will not be fetched again."
        )
        raise typer.Exit(code=INCOMPLETE_EXIT_CODE)
    typer.echo(f"repaired {len(report.recovered)} hour(s); `reporadar verify` should now pass.")


@app.command(name="marts-status")
def marts_status() -> None:
    """Report whether the published marts still reflect every hour the lake holds."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = get_settings()

    async def _run() -> FreshnessReport:
        async with pg_connection(settings) as connection:
            # No create_schema here, unlike verify and backfill. This reads the
            # marts and the lake and never the ledger, and the one thing it does
            # ask the database — whether the mart table exists — answers with a
            # null rather than failing. A read-only command should not be issuing
            # DDL to make itself work.
            return await marts_freshness(connection, lake_dir=settings.lake_dir)

    report = asyncio.run(_run())
    for day in report.drift:
        marker = "STALE  " if day.stale else "surplus"
        typer.echo(f"{marker} {day.day}  {day.kind}: {day.detail}")
    typer.echo(report.as_dict())
    if not report.built:
        typer.echo("note: the marts have never been built.")
    if not report.ok:
        # A distinct code, so a caller can tell "the marts need rebuilding" from
        # "this check could not run". See STALE_EXIT_CODE.
        typer.echo(
            f"STALE: {len(report.stale_days)} day(s) behind by {report.hours_behind} "
            "ingested hour(s). Rebuild with `make marts`."
        )
        raise typer.Exit(code=STALE_EXIT_CODE)


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
    """Overlap between a live sample and an archived hour, matched on event id.

    Reports how many of the archive hour's events also appear in the sample.
    This is not a measure of how much of the feed was captured: the two sources
    were found not to share an event identifier, so the join comes back empty
    and that case is named rather than reported as a rate of zero.
    """
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
