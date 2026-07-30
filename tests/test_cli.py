from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from reporadar import cli
from reporadar.analysis.capture import CaptureReport
from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.ledger import HourStatus
from reporadar.ingest.metrics import ArchiveCounters, ConsumeCounters, PollCounters
from reporadar.ingest.sinks import EventSink
from reporadar.ingest.verify import Finding, Problem, VerifyReport

runner = CliRunner()


@pytest.fixture()
def pinned_cli_settings(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> Settings:
    """The CLI reads config through get_settings(); pin it so a developer's real
    environment (a live GITHUB_TOKEN, a populated data dir) can never leak into a
    command test."""
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    return settings


def test_fetch_archive_parses_day_hour_and_reports_path(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The command's own work is argument translation: a YYYY-MM-DD string to a
    # date, and settings into the download call. The download itself is mocked —
    # its behavior is proven in test_archive.py, not re-proven here.
    calls: dict[str, object] = {}

    def fake_download(day: date, hour: int, dest_dir: Path, *, base_url: str) -> Path:
        calls.update(day=day, hour=hour, dest_dir=dest_dir, base_url=base_url)
        return dest_dir / "2026-07-07-15.json.gz"

    monkeypatch.setattr(cli, "download_hour", fake_download)

    result = runner.invoke(cli.app, ["fetch-archive", "2026-07-07", "15"])

    assert result.exit_code == 0
    assert calls["day"] == date(2026, 7, 7)  # parsed, not passed through as a string
    assert calls["hour"] == 15
    assert calls["dest_dir"] == pinned_cli_settings.archive_dir
    assert calls["base_url"] == pinned_cli_settings.archive_base
    assert "2026-07-07-15.json.gz" in result.output


def test_explore_prints_histogram_and_total(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_type_counts(archive_path: Path) -> list[tuple[str, int]]:
        seen["path"] = archive_path
        return [("PushEvent", 3), ("WatchEvent", 2)]

    monkeypatch.setattr(cli, "type_counts", fake_type_counts)

    result = runner.invoke(cli.app, ["explore", "some/archive.json.gz"])

    assert result.exit_code == 0
    assert seen["path"] == Path("some/archive.json.gz")  # str arg coerced to Path
    assert "PushEvent" in result.output
    assert "WatchEvent" in result.output
    assert "TOTAL" in result.output
    assert "5" in result.output  # 3 + 2, summed by the command itself


def test_capture_rate_formats_report(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Path] = {}

    def fake_capture_rate(archive_path: Path, live_path: Path) -> CaptureReport:
        seen["archive"] = archive_path
        seen["live"] = live_path
        return CaptureReport(archive_events=1000, live_events=250, matched=200)

    monkeypatch.setattr(cli, "capture_rate", fake_capture_rate)

    result = runner.invoke(cli.app, ["capture-rate", "a.json.gz", "live.ndjson"])

    assert result.exit_code == 0
    assert seen["archive"] == Path("a.json.gz")
    assert seen["live"] == Path("live.ndjson")
    assert "1,000" in result.output  # thousands separator applied by the command
    assert "20.0%" in result.output  # 200 / 1000, formatted as a percentage


def test_capture_rate_refuses_and_exits_nonzero_when_not_reconcilable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A scheduled reconciliation must not record "0.0%" as a successful run when
    # the two files share no event identity. The counts still print — they are
    # the diagnosis — but the rate line names the cause and the exit code makes
    # the failure legible to whatever runs the command.
    def fake_capture_rate(archive_path: Path, live_path: Path) -> CaptureReport:
        return CaptureReport(archive_events=157_856, live_events=100, matched=0)

    monkeypatch.setattr(cli, "capture_rate", fake_capture_rate)

    result = runner.invoke(cli.app, ["capture-rate", "a.json.gz", "live.ndjson"])

    assert result.exit_code == 1
    assert "NOT RECONCILABLE" in result.output
    assert "157,856" in result.output  # the counts are still reported
    assert "0.0%" not in result.output  # the number that must never be printed


def test_poll_forwards_options_and_reports_output(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings, tmp_path: Path
) -> None:
    out_path = tmp_path / "events_20260707T150000Z.ndjson"
    calls: dict[str, object] = {}

    async def fake_collect_sample(
        settings: Settings, *, cycles: int, interval_s: float, pages: int, seen_window: int
    ) -> Path:
        calls.update(
            settings=settings,
            cycles=cycles,
            interval_s=interval_s,
            pages=pages,
            seen_window=seen_window,
        )
        return out_path

    monkeypatch.setattr(cli, "collect_sample", fake_collect_sample)

    result = runner.invoke(cli.app, ["poll", "--cycles", "2", "--interval-s", "0", "--pages", "1"])

    assert result.exit_code == 0
    assert calls["settings"] is pinned_cli_settings  # config threaded through, not reconstructed
    assert calls["cycles"] == 2
    assert calls["interval_s"] == 0.0
    assert calls["pages"] == 1
    # the configured window, not the library default — the command resolves it
    assert calls["seen_window"] == pinned_cli_settings.seen_window == 1_000
    assert str(out_path) in result.output


def test_serve_tees_capture_to_the_files_and_the_stream(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings, event_dict: dict[str, Any]
) -> None:
    # serve's own work is composition: check the live topic exists, open the Kafka
    # producer, and feed the loop a tee that writes each batch to the hourly files
    # (the reconciliation record) AND publishes it to the stream. The loop, the
    # sinks and the tee's failure policy are proven in their own tests; what belongs
    # here is that the two sinks are actually wired into one tee — proven by driving
    # a batch through the sink the loop receives and seeing both sinks record it.
    sample = RawEvent.model_validate(event_dict)
    calls: dict[str, object] = {}
    order: list[str] = []

    class Recorder:
        def __init__(self) -> None:
            self.batches: list[Sequence[RawEvent]] = []

        async def __call__(self, events: Sequence[RawEvent]) -> None:
            self.batches.append(events)

    class FakeNdjsonSink(Recorder):
        def __init__(self, base_dir: Path) -> None:
            super().__init__()
            calls["sink_dir"] = base_dir

    stream = Recorder()
    ndjson_sinks: list[FakeNdjsonSink] = []

    def make_ndjson(base_dir: Path) -> FakeNdjsonSink:
        sink = FakeNdjsonSink(base_dir)
        ndjson_sinks.append(sink)
        return sink

    @asynccontextmanager
    async def fake_kafka_sink(settings: Settings) -> AsyncIterator[Recorder]:
        order.append("open stream")
        try:
            yield stream
        finally:
            order.append("close stream")

    async def fake_require_topics(settings: Settings, topics: Sequence[str]) -> None:
        order.append("verify")
        calls["required_topics"] = list(topics)

    async def fake_poll_stream(
        settings: Settings,
        sink: EventSink,
        *,
        interval_s: float,
        pages: int,
        seen_window: int,
        max_cycles: int | None,
        stop: asyncio.Event,
    ) -> PollCounters:
        order.append("poll")
        calls.update(max_cycles=max_cycles, seen_window=seen_window, pages=pages)
        await sink([sample])  # drive one batch through the wired sink
        counters = PollCounters()
        counters.record_cycle(fetched=5, fresh=3)
        return counters

    monkeypatch.setattr(cli, "HourlyNdjsonSink", make_ndjson)
    monkeypatch.setattr(cli, "kafka_sink", fake_kafka_sink)
    monkeypatch.setattr(cli, "require_topics", fake_require_topics)
    monkeypatch.setattr(cli, "poll_stream", fake_poll_stream)

    result = runner.invoke(cli.app, ["serve", "--cycles", "2", "--interval-s", "0", "--pages", "1"])

    assert result.exit_code == 0
    assert calls["required_topics"] == [pinned_cli_settings.kafka_live_topic]  # live topic only
    assert calls["sink_dir"] == pinned_cli_settings.live_dir
    assert calls["max_cycles"] == 2  # --cycles arrives as the loop's bound
    assert calls["seen_window"] == pinned_cli_settings.seen_window == 1_000
    # the one batch the loop pushed reached BOTH the files and the stream — the tee is real
    assert ndjson_sinks[0].batches == [[sample]]
    assert stream.batches == [[sample]]
    # verify before the producer opens; the producer closes after the loop returns
    assert order == ["verify", "open stream", "poll", "close stream"]
    assert "stopped:" in result.output
    assert "'fresh': 3" in result.output  # the final counters surface to the operator
    assert "stream_drops=0" in result.output  # drops are observable at shutdown


def test_consume_wires_source_store_and_dead_letter_sink(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # consume's own work is composition: open the three collaborators from settings,
    # hand them to the loop with a live stop event, and surface the final counters.
    # The loop is proven in test_consumer.py and each adapter in its own module's
    # tests; what belongs here is the wiring — including the open/close order, which
    # is a deliberate choice and would otherwise be a comment nothing enforces.
    calls: dict[str, object] = {}
    order: list[str] = []

    def fake_resource(
        name: str, resource: object
    ) -> Callable[[Settings], AbstractAsyncContextManager[object]]:
        @asynccontextmanager
        async def factory(settings: Settings) -> AsyncIterator[object]:
            calls[f"{name}_settings"] = settings
            order.append(f"open {name}")
            try:
                yield resource
            finally:
                order.append(f"close {name}")

        return factory

    async def fake_require_topics(settings: Settings, topics: Sequence[str]) -> None:
        order.append("verify")
        calls["required_topics"] = list(topics)

    monkeypatch.setattr(cli, "require_topics", fake_require_topics)

    source, store, dead_letter = object(), object(), object()
    monkeypatch.setattr(cli, "pg_store", fake_resource("store", store))
    monkeypatch.setattr(cli, "kafka_dead_letter_sink", fake_resource("dlq", dead_letter))
    monkeypatch.setattr(cli, "kafka_source", fake_resource("source", source))

    async def fake_consume_stream(
        source_arg: object,
        store_arg: object,
        dead_letter_arg: object,
        *,
        seen_window: int,
        report_every: int,
        stop: asyncio.Event,
    ) -> ConsumeCounters:
        calls.update(
            source=source_arg,
            store=store_arg,
            dead_letter=dead_letter_arg,
            seen_window=seen_window,
            report_every=report_every,
            stop_set=stop.is_set(),
        )
        order.append("consume")
        counters = ConsumeCounters()
        counters.record_batch(consumed=5, stored=3, dead_lettered=1)
        return counters

    monkeypatch.setattr(cli, "consume_stream", fake_consume_stream)

    result = runner.invoke(cli.app, ["consume", "--seen-window", "10", "--report-every", "0"])

    assert result.exit_code == 0
    # every adapter is built from the one pinned Settings, and the objects they
    # yield are the ones the loop actually receives
    assert calls["store_settings"] is pinned_cli_settings
    assert calls["dlq_settings"] is pinned_cli_settings
    assert calls["source_settings"] is pinned_cli_settings
    assert calls["source"] is source
    assert calls["store"] is store
    assert calls["dead_letter"] is dead_letter
    assert calls["seen_window"] == 10  # --seen-window sizes the dedup window
    assert calls["report_every"] == 0  # 0 disables progress logging
    assert calls["stop_set"] is False  # a real, un-fired stop event was threaded through
    # the consumer reads the live topic and writes the dead-letter one, so it checks both
    assert calls["required_topics"] == [
        pinned_cli_settings.kafka_live_topic,
        pinned_cli_settings.kafka_dlq_topic,
    ]
    # the store opens first (its config check fails before the group is joined) and
    # the source closes first (reading stops before what it feeds is torn down)
    assert order == [
        # the topics are checked before anything opens: a missing topic must not
        # cost a consumer-group join and a request-timeout stall to discover
        "verify",
        "open store",
        "open dlq",
        "open source",
        "consume",
        "close source",
        "close dlq",
        "close store",
    ]
    assert "stopped:" in result.output
    assert "'stored': 3" in result.output  # the final counters surface to the operator
    assert "'dead_lettered': 1" in result.output


def test_the_configured_seen_window_applies_when_the_flag_is_absent(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The flag overrides the setting, and the test above passes --seen-window, so
    # without this one nothing reaches the setting at all: the loop would still be
    # handed the library default and every assertion would stay green. The pinned
    # fixture value is deliberately not 50_000, so this can only pass by the
    # configured window actually arriving.
    calls: dict[str, object] = {}

    def fake_resource(
        resource: object,
    ) -> Callable[[Settings], AbstractAsyncContextManager[object]]:
        @asynccontextmanager
        async def factory(settings: Settings) -> AsyncIterator[object]:
            yield resource

        return factory

    async def fake_require_topics(settings: Settings, topics: Sequence[str]) -> None:
        return None

    monkeypatch.setattr(cli, "require_topics", fake_require_topics)
    monkeypatch.setattr(cli, "pg_store", fake_resource(object()))
    monkeypatch.setattr(cli, "kafka_dead_letter_sink", fake_resource(object()))
    monkeypatch.setattr(cli, "kafka_source", fake_resource(object()))

    async def fake_consume_stream(
        source_arg: object,
        store_arg: object,
        dead_letter_arg: object,
        *,
        seen_window: int,
        report_every: int,
        stop: asyncio.Event,
    ) -> ConsumeCounters:
        calls["seen_window"] = seen_window
        return ConsumeCounters()

    monkeypatch.setattr(cli, "consume_stream", fake_consume_stream)

    result = runner.invoke(cli.app, ["consume", "--report-every", "0"])

    assert result.exit_code == 0
    assert calls["seen_window"] == pinned_cli_settings.seen_window == 1_000


#: A publisher that is *not* archive.DEFAULT_BASE_URL. The shared settings fixture
#: pins archive_base to the shipped default, so "did this come from settings?" is
#: unanswerable against it — a command that hard-coded the library constant would
#: pass. Overriding it here is what makes the assertion able to fail.
MIRROR_BASE = "https://archive-mirror.invalid"


def _archive_settings(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> Settings:
    """The pinned settings with a distinctive publisher, re-pinned onto the CLI."""
    distinct = settings.model_copy(update={"archive_base": MIRROR_BASE})
    monkeypatch.setattr(cli, "get_settings", lambda: distinct)
    return distinct


def _recording_connection(
    order: list[str], connection: object
) -> Callable[[Settings], AbstractAsyncContextManager[object]]:
    @asynccontextmanager
    async def factory(settings: Settings) -> AsyncIterator[object]:
        order.append("open connection")
        try:
            yield connection
        finally:
            order.append("close connection")

    return factory


def test_archive_serve_wires_the_connection_and_the_convergence_loop(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The loop itself is proven in test_converge.py; what belongs here is that the
    # command hands it the right things — every path and bound coming from settings
    # or the flags rather than from a library default that happens to match.
    calls: dict[str, object] = {}
    order: list[str] = []
    connection = object()
    archive_settings = _archive_settings(monkeypatch, pinned_cli_settings)
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, connection))

    @contextmanager
    def recording_stop_on_signals(*sigs: object) -> Iterator[asyncio.Event]:
        order.append("install handlers")
        try:
            yield asyncio.Event()
        finally:
            order.append("remove handlers")

    monkeypatch.setattr(cli, "stop_on_signals", recording_stop_on_signals)

    async def fake_converge_forever(
        connection_arg: object,
        *,
        archive_dir: Path,
        lake_dir: Path,
        concurrency: int,
        lookback_days: int,
        interval_s: float,
        base_url: str,
        max_passes: int | None,
        stop: asyncio.Event,
    ) -> ArchiveCounters:
        calls.update(
            connection=connection_arg,
            archive_dir=archive_dir,
            lake_dir=lake_dir,
            concurrency=concurrency,
            lookback_days=lookback_days,
            interval_s=interval_s,
            base_url=base_url,
            max_passes=max_passes,
            stop_set=stop.is_set(),
        )
        order.append("converge")
        counters = ArchiveCounters()
        counters.record_pass(due=2)
        counters.record_hour(status=HourStatus.INGESTED, events=157_856)
        return counters

    monkeypatch.setattr(cli, "converge_forever", fake_converge_forever)

    result = runner.invoke(
        cli.app,
        [
            "archive-serve",
            "--interval-s",
            "0",
            "--concurrency",
            "2",
            "--lookback-days",
            "5",
            "--passes",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert calls["connection"] is connection  # the opened connection, not a fresh one
    assert calls["archive_dir"] == archive_settings.archive_dir
    assert calls["lake_dir"] == archive_settings.lake_dir
    # The *configured* publisher, and MIRROR_BASE is deliberately not the library
    # default — so a command that passed archive.DEFAULT_BASE_URL instead of reading
    # settings fails here rather than matching by coincidence.
    assert calls["base_url"] == MIRROR_BASE
    assert calls["concurrency"] == 2
    assert calls["lookback_days"] == 5
    assert calls["interval_s"] == 0
    assert calls["max_passes"] == 1  # --passes bounds a run; absent it runs forever
    assert calls["stop_set"] is False  # a real, un-fired stop event was threaded through
    # Handlers installed before the connection opens and removed after it closes: a
    # SIGTERM arriving during a slow connect should end the run rather than be missed
    # because the handler was not in place yet. Asserted, because the alternative
    # nesting produces an identical result and differs only in this ordering.
    assert order == [
        "install handlers",
        "open connection",
        "converge",
        "close connection",
        "remove handlers",
    ]
    assert "stopped:" in result.output
    assert "'ingested': 1" in result.output  # final counters reach the operator
    assert "'events': 157856" in result.output


def test_backfill_ensures_the_schema_then_converges_the_given_range_once(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    calls: dict[str, object] = {}
    order: list[str] = []
    installed_handlers: list[str] = []
    connection = object()
    _archive_settings(monkeypatch, pinned_cli_settings)
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, connection))

    @contextmanager
    def recording_stop_on_signals(*sigs: object) -> Iterator[asyncio.Event]:
        installed_handlers.append("installed")
        yield asyncio.Event()

    monkeypatch.setattr(cli, "stop_on_signals", recording_stop_on_signals)

    async def fake_create_schema(connection_arg: object) -> None:
        calls["schema_connection"] = connection_arg
        order.append("create schema")

    monkeypatch.setattr(cli, "create_schema", fake_create_schema)

    async def fake_converge_once(
        connection_arg: object,
        *,
        archive_dir: Path,
        lake_dir: Path,
        now: datetime,
        first_day: date,
        last_day: date,
        concurrency: int,
        retry_failed: bool,
        base_url: str,
    ) -> ArchiveCounters:
        calls.update(
            connection=connection_arg,
            now=now,
            first_day=first_day,
            last_day=last_day,
            concurrency=concurrency,
            retry_failed=retry_failed,
            base_url=base_url,
        )
        order.append("converge")
        counters = ArchiveCounters()
        counters.record_pass(due=48)
        counters.record_hour(status=HourStatus.MISSING, events=None)
        return counters

    monkeypatch.setattr(cli, "converge_once", fake_converge_once)

    result = runner.invoke(cli.app, ["backfill", "2026-07-21", "2026-07-22"])

    assert result.exit_code == 0
    assert calls["first_day"] == date(2026, 7, 21)  # inclusive both ends
    assert calls["last_day"] == date(2026, 7, 22)
    assert calls["base_url"] == MIRROR_BASE  # from settings, not the library constant
    # True by default, and the whole reason an explicit range exists: the always-on
    # loop skips failed hours so it cannot spin, so nothing else ever retries them.
    assert calls["retry_failed"] is True
    # ingest_hour rejects a naive clock outright, so a tz-less now here would fail
    # only once a real hour was attempted — long after this command returned 0.
    now = calls["now"]
    assert isinstance(now, datetime) and now.tzinfo is not None
    # The schema is created before the scan and on the same connection.
    # converge_forever does this itself; converge_once does not, and a backfill is
    # commonly the first thing ever pointed at a database — the scan would fail on
    # a missing archive_hours table.
    assert order == ["open connection", "create schema", "converge", "close connection"]
    assert calls["schema_connection"] is connection
    # No signal handlers, deliberately: converge_once has no stop event to give one
    # to, so installing them would swallow Ctrl-C into an event nothing reads and
    # leave a year-long range killable only by SIGKILL. This pins that decision,
    # which a comment alone would leave as a suggestion.
    assert installed_handlers == []
    assert "done:" in result.output
    assert "'missing': 1" in result.output


def test_backfill_refuses_a_range_whose_days_are_transposed(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The ledger scan builds its calendar with generate_series, which returns no
    # rows for a descending range. So the natural failure mode is not an error but
    # "nothing outstanding" and exit 0 — a typo that reads as a completed backfill,
    # leaving every hour it skipped looking settled to later readers. Opening
    # nothing is part of the claim: the refusal has to land before any connection.
    order: list[str] = []
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, object()))

    async def unreachable_converge_once(*args: object, **kwargs: object) -> ArchiveCounters:
        raise AssertionError("a transposed range must never reach the ledger scan")

    monkeypatch.setattr(cli, "converge_once", unreachable_converge_once)

    result = runner.invoke(cli.app, ["backfill", "2026-07-22", "2026-07-21"])

    assert result.exit_code != 0
    assert order == []  # refused on its arguments, before a socket was opened
    assert "FIRST_DAY 2026-07-22 is after LAST_DAY 2026-07-21" in result.output


def test_verify_reports_a_clean_lake_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    calls: dict[str, object] = {}
    order: list[str] = []
    connection = object()
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, connection))

    async def fake_create_schema(connection_arg: object) -> None:
        order.append("create schema")

    monkeypatch.setattr(cli, "create_schema", fake_create_schema)

    async def fake_verify_lake(
        connection_arg: object, *, lake_dir: Path, check_counts: bool
    ) -> VerifyReport:
        calls.update(connection=connection_arg, lake_dir=lake_dir, check_counts=check_counts)
        order.append("verify")
        return VerifyReport(claimed=3, agreed=3)

    monkeypatch.setattr(cli, "verify_lake", fake_verify_lake)

    result = runner.invoke(cli.app, ["verify"])

    assert result.exit_code == 0
    assert calls["connection"] is connection
    assert calls["lake_dir"] == pinned_cli_settings.lake_dir
    # Reading the lake is opt-in: the default must not scan every partition, and
    # without this assertion a flipped default would still pass every other test.
    assert calls["check_counts"] is False
    # The schema is ensured before the read, so verifying a database nothing has
    # ingested into reports "nothing claimed" rather than failing on a missing table.
    assert order == ["open connection", "create schema", "verify", "close connection"]
    assert "'claimed': 3" in result.output


def test_verify_exits_nonzero_when_a_recorded_hour_is_not_backed(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The deploy-gate contract, and the only reason the command is worth running
    # from a script: a claim with no file must make the process fail, or a
    # scheduled check reports success over a lake with permanent holes.
    order: list[str] = []
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, object()))

    async def fake_create_schema(connection_arg: object) -> None:
        return None

    monkeypatch.setattr(cli, "create_schema", fake_create_schema)

    async def fake_verify_lake(
        connection_arg: object, *, lake_dir: Path, check_counts: bool
    ) -> VerifyReport:
        report = VerifyReport(claimed=2, agreed=1)
        report.findings.append(
            Finding(date(2026, 7, 22), 22, Problem.ABSENT, "no file at lake/dt=2026-07-22/hr=22")
        )
        return report

    monkeypatch.setattr(cli, "verify_lake", fake_verify_lake)

    result = runner.invoke(cli.app, ["verify", "--counts"])

    assert result.exit_code == 1
    assert "UNBACKED" in result.output
    assert "absent" in result.output
    assert "1 of 2 recorded hour(s)" in result.output


def test_verify_reports_a_surplus_file_without_failing(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # The asymmetry, at the command's edge: a file nothing claims is printed so an
    # operator sees the drift, but it misreports nothing, so the exit code stays 0.
    # Without this, "report everything, fail on some of it" would be a claim made
    # only by a comment.
    order: list[str] = []
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, object()))

    async def fake_create_schema(connection_arg: object) -> None:
        return None

    monkeypatch.setattr(cli, "create_schema", fake_create_schema)

    async def fake_verify_lake(
        connection_arg: object, *, lake_dir: Path, check_counts: bool
    ) -> VerifyReport:
        report = VerifyReport(claimed=1, agreed=1)
        report.findings.append(
            Finding(date(2026, 7, 22), 21, Problem.UNRECORDED, "file with no ingested row")
        )
        return report

    monkeypatch.setattr(cli, "verify_lake", fake_verify_lake)

    result = runner.invoke(cli.app, ["verify"])

    assert result.exit_code == 0
    assert "surplus" in result.output
    assert "unrecorded" in result.output
    assert "UNBACKED" not in result.output


def test_verify_says_when_it_could_only_check_presence(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # A row with no recorded size gets a weaker check than the others. Counting it
    # among the agreed without saying so would overstate what ran.
    order: list[str] = []
    monkeypatch.setattr(cli, "pg_connection", _recording_connection(order, object()))

    async def fake_create_schema(connection_arg: object) -> None:
        return None

    monkeypatch.setattr(cli, "create_schema", fake_create_schema)

    async def fake_verify_lake(
        connection_arg: object, *, lake_dir: Path, check_counts: bool
    ) -> VerifyReport:
        return VerifyReport(claimed=2, agreed=2, unsized=1)

    monkeypatch.setattr(cli, "verify_lake", fake_verify_lake)

    result = runner.invoke(cli.app, ["verify"])

    assert result.exit_code == 0
    assert "presence only" in result.output
    assert "1 hour(s) carry no recorded size" in result.output
