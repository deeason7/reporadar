from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from reporadar import cli
from reporadar.analysis.capture import CaptureReport
from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest.metrics import ConsumeCounters, PollCounters
from reporadar.ingest.sinks import EventSink

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
