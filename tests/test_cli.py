from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from reporadar import cli
from reporadar.analysis.capture import CaptureReport
from reporadar.config import Settings
from reporadar.ingest.metrics import PollCounters

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


def test_poll_forwards_options_and_reports_output(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings, tmp_path: Path
) -> None:
    out_path = tmp_path / "events_20260707T150000Z.ndjson"
    calls: dict[str, object] = {}

    async def fake_collect_sample(
        settings: Settings, *, cycles: int, interval_s: float, pages: int
    ) -> Path:
        calls.update(settings=settings, cycles=cycles, interval_s=interval_s, pages=pages)
        return out_path

    monkeypatch.setattr(cli, "collect_sample", fake_collect_sample)

    result = runner.invoke(cli.app, ["poll", "--cycles", "2", "--interval-s", "0", "--pages", "1"])

    assert result.exit_code == 0
    assert calls["settings"] is pinned_cli_settings  # config threaded through, not reconstructed
    assert calls["cycles"] == 2
    assert calls["interval_s"] == 0.0
    assert calls["pages"] == 1
    assert str(out_path) in result.output


def test_serve_wires_stream_sink_and_signals(
    monkeypatch: pytest.MonkeyPatch, pinned_cli_settings: Settings
) -> None:
    # serve's own work is composition: build the sink where settings point, translate
    # --cycles into the loop's max_cycles bound, and run the stream under a live stop
    # event. The loop is proven in test_service.py, the sink in test_sinks.py, and the
    # signal handling in test_signals.py; stop_on_signals runs for real here (its
    # handlers are removed on exit, so nothing leaks out of the test).
    calls: dict[str, object] = {}

    class FakeSink:
        def __init__(self, base_dir: Path) -> None:
            calls["sink_dir"] = base_dir

    async def fake_poll_stream(
        settings: Settings,
        sink: object,
        *,
        interval_s: float,
        pages: int,
        max_cycles: int | None,
        stop: asyncio.Event,
    ) -> PollCounters:
        calls.update(
            settings=settings,
            sink=sink,
            interval_s=interval_s,
            pages=pages,
            max_cycles=max_cycles,
            stop_set=stop.is_set(),
        )
        counters = PollCounters()
        counters.record_cycle(fetched=5, fresh=3)
        return counters

    monkeypatch.setattr(cli, "HourlyNdjsonSink", FakeSink)
    monkeypatch.setattr(cli, "poll_stream", fake_poll_stream)

    result = runner.invoke(cli.app, ["serve", "--cycles", "2", "--interval-s", "0", "--pages", "1"])

    assert result.exit_code == 0
    assert calls["sink_dir"] == pinned_cli_settings.live_dir
    assert calls["settings"] is pinned_cli_settings
    assert isinstance(calls["sink"], FakeSink)  # the sink built from settings is the one serving
    assert calls["max_cycles"] == 2  # --cycles arrives as the loop's bound
    assert calls["interval_s"] == 0.0
    assert calls["pages"] == 1
    assert calls["stop_set"] is False  # a real, un-fired stop event was threaded through
    assert "stopped:" in result.output
    assert "'fresh': 3" in result.output  # the final counters surface to the operator
