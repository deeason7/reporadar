from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from reporadar.analysis.capture import capture_rate, type_counts


def _event(event_id: str, event_type: str = "PushEvent") -> dict[str, Any]:
    return {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "octo-tester"},
        "repo": {"id": 2, "name": "octo/widgets"},
        "payload": {},
        "public": True,
        "created_at": "2026-07-07T15:00:00Z",
    }


def _write_archive_gz(path: Path, events: list[dict[str, Any]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def _write_ndjson(path: Path, events: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")


def test_type_counts_orders_by_frequency(tmp_path: Path) -> None:
    archive = tmp_path / "2026-07-07-15.json.gz"
    _write_archive_gz(
        archive,
        [
            _event("1"),
            _event("2"),
            _event("3"),
            _event("4", "WatchEvent"),
            _event("5", "WatchEvent"),
            _event("6", "ForkEvent"),
        ],
    )
    assert type_counts(archive) == [("PushEvent", 3), ("WatchEvent", 2), ("ForkEvent", 1)]


def test_capture_rate_counts_id_overlap(tmp_path: Path) -> None:
    archive = tmp_path / "2026-07-07-15.json.gz"
    live = tmp_path / "live.ndjson"
    _write_archive_gz(archive, [_event(str(i)) for i in range(1, 6)])  # ids 1..5
    _write_ndjson(live, [_event("2"), _event("3"), _event("999")])  # 2 overlap + 1 outside

    report = capture_rate(archive, live)

    assert report.archive_events == 5
    assert report.live_events == 3
    assert report.matched == 2
    assert report.capture_rate == 0.4


def test_capture_rate_of_empty_archive_is_zero(tmp_path: Path) -> None:
    archive = tmp_path / "2026-07-07-15.json.gz"
    live = tmp_path / "live.ndjson"
    # Headers-only files: duckdb needs at least a schema, so give each one row
    # and make them disjoint — the zero case we guard is archive_events == 0
    # via the dataclass property, exercised directly here.
    from reporadar.analysis.capture import CaptureReport

    assert CaptureReport(archive_events=0, live_events=0, matched=0).capture_rate == 0.0
    _write_archive_gz(archive, [_event("1")])
    _write_ndjson(live, [_event("2")])
    assert capture_rate(archive, live).matched == 0
