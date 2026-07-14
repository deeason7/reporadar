from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

from reporadar.ingest.archive import download_hour, hour_filename

HOUR_URL = "https://data.gharchive.org/2026-07-07-9.json.gz"


def test_hour_filename_is_unpadded() -> None:
    assert hour_filename(date(2026, 7, 7), 9) == "2026-07-07-9.json.gz"
    assert hour_filename(date(2026, 7, 7), 15) == "2026-07-07-15.json.gz"


def test_hour_filename_rejects_invalid_hour() -> None:
    with pytest.raises(ValueError):
        hour_filename(date(2026, 7, 7), 24)


@respx.mock
def test_download_writes_file_and_skips_when_present(tmp_path: Path) -> None:
    route = respx.get(HOUR_URL).mock(return_value=httpx.Response(200, content=b"gzip-bytes"))

    first = download_hour(date(2026, 7, 7), 9, tmp_path)
    second = download_hour(date(2026, 7, 7), 9, tmp_path)  # must not re-download

    assert first == second == tmp_path / "2026-07-07-9.json.gz"
    assert first.read_bytes() == b"gzip-bytes"
    assert route.call_count == 1
    assert not list(tmp_path.glob("*.part"))  # no partials left behind


@respx.mock
def test_failed_download_leaves_no_files(tmp_path: Path) -> None:
    respx.get(HOUR_URL).mock(return_value=httpx.Response(500))

    with pytest.raises(httpx.HTTPStatusError):
        download_hour(date(2026, 7, 7), 9, tmp_path)

    assert list(tmp_path.iterdir()) == []  # neither final file nor .part survives
