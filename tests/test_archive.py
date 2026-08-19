from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

from reporadar.ingest.archive import download_hour, hour_filename

HOUR_URL = "https://data.gharchive.org/2026-07-07-9.json.gz"
NEXT_HOUR_URL = "https://data.gharchive.org/2026-07-08-9.json.gz"


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


@respx.mock
def test_download_hour_closes_the_client_it_created(tmp_path: Path) -> None:
    # download_hour makes its own client when none is passed, and a client that is
    # never closed leaks a connection pool per call. Nothing else asserted this,
    # so the ownership flag could be inverted without a single test noticing.
    respx.get(HOUR_URL).mock(return_value=httpx.Response(200, content=b"gzip-bytes"))
    made: list[httpx.Client] = []
    real_client = httpx.Client

    def recording_client(*args: object, **kwargs: object) -> httpx.Client:
        client = real_client(*args, **kwargs)  # type: ignore[arg-type]
        made.append(client)
        return client

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(httpx, "Client", recording_client)
        download_hour(date(2026, 7, 7), 9, tmp_path)

    assert len(made) == 1
    assert made[0].is_closed


@respx.mock
def test_download_hour_leaves_a_caller_supplied_client_open(tmp_path: Path) -> None:
    # The opposite half, and the one the convergence loop depends on: hour.py hands
    # the same client to every hour in a range, so closing it after the first would
    # break every download that follows. Borrowed is not owned.
    respx.get(HOUR_URL).mock(return_value=httpx.Response(200, content=b"gzip-bytes"))

    respx.get(NEXT_HOUR_URL).mock(return_value=httpx.Response(200, content=b"more-bytes"))

    with httpx.Client() as client:
        download_hour(date(2026, 7, 7), 9, tmp_path, client=client)
        assert not client.is_closed  # still usable for the next hour in the range

        # And it really is still usable, not merely un-flagged.
        second = download_hour(date(2026, 7, 8), 9, tmp_path, client=client)
        assert second.exists()
