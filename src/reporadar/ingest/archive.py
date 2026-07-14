"""GH Archive hourly downloads — idempotent by construction.

Files are written to ``<name>.part`` and atomically renamed on success, so a
crashed download can never masquerade as a complete hour; an existing final
file short-circuits the download entirely.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx

DEFAULT_BASE_URL = "https://data.gharchive.org"


def hour_filename(day: date, hour: int) -> str:
    """GH Archive naming: unpadded hour, e.g. ``2026-07-07-9.json.gz``."""
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be 0–23, got {hour}")
    return f"{day:%Y-%m-%d}-{hour}.json.gz"


def download_hour(
    day: date,
    hour: int,
    dest_dir: Path,
    base_url: str = DEFAULT_BASE_URL,
    client: httpx.Client | None = None,
) -> Path:
    """Fetch one archive hour into ``dest_dir``; returns the final path.

    Skips the network entirely when the file already exists.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / hour_filename(day, hour)
    if dest.exists():
        return dest

    part = dest.with_suffix(dest.suffix + ".part")
    own_client = client is None
    http = client or httpx.Client(timeout=60.0, follow_redirects=True)
    try:
        with http.stream("GET", f"{base_url}/{dest.name}") as resp:
            resp.raise_for_status()
            with part.open("wb") as fh:
                for chunk in resp.iter_bytes():
                    fh.write(chunk)
        part.rename(dest)
        return dest
    finally:
        part.unlink(missing_ok=True)
        if own_client:
            http.close()
