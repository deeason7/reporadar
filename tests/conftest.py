from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import PostgresDsn

from reporadar.config import Settings


@pytest.fixture()
def event_dict() -> dict[str, Any]:
    """One /events item shaped like the real API (extra keys included on purpose —
    the envelope must tolerate fields it doesn't model)."""
    return {
        "id": "45000000001",
        "type": "PushEvent",
        "actor": {
            "id": 1,
            "login": "octo-tester",
            "gravatar_id": "",
            "url": "https://api.github.com/users/octo-tester",
        },
        "repo": {
            "id": 2,
            "name": "octo/widgets",
            "url": "https://api.github.com/repos/octo/widgets",
        },
        "payload": {"push_id": 99, "size": 1},
        "public": True,
        "created_at": "2026-07-07T15:00:00Z",
    }


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Fully pinned settings — init args outrank env/.env, so every field is
    passed explicitly and no developer environment can leak into a test."""
    pinned: dict[str, Any] = {
        "github_token": "test-token",
        "user_agent": "reporadar (test-suite)",
        "api_base": "https://api.github.com",
        "archive_base": "https://data.gharchive.org",
        "kafka_bootstrap_servers": "kafka.invalid:9092",  # .invalid never resolves — fails fast
        "kafka_live_topic": "raw.events.test",
        "kafka_dlq_topic": "raw.events.dlq.test",
        "kafka_topic_partitions": 2,
        "kafka_topic_replication_factor": 1,
        "postgres_dsn": PostgresDsn("postgresql://reporadar:test@db.invalid:5432/reporadar"),
        "data_dir": tmp_path / "data",
        # Deliberately not the shipped default: a test that only passes because
        # the fixture happens to match production is not testing the wiring.
        "seen_window": 1_000,
    }
    # The docstring above is a claim; this is what keeps it true. A field added to
    # Settings without a line here would fall back to the environment and source
    # itself from whoever is running the suite — on their machine only, and
    # without failing. "Every field" is the whole basis of the guarantee, so it
    # is checked rather than trusted.
    assert pinned.keys() == Settings.model_fields.keys()
    return Settings(**pinned)
