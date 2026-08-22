from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import PostgresDsn

from reporadar.config import Settings
from reporadar.github.events import RawEvent, iter_ndjson


def read_ndjson(path: Path) -> list[RawEvent]:
    """Read a whole NDJSON file back through the system's own parser.

    Split on ``"\\n"`` and nothing else. ``str.splitlines()`` is the obvious
    spelling and the wrong one: it also breaks on U+2028 LINE SEPARATOR, which is
    legal *unescaped* inside a JSON string and is written out raw by
    ``model_dump_json()``. One event carrying it — a commit message, an issue
    title — comes back as two fragments, neither of which parses, so the event is
    not merely lost: it arrives in the dead-letter path as two failures, and the
    count of what went wrong is wrong too. Every reader here goes through this
    function so the decision lives in one place; ``test_ndjson_lines.py`` holds it.
    """
    return list(iter_ndjson(path.read_text(encoding="utf-8").split("\n")))


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


#: The pinned bases, exported so a test can build a mocked URL from the same value
#: the fixture hands the code under test. A route registered at the *shipped* URL
#: matches whether or not the caller read the setting, which is the difference
#: between testing the wiring and testing that two constants are equal.
TEST_API_BASE = "https://api.github.invalid"
TEST_ARCHIVE_BASE = "https://archive.gharchive.invalid"

#: Fields allowed to carry the value that ships, with the reason each is exempt.
#: Every other field must differ — see the second assertion in `settings`. Adding a
#: name here is a deliberate act that costs a sentence, which is the point.
MAY_MATCH_SHIPPED_DEFAULT: dict[str, str] = {
    # `ensure_topics` refuses a replication factor above the registered broker
    # count, and the admin doubles register one broker, so any value but 1 makes
    # every test sharing this fixture fail inside that guard instead of on its own
    # subject. The flow from setting to guard is proven separately, with a value
    # production would not produce: see
    # test_topics.py::test_the_replication_factor_the_guard_refuses_came_from_settings.
    "kafka_topic_replication_factor": "the admin doubles register a single broker",
}


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Fully pinned settings — init args outrank env/.env, so every field is
    passed explicitly and no developer environment can leak into a test.

    Every value also differs from the one that ships, so a test asserting "this
    came from settings" can fail. Both properties are enforced below rather than
    trusted, because both are invisible when broken.
    """
    pinned: dict[str, Any] = {
        "github_token": "test-token",
        "user_agent": "reporadar (test-suite)",
        # Pinned to values production would never produce, for the reason given in
        # the second assertion below.
        "api_base": TEST_API_BASE,
        "archive_base": TEST_ARCHIVE_BASE,
        "kafka_topic_replication_factor": 1,  # exempt; see MAY_MATCH_SHIPPED_DEFAULT
        "kafka_bootstrap_servers": "kafka.invalid:9092",  # .invalid never resolves — fails fast
        "kafka_live_topic": "raw.events.test",
        "kafka_dlq_topic": "raw.events.dlq.test",
        "kafka_topic_partitions": 2,
        "postgres_dsn": PostgresDsn("postgresql://reporadar:test@db.invalid:5432/reporadar"),
        "data_dir": tmp_path / "data",
        "aggregate_dir": tmp_path / "aggregates",
        "seen_window": 1_000,
    }
    # The docstring above is a claim; this is what keeps it true. A field added to
    # Settings without a line here would fall back to the environment and source
    # itself from whoever is running the suite — on their machine only, and
    # without failing. "Every field" is the whole basis of the guarantee, so it
    # is checked rather than trusted.
    assert pinned.keys() == Settings.model_fields.keys()

    # The other half of the guarantee, and the one that used to be a comment. A
    # pinned value equal to the shipped default makes "did this come from
    # settings?" unanswerable: the assertion passes whether the caller read the
    # field or hard-coded the library constant, so the test is green against the
    # bug it exists to catch. The rule was written next to `seen_window` and three
    # fields breached it, which is the difference between a rule and an instrument.
    shipped = {
        name
        for name, value in pinned.items()
        if name not in MAY_MATCH_SHIPPED_DEFAULT and value == Settings.model_fields[name].default
    }
    assert not shipped, f"pinned to the shipped default, so wiring cannot fail: {sorted(shipped)}"
    return Settings(**pinned)
