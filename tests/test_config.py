from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from reporadar.config import Settings, get_settings

_ENV_VARS = (
    "GITHUB_TOKEN",
    "REPORADAR_GITHUB_TOKEN",
    "REPORADAR_USER_AGENT",
    "REPORADAR_API_BASE",
    "REPORADAR_ARCHIVE_BASE",
    "REPORADAR_KAFKA_BOOTSTRAP_SERVERS",
    "REPORADAR_KAFKA_LIVE_TOPIC",
    "REPORADAR_POSTGRES_DSN",
    "REPORADAR_DATA_DIR",
)


@pytest.fixture(autouse=True)
def _hermetic_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Tests see only the world they build: every setting is stripped from the
    process environment, and the working directory moves to an empty tmp dir so
    a developer's local ``.env`` is out of reach of ``Settings()``."""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)


def test_defaults_hold_without_environment() -> None:
    settings = Settings()
    assert settings.github_token is None  # unauthenticated is a supported (60 req/hr) mode
    assert settings.api_base == "https://api.github.com"
    assert settings.archive_base == "https://data.gharchive.org"
    assert settings.kafka_bootstrap_servers == "localhost:9092"  # the compose stack's listener
    assert settings.kafka_live_topic == "raw.events.live"
    assert settings.postgres_dsn is None  # no default: only the store needs a database
    assert settings.data_dir == Path("data")


def test_postgres_dsn_is_parsed_and_a_malformed_one_is_refused_at_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORADAR_POSTGRES_DSN", "postgresql://u:p@db.example:5432/reporadar")
    dsn = Settings().postgres_dsn
    assert dsn is not None
    assert str(dsn) == "postgresql://u:p@db.example:5432/reporadar"  # round-trips for the driver

    # A typo becomes a startup error, not a confusing connection failure later.
    monkeypatch.setenv("REPORADAR_POSTGRES_DSN", "localhost:5432/reporadar")
    with pytest.raises(ValidationError):
        Settings()


def test_env_prefix_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORADAR_USER_AGENT", "reporadar (pinned)")
    assert Settings().user_agent == "reporadar (pinned)"


def test_token_accepts_the_conventional_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "gh-conventional")
    assert Settings().github_token == "gh-conventional"


def test_token_prefixed_env_name_outranks_conventional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "gh-conventional")
    monkeypatch.setenv("REPORADAR_GITHUB_TOKEN", "gh-prefixed")
    assert Settings().github_token == "gh-prefixed"


def test_token_accepted_as_init_argument() -> None:
    # Guards populate_by_name: without it this kwarg would be silently dropped.
    assert Settings(github_token="direct").github_token == "direct"


def test_dotenv_file_is_read_from_cwd(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("GITHUB_TOKEN=gh-from-dotenv\n", encoding="utf-8")
    assert Settings().github_token == "gh-from-dotenv"


def test_real_environment_outranks_dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("GITHUB_TOKEN=gh-from-dotenv\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN", "gh-from-env")
    assert Settings().github_token == "gh-from-env"


def test_derived_dirs_follow_data_dir(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path / "data")
    assert settings.live_dir == tmp_path / "data" / "raw" / "live"
    assert settings.archive_dir == tmp_path / "data" / "raw" / "gharchive"


def test_get_settings_returns_one_cached_instance() -> None:
    get_settings.cache_clear()
    try:
        assert get_settings() is get_settings()
    finally:
        get_settings.cache_clear()
