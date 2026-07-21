"""Runtime configuration.

Every knob comes from the environment (or a local ``.env``); business logic
never hardcodes paths, tokens, or URLs. New settings must be documented in
``.env.example`` in the same change.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide settings, sourced from the environment / ``.env``."""

    # populate_by_name matters: github_token carries a validation_alias, and
    # without it pydantic would silently ignore Settings(github_token=...) —
    # the alias would be required instead. Field names must always work.
    model_config = SettingsConfigDict(
        env_prefix="REPORADAR_", env_file=".env", extra="ignore", populate_by_name=True
    )

    # Accept the conventional GITHUB_TOKEN as well as the prefixed form.
    github_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices("REPORADAR_GITHUB_TOKEN", "GITHUB_TOKEN"),
    )
    user_agent: str = "reporadar (independent research project)"
    api_base: str = "https://api.github.com"
    archive_base: str = "https://data.gharchive.org"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_live_topic: str = "raw.events.live"
    kafka_dlq_topic: str = "raw.events.dlq"  # undecodable messages, for triage and replay
    # No default: the DSN carries a password, and a wrong-by-default database is
    # worse than an absent one. Only the store requires it, so it stays optional
    # here and pg_store() fails loudly when it is missing — polling needs no DB.
    postgres_dsn: PostgresDsn | None = None
    data_dir: Path = Path("data")

    @property
    def live_dir(self) -> Path:
        """Where live /events poll samples land (NDJSON)."""
        return self.data_dir / "raw" / "live"

    @property
    def archive_dir(self) -> Path:
        """Where GH Archive hourly files land (.json.gz, exactly as published)."""
        return self.data_dir / "raw" / "gharchive"


@lru_cache
def get_settings() -> Settings:
    return Settings()
