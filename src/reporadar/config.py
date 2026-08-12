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

from reporadar.ingest.dedup import DEFAULT_SEEN_WINDOW


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
    # One partition count for both topics, deliberately: the dead-letter sink keys
    # each record with the original repo id, so a repository's poison messages land
    # on the partition its live events do — a promise that only holds while the two
    # topics are partitioned identically. Raising this later re-maps every key, so
    # it is sized once at provisioning time rather than tuned casually.
    kafka_topic_partitions: int = Field(default=3, ge=1)
    # 1 is correct for the single-node stack this runs against; a real multi-broker
    # cluster wants 3. Above the number of registered brokers the broker refuses
    # outright, so this is checked before anything is created.
    kafka_topic_replication_factor: int = Field(default=1, ge=1)
    # No default: the DSN carries a password, and a wrong-by-default database is
    # worse than an absent one. Only the store requires it, so it stays optional
    # here and pg_store() fails loudly when it is missing — polling needs no DB.
    postgres_dsn: PostgresDsn | None = None
    data_dir: Path = Path("data")
    # How many recent event ids each poll loop remembers. Bigger costs memory and
    # buys a longer duplication horizon; it is a deployment-shaped number (traffic
    # and cycle interval), which is why it is settable rather than compiled in.
    seen_window: int = Field(default=DEFAULT_SEEN_WINDOW, ge=1)

    @property
    def live_dir(self) -> Path:
        """Where live /events poll samples land (NDJSON)."""
        return self.data_dir / "raw" / "live"

    @property
    def archive_dir(self) -> Path:
        """Where GH Archive hourly files land (.json.gz, exactly as published)."""
        return self.data_dir / "raw" / "gharchive"

    @property
    def lake_dir(self) -> Path:
        """Root of the columnar copy of the archive (Parquet, partitioned dt/hr).

        Derived from ``data_dir`` like the two above rather than being its own
        setting: one knob for where data lives is easier to deploy correctly than
        three, and putting the lake on its own disk is an additive change if a
        deployment ever needs it.
        """
        return self.data_dir / "lake"


@lru_cache
def get_settings() -> Settings:
    return Settings()
