# Changelog

All notable changes to this project are documented here, per
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[SemVer](https://semver.org/).

## [Unreleased]

### Added
- Project scaffold: src layout, uv-managed environment, ruff + mypy (strict) + pytest
  zero-warning gate, pre-commit hooks, and CI (lint/type/test).
- MIT license and security policy.
- Typed settings (pydantic-settings) and the GitHub public-event envelope, with
  order-preserving NDJSON parsing and deduplication.
- ETag-aware, rate-limit-honest GitHub API client.
- Live `/events` poller and idempotent GH Archive hour downloads.
- DuckDB archive analysis (event-type histogram and the capture-rate KPI) and the
  `reporadar` command-line interface.
- Local development stack (Kafka in KRaft mode, TimescaleDB, Grafana) via docker
  compose, with a CI job validating the compose file.
- Always-on capture service behind `reporadar serve`: an interval-driven poll loop with
  bounded cross-run deduplication, run counters, capped rate-limit pauses, and clean
  shutdown on SIGINT/SIGTERM, writing fresh events into hourly NDJSON files that mirror
  the archive layout.
- Versioned wire format for events entering the message stream: a JSON envelope carrying
  the schema version and capture time, keyed by repository id so one repository's events
  stay ordered.
