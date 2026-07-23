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
- Kafka producer sink for captured events: messages carry the versioned wire envelope keyed
  by repository id, with per-batch delivery confirmation; broker address and topic are
  configurable settings.
- Stream consumer behind `reporadar consume`: it reads the event stream, validates every
  message against the versioned wire contract, deduplicates by event id within a bounded
  window, and writes the surviving events to a TimescaleDB hypertable carrying both the
  event and capture clocks. Offsets are committed only after a batch is stored and the
  writes are idempotent by event id, so an interrupted run redelivers without duplicating;
  progress counters are logged as it goes, and SIGINT/SIGTERM end the run cleanly.
- Dead-letter routing for messages that cannot be decoded: each is published to a dead-letter
  topic as a self-describing versioned record carrying the triage reason and the original
  bytes, so one malformed message is isolated and replayable rather than dropped or fatal.
- The capture service now feeds the stream: `serve` writes each batch of fresh events to the
  hourly NDJSON files (the reconciliation record) and publishes it to Kafka in one step, so the
  live path and the consumer are finally connected end to end. The two are not equals — the
  files are the record and a write failure there stops the run, while the stream is best-effort:
  a broker outage is logged and counted and the service keeps capturing, because the archive
  reconciliation, not the live stream, is what completeness is measured against.
- `reporadar provision` creates the Kafka topics the pipeline needs, idempotently, with a
  `--check` mode that reports without creating and exits non-zero when the broker is not ready.
  Reading commands now verify the topics before joining a consumer group, so a fresh broker
  fails immediately naming the missing topic instead of stalling and then reporting nothing
  useful. Partition and replication counts are settings; existing topics are never altered.
- Configurable deduplication window (`REPORADAR_SEEN_WINDOW`): how many recent event ids the
  poll and consume loops remember before an id counts as fresh again. It trades memory against
  the duplication horizon, so a deployment can size it to its own traffic from the environment
  rather than passing a flag on every restart. A window below 1 is refused at startup.

### Fixed
- The capture-rate KPI no longer reports a rate it cannot support. When a live sample holds
  events but none of them appear in the archive hour it is compared against, the two files
  cannot be reconciled at all — an unshared event identifier, or a sample drawn from a
  different hour — and that is a different fact from capturing nothing. The previous
  arithmetic turned it into a confident `0.0%`: correctly typed, in range, and wrong in a way
  nothing downstream could detect, pointing any investigation at the poller instead of at the
  comparison. `capture-rate` now prints the counts, names the cause, and exits non-zero, so a
  scheduled run cannot record it as a successful measurement.
