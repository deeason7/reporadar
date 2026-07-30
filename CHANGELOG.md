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
- DuckDB archive analysis (event-type histogram and a live-sample-to-archive-hour
  comparison) and the `reporadar` command-line interface.
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
- The capture service now measures how much of the feed it is actually seeing, using nothing
  but the feed itself. Public event ids run in near sequence, so the distance the feed travels
  between two polls says how many events went by, and the poller knows how many of those it
  holds; the ratio is reported alongside the run counters. Two details make it trustworthy: the
  feed carries more than one id sequence, so sequences are detected from the data and never
  subtracted across; and the id spacing is measured on every sweep rather than configured,
  because a constant would quietly stop being true. It is published as an estimate — absent
  rather than zero when a cycle cannot support one, such as the first cycle of a run or a
  repeated page — and it rests on the stated assumption that a returned page is contiguous.
- Archive hours can be converted into a columnar store: one Parquet file per hour, laid out as
  `dt=<date>/hr=<hour>` and compressed with zstd. The published gzip files are whole-file and
  row-oriented, so any question asked of them decompresses everything; against the same three
  hours, counting the distinct event types took 0.67 s over the gzip and 0.001 s over the
  columnar copy, which is the entire reason the copy exists. It is also smaller than its source
  (0.76x measured), so keeping both costs little. The column list is written down rather than
  inferred, because a reader that guesses the shape of a nested field from the first rows will
  fail later on a row carrying a key it never sampled — and whether it fails depends on which
  columns a query happens to select, which is worse than failing outright. Event payloads are
  therefore kept as JSON: they differ between events of the very same type, so a fixed shape
  would turn each new key into a rejected hour. Each hour is staged and renamed into place, and
  is checked against the hour it claims to be before that happens: a truncated or misfiled hour
  still reads as valid data, and nothing downstream could tell it from a quiet one.
- Ingested archive hours are now recorded, one row per hour, so the system knows what it has
  without listing the store: which hours are present and counted, which the publisher does not
  have, and which could not be trusted. The record holds outcomes rather than schedules, so
  "what is still missing" is a question rather than a replay — downtime, a partial failure and
  an hour published late all resolve on the next pass with nothing to re-run. Hours the
  publisher has not released yet are deliberately absent from it rather than marked pending,
  because absence is what makes the next pass pick them up. A recorded hour can be corrected
  when a late one arrives, and a completed hour is never quietly downgraded by a passing
  failure. The table enforces its own rules, so a counted hour cannot be recorded without its
  count even by a future writer that forgets.
- One archive hour can now be taken from the publisher into the columnar store in a single
  step that records what became of it. The outcomes are the design: an hour that arrived is
  recorded with its counts, an hour the publisher will never release is recorded so the scan
  stops asking, and an hour that has merely not appeared yet is recorded nowhere at all —
  its absence is what makes the next pass try again, so there is no schedule to miss and
  nothing to replay after downtime. Only a genuine "not found" says anything about the hour
  itself; a server error or a dropped connection is a fact about the request and leaves the
  hour outstanding rather than written off. An hour is given a full day to appear before it
  is called permanently absent, because that verdict is never revisited while publication
  normally takes minutes. An hour that arrives and cannot be read is a third outcome again:
  recorded, so it is not retried forever, and never counted as coverage — but failures
  meaning the software is wrong rather than the file are deliberately left uncaught, so one
  mistake cannot mark a whole range unreadable. The fetch and the conversion both run off
  the event loop, so a range can be ingested several hours at a time.
- Archive hours now ingest themselves. A loop asks the record which closed hours are still
  outstanding and converts them, a few at a time, until none are left. There is no schedule
  and therefore no missed run: downtime, a partial failure and an hour published late all
  resolve on the next pass. A timer could not substitute, because a missed timer fires once
  when the machine returns rather than once per interval it slept through. Hours that have
  not finished are never attempted — a partial hour filed as a whole one is indistinguishable
  from a quiet one ever after. Work is bounded two ways: how many hours convert at once,
  since the publisher is somebody else's server, and how far back each scan looks, so that a
  loop cannot turn one bad night into a week of surprise traffic. Anything older is reached
  by asking for a range explicitly, which is also what picks up hours that failed and have
  since been fixed. Database writes are serialised behind the concurrent conversions, because
  one connection cannot carry two overlapping operations. The run reports four numbers rather
  than two: hours converted, hours the publisher does not have, hours that could not be
  trusted, and hours deliberately left for the next pass.
- Two commands now drive the archive ingest: `reporadar archive-serve` keeps the columnar
  store converged on the published archive indefinitely, and `reporadar backfill` converges
  one explicit range of days and stops. The service ends cleanly on SIGINT/SIGTERM, waking
  out of its wait rather than finishing it, so stopping a container takes no longer than the
  hour in flight. The range command differs in three deliberate ways, each following from
  being asked for rather than scheduled: it retries hours previously found unreadable, which
  is how a correction reaches the hours it corrects and which the always-on loop must not do
  or a permanently broken hour would occupy it forever; it creates the record's table if it
  is absent, since a range is commonly the first thing pointed at a new database; and it
  refuses a pair of dates given in the wrong order instead of accepting them, because the
  scan builds its calendar forwards and would otherwise find nothing outstanding and report
  success — leaving every hour it skipped looking settled to anything that read the record
  afterwards. Both open one connection for the run and close it on the way out, including
  after a failure. Neither reconnects, deliberately: a dropped connection ends the run, and
  because the next one re-derives what is outstanding from the record rather than resuming a
  plan, a restart costs one interval and loses no work.
- The long-running commands now ship as a container image, and the local stack can run them:
  `docker compose --profile app up -d` starts topic provisioning, live capture, the stream
  consumer and the archive ingest alongside the infrastructure. All four are the same image and
  differ only in the command they are given, so no deployment can put a different build behind one
  process than another. They sit behind a profile deliberately, which keeps the plain `up` an
  infrastructure-only command — starting the stack to run the tests should not begin polling
  GitHub. The image installs from the lockfile and never resolves, so it carries the versions the
  test suite ran against; it leaves the build tools, the test suite and the package manager out of
  the runtime layer; and it runs as an unprivileged user that can write only its data directory.
  Python output is unbuffered in the image, because container output is a pipe and a service whose
  progress logs are block-buffered goes silent exactly when someone needs to read them.
  Configuration comes from the same environment file, with the broker address and the database
  connection string overridden to their in-network equivalents for these services only — the file
  holds what a developer needs from the host, and inside a container `localhost` is the container.
  Continuous integration now builds the image **and runs it**: the first version of it built,
  exported and tagged without a warning, then failed on startup, because the project had been
  installed as a link to a source directory the runtime layer deliberately does not carry.
- `reporadar verify` checks the hours record against the columnar store it describes. Two records of
  the same fact existed and had never been compared: every counter, gap scan and coverage number
  reads the record, so a row claiming an hour that is not on disk is not a small inconsistency but a
  number that misreports, and it misreports in the reassuring direction — nothing revisits a settled
  hour, so the hole is permanent and invisible. Which direction of disagreement matters decides the
  exit code: a claim with no file fails the check, while a file no row claims is reported and does
  not, because it misstates nothing and the next scan simply converts that hour again. By default the
  check costs one filesystem call per recorded hour and compares the stored size as well as presence,
  which catches a truncated or replaced file that mere existence would pass. `--counts` additionally
  compares event counts, reading the whole store in a single query — and reading the partition
  columns kept *inside* each file rather than the ones implied by its directory name, so an hour that
  was copied or misfiled cannot verify by describing where it happens to sit. It exits non-zero when
  anything is unbacked, so a scheduled run cannot report success over a store with holes. It never
  writes: a checker that repaired what it found would become a second author of the record, which is
  the situation it exists to detect.

### Changed
- The documented ingestion design no longer describes the hourly archive as a complete record to
  reconcile the live feed against. That framing was written before the two sources were compared,
  and comparing them did not support it: in the hours sampled they did not share events, and
  matching on the commit SHA a push carries — a value that cannot differ between two records of the
  same event — found no meaningful overlap in the adjacent hours either. Whatever the cause, the
  archive could not serve as ground truth for what the poller missed, so a figure derived by
  reconciling the two would report the mismatch rather than the miss. The README now states what
  is actually measured: coverage is estimated from the live feed alone, from the id spacing
  observable inside each returned page, and it is named an estimated capture ratio rather than a
  capture rate. The code already behaved this way; only the description had not caught up.
- The ingest commands no longer keep an archive hour's compressed source after converting it.
  A converted hour exists twice: as the downloaded `.json.gz` and as the columnar copy the hours
  record points at. Keeping both costs two and a half times the disk of keeping one — 34.5 MB per
  hour against 14.0, measured over 35 hours — and buys only the ability to skip a re-download of a
  file the publisher still serves unchanged. On a service that is not turned off, that difference
  decides whether a disk lasts a year, so `archive-serve` and `backfill` now remove the source once
  the hours record has been written, and say how many bytes each removal reclaimed. Removal happens
  strictly after that record exists: until then the hour is still outstanding and the next pass
  needs either the file or a fresh download. Pass `--keep-source` to keep them, which is what a
  workstation wants when the raw hour is the thing being examined.

### Fixed
- The poller now honours the cadence the API asks for. Every /events response states a minimum
  interval between polls, and the service was configured to poll six times faster than that,
  against an endpoint cached for longer still. Polling faster cannot surface more events: it
  spends quota re-reading a page already held and records the result as duplicates, which then
  reads as a property of the feed rather than of our own cadence. The stated interval is taken
  from the response rather than hardcoded — so it stays correct if the API changes it — and the
  slower of it and the configured interval is used, leaving a deliberately gentle setting alone
  while making a too-fast one unreachable. An override is logged once, so an operator can see
  why a configured interval is not the one in effect.
- The capture-rate KPI no longer reports a rate it cannot support. When a live sample holds
  events but none of them appear in the archive hour it is compared against, the two files
  cannot be reconciled at all — an unshared event identifier, or a sample drawn from a
  different hour — and that is a different fact from capturing nothing. The previous
  arithmetic turned it into a confident `0.0%`: correctly typed, in range, and wrong in a way
  nothing downstream could detect, pointing any investigation at the poller instead of at the
  comparison. `capture-rate` now prints the counts, names the cause, and exits non-zero, so a
  scheduled run cannot record it as a successful measurement.
