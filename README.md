# RepoRadar — ecosystem intelligence for open source

RepoRadar ingests the public GitHub event firehose and turns it into three answers that
engineering teams actually need:

1. **"Is this project's momentum real?"** — trend detection with fake-star / coordinated-campaign
   screening, evaluated as alert precision under an explicit review budget.
2. **"Which of our dependencies are at risk of abandonment?"** — survival-analysis risk scores
   (who maintains what we depend on, and is the project decaying?), backtested for warning lead time.
3. **"What actually causes adoption?"** — event studies on natural experiments: license changes,
   front-page moments, CVE disclosures.

The core is deliberately boring and statistical: pipelines, reconciliation, baselines,
calibration. A local-LLM layer will eventually write weekly ecosystem briefs — with a plain
template fallback, so no model is ever a point of failure.

> **Status:** early scaffold. This repository currently contains the project's packaging,
> tooling, and CI. Ingestion, analytics, and the local stack land incrementally — the changelog
> tracks what is actually in place.

## The honest-ingestion design

GitHub's `/events` API is a **fast but lossy** window (pagination caps what one poller sees at
peak). [GH Archive](https://www.gharchive.org/) publishes the **complete but hourly** record.
RepoRadar is built to run both and measure the gap — **capture rate** is a first-class KPI,
not a footnote.

```mermaid
flowchart LR
    GH[GitHub /events] -->|async poller, ETag-aware| K[Kafka]
    GA[GH Archive hourly] -->|batch ingest| L[Parquet lake]
    K --> C[Python consumers: validate, dedupe, DLQ]
    C --> TS[(TimescaleDB: hot 90d)]
    L --> D[DuckDB analytics]
    L -. reconcile ids .-> R[capture-rate KPI]
    K -. sampled ids .-> R
    TS --> G[Grafana: ops + product]
```

Design rules that hold everywhere:

- **Completeness is measured, never assumed.** The live stream is honest about what it misses;
  the archive is the arbiter.
- **Every model must beat a named dumb baseline on a time-based split** before it ships.
- **Person-level data is aggregated to repo/ecosystem level** in everything published. No
  individual-maintainer risk pages, ever.

## Development

```bash
make setup        # uv sync + install pre-commit hooks
make lint test    # zero-warning gate: ruff, mypy --strict, pytest
```

Python 3.12; dependencies and the toolchain are managed with [uv](https://docs.astral.sh/uv/).

## Compliance

Independent research project; **not affiliated with or endorsed by GitHub**. Data comes from
the official GitHub REST API (authenticated, within published rate limits) and GH Archive —
no scraping. Person-level signals are aggregated to repo/ecosystem level in everything
published; raw event data is never redistributed as a dataset.

## License

[MIT](LICENSE)
