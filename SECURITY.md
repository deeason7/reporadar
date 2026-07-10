# Security

## Reporting

Found a vulnerability? Email **deeasonsitaula5@gmail.com** — please don't open a public issue
for security reports. You'll get an acknowledgment within a few days.

## Posture

- **Secrets:** environment-only (`.env`, gitignored). No secrets in code, logs, or history.
  The GitHub token used for ingestion is fine-grained and read-only on public data.
- **Data:** public GitHub event data only; no PII is collected beyond what GitHub already
  publishes, and published analyses aggregate person-level signals to repo/ecosystem level.
- **Dependencies:** version-bounded in `pyproject.toml` with a committed `uv.lock`.
