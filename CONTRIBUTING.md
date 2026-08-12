# Contributing

Thanks for your interest in RepoRadar. This is an independent research project; issues and
pull requests are welcome.

## Development setup

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
make setup        # create the environment and install pre-commit hooks
make lint test    # run the same checks CI enforces
```

## Standards

- **Zero-warning gate.** `ruff check`, `ruff format --check`, and `mypy --strict` must pass with
  no findings, and the test suite must be green. CI runs exactly these.
- **Types and tests travel with code.** Public functions are fully typed; new behavior lands with
  tests in the same change.
- **Conventional commits.** Use `feat:`, `fix:`, `test:`, `docs:`, `chore:`, or `refactor:` with an
  optional scope, e.g. `feat(ingest): add archive downloader`.

## Branches

`main` is the only long-lived branch and is kept releasable. Branch from it for a change and open a
pull request back into it, naming the branch for the change — `feature/*`, `fix/*`, or `test/*`.

## Security

Please report vulnerabilities per [SECURITY.md](SECURITY.md) rather than opening a public issue.
