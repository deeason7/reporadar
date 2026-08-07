.PHONY: setup lint fmt test up down logs provision up-app down-app logs-app image marts

# One-time dev setup: environment + hooks
setup:
	uv sync --extra dev --extra dbt
	uv run pre-commit install

# Zero-warning gate: style, format, types (same three CI runs)
lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

fmt:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest -q

# Local stack (Kafka + TimescaleDB + Grafana); host ports + secrets come from .env
up:
	docker compose up -d

provision:
	uv run reporadar provision

down:
	docker compose down

logs:
	docker compose logs -f

# The same stack plus the application services. Separate targets rather than a
# flag on the ones above, so that `make up` cannot start polling GitHub by
# accident — the profile is the mechanism, these are just the two names for it.
up-app:
	docker compose --profile app up -d --build

down-app:
	docker compose --profile app down

logs-app:
	docker compose --profile app logs -f serve consume archive-serve

# Build the runtime image alone, the way CI does.
image:
	docker build -t reporadar:dev .

# Build the marts and run their tests. Reads the Parquet lake in place and writes
# tables into Postgres, so the database has to be up (`make up`).
#
# The settings come from .env, the same file every service reads, so there is one
# place where the database address lives. `build` rather than `run`: a model and
# its tests are one unit, and a mart that fails its tests should not be left
# sitting in the schema the dashboard reads.
marts:
	set -a; . ./.env; set +a; \
	REPORADAR_DATA_DIR="$${REPORADAR_DATA_DIR:-data}" \
	uv run dbt build --project-dir dbt --profiles-dir dbt
