.PHONY: setup lint fmt test up down logs provision up-app down-app logs-app image marts grafana-grants

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

# Create (or update) the role the dashboard connects as, and grant it exactly the
# published aggregates and the hours record.
#
# Run once after `make up`, and again after changing GRAFANA_DB_PASSWORD. It is
# idempotent on purpose: the container's own initialisation scripts only run on a
# database that does not exist yet, and by the time anyone wants a dashboard the
# database is long since created.
#
# Piped through stdin rather than passed as a path, because the file lives in the
# working tree and psql runs inside the container.
grafana-grants:
	set -a; . ./.env; set +a; \
	docker compose exec -T timescaledb psql \
		-U "$${POSTGRES_USER:-reporadar}" -d "$${POSTGRES_DB:-reporadar}" \
		-v grafana_password="$${GRAFANA_DB_PASSWORD:?set GRAFANA_DB_PASSWORD in .env}" \
		-v dbname="$${POSTGRES_DB:-reporadar}" \
		-v owner="$${POSTGRES_USER:-reporadar}" \
		-f - < sql/grafana_reader.sql
