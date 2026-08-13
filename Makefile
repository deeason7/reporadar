.PHONY: setup lint fmt test up down logs provision up-app down-app logs-app image marts \
        marts-status marts-converge grafana-grants site

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

# Rebuild the public result page from the lake, in place.
#
# Every figure on that page is re-derived here; none of them is stored in the
# HTML by hand. That is what makes this safe to run at any time and pointless to
# run twice: the output carries no timestamp, so a rebuild over an unchanged lake
# produces a byte-identical file and an empty diff. A diff after this ran means
# the lake moved, which is exactly when the page needed rewriting.
#
# It reads `data/lake`, which is not in the repository — build it first with
# `reporadar backfill <from> <to>`. With no lake the command says so and stops,
# rather than writing a page with nothing on it.
site:
	uv run python tools/build_site.py

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

# Settings come from .env, the same file every service reads, so there is one
# place where the database address lives. Sourced only if it is there, the way
# compose marks its env_file optional and for the same reason: a fresh clone has
# no .env yet, and CI runs exactly that. A setting that is genuinely missing then
# produces the tool's own named error rather than the shell's
# "No such file or directory", which names neither the setting nor the fix.
DOTENV = set -a; [ -f .env ] && . ./.env; set +a

# Build the marts and run their tests. Reads the Parquet lake in place and writes
# tables into Postgres, so the database has to be up (`make up`).
#
# `build` rather than `run`: a model and its tests are one unit, and a mart that
# fails its tests should not be left sitting in the schema the dashboard reads.
marts:
	$(DOTENV); \
	REPORADAR_DATA_DIR="$${REPORADAR_DATA_DIR:-data}" \
	uv run dbt build --project-dir dbt --profiles-dir dbt

# Is what the dashboard is showing built from every hour the lake holds?
marts-status:
	$(DOTENV); \
	uv run reporadar marts-status

# Build the marts, but only if the lake has moved since they were last built.
#
# The same treatment the ingest loop already gets: not a schedule, a difference.
# Nothing records when a build last ran — the comparison is between the hours the
# lake holds and the hours each mart row says it was computed from, so the answer
# stays true no matter who ran what, or when, or whether it finished. That is also
# why this is safe to run repeatedly: over an unchanged lake it is a directory
# walk, one small query, and no build at all.
#
# Three outcomes, not two, and the difference is the point. Exit 3 means stale
# and is the only code that triggers a build; any other non-zero means the check
# itself did not run, and rebuilding on that would turn an unreachable database
# into a rebuild whose own failure becomes the answer.
#
# Three rather than two, and that is not cosmetic. Two is the conventional
# usage-error code, so a misspelled flag exits 2 from the command-line framework
# before the check runs; and the runner exits 2 when it cannot spawn the command
# at all. Branching on 2 was watched rebuilding the published aggregates in both
# cases -- the two states that most clearly mean "this did not run" were the two
# being read as "it is stale". Codes carrying application meaning start at 3,
# because 0, 1 and 2 are already spoken for.
marts-converge:
	@$(DOTENV); \
	status=0; uv run reporadar marts-status || status=$$?; \
	if [ "$$status" -eq 0 ]; then \
		echo "marts are current with the lake; nothing to build"; \
	elif [ "$$status" -eq 3 ]; then \
		$(MAKE) marts; \
	else \
		echo "marts-status did not complete (exit $$status); not rebuilding"; \
		exit "$$status"; \
	fi

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
	$(DOTENV); \
	docker compose exec -T timescaledb psql \
		-U "$${POSTGRES_USER:-reporadar}" -d "$${POSTGRES_DB:-reporadar}" \
		-v grafana_password="$${GRAFANA_DB_PASSWORD:?set GRAFANA_DB_PASSWORD in .env}" \
		-v dbname="$${POSTGRES_DB:-reporadar}" \
		-v owner="$${POSTGRES_USER:-reporadar}" \
		-f - < sql/grafana_reader.sql
