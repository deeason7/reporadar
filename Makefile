.PHONY: setup lint fmt test up down logs provision

# One-time dev setup: environment + hooks
setup:
	uv sync --extra dev
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
