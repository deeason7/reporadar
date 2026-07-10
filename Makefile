.PHONY: setup lint fmt test

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
