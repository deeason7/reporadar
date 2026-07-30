# syntax=docker/dockerfile:1

# Two stages, because the tools that build an environment are not the tools that
# run one. The build stage carries a package manager, a compiler toolchain and
# the lockfile; the runtime stage carries an interpreter and a virtualenv. Every
# byte left behind in the final image is a byte somebody has to patch.

FROM python:3.12-slim-bookworm AS build

# uv is pinned to an exact version rather than a moving tag: an image that
# resolves differently next month is not the image that was tested.
COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies before source, and from the lockfile alone. This layer is
# invalidated when the lock changes rather than when a source file does, which
# is the difference between rebuilding in seconds and re-resolving the whole
# dependency set on every commit.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# --frozen, never a fresh resolve: the image must install the versions the test
# suite ran against. A build that is allowed to resolve is a build that can ship
# a dependency nobody tested.
#
# --no-editable is load-bearing, not tidiness. uv installs the project itself as
# an editable install by default, which leaves the venv holding a pointer to
# /app/src rather than the code. The runtime stage copies only the venv, so the
# image built, exported, and tagged perfectly happily — and then died on
# `ModuleNotFoundError: No module named 'reporadar'` the first time it was run.
# A regular install puts the package in site-packages, so the venv is complete
# on its own.
COPY src/ ./src/
RUN uv sync --frozen --no-dev --no-editable


FROM python:3.12-slim-bookworm AS runtime

# A service that only ever writes into its data directory has no business being
# able to write anywhere else. Fixed uid so the mounted volume's ownership does
# not depend on whatever the base image happened to allocate.
RUN useradd --create-home --uid 10001 reporadar \
    && mkdir -p /app/data \
    && chown -R reporadar:reporadar /app

COPY --from=build --chown=reporadar:reporadar /app/.venv /app/.venv

# PYTHONUNBUFFERED is not a preference here. Container stdout is a pipe, so
# Python block-buffers it, and a long-running service's progress logs then
# appear in `docker logs` only in 8 KB bursts — or, when it hangs, not at all.
# A progress log that is not flushed is not a progress log, and the one time it
# matters most is the one time it is silent.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    REPORADAR_DATA_DIR=/app/data

WORKDIR /app
USER reporadar

# The entrypoint is the CLI, so a service chooses itself with a command
# (`serve`, `consume`, `archive-serve`) and every one of them is the same image.
# One artifact for every process means they cannot drift apart between deploys.
ENTRYPOINT ["reporadar"]
CMD ["--help"]
