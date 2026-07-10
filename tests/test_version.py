"""Smoke test: the package imports and exposes a valid semantic version."""

from reporadar import __version__


def test_version_is_semver() -> None:
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
