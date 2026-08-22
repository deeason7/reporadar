"""Smoke test: the package imports and exposes a valid semantic version."""

from reporadar import __version__


def test_version_is_semver() -> None:
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_the_two_copies_of_the_version_agree() -> None:
    """The version is written in two files, and nothing was comparing them.

    ``pyproject.toml`` carried ``0.1.0`` against a repository tagged ``v1.0.0``,
    and ``__init__.py`` carried its own copy of the same wrong number — so the
    semver test above passed on a value that was correct in shape and wrong in
    fact. ⇒ 🔑 *A format check is not a value check*, and two copies of a constant
    drift the moment only one of them is edited.
    """
    import re
    from pathlib import Path

    pyproject = (Path(__file__).resolve().parent.parent / "pyproject.toml").read_text()
    match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE)
    assert match, "pyproject.toml has no top-level version to compare against"
    assert match.group(1) == __version__, (
        f"pyproject.toml says {match.group(1)}, reporadar.__version__ says {__version__}"
    )
