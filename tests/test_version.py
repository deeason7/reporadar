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


def test_the_declared_version_is_one_the_changelog_has_released() -> None:
    """A version number is a claim that a release exists, and nothing was checking it.

    ``pyproject.toml`` briefly declared ``1.1.0`` — a version with no tag and no
    changelog entry, so a reader installing the package would be told they had a
    release that was never cut. The check above compares the two *copies* of the
    number and would pass on any value at all as long as both copies agreed.

    ⇒ 🔑 *Two wrong copies of a number are consistent.* Unreleased work belongs in
    ``[Unreleased]``; the declared version stays at the last release until a real
    one is cut, which is an owner action rather than an edit.
    """
    import re
    from pathlib import Path

    changelog = (Path(__file__).resolve().parent.parent / "CHANGELOG.md").read_text()
    released = re.findall(r"^## \[(\d+\.\d+\.\d+)\]", changelog, re.MULTILINE)
    assert released, "the changelog declares no released version to check against"
    assert __version__ in released, (
        f"version {__version__} has no `## [{__version__}]` entry in CHANGELOG.md. "
        f"Released versions are {released}. Unreleased work goes under [Unreleased]."
    )
